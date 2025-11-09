use crate::formatter::OutputFormat;

#[derive(Debug, Clone)]
pub enum MetaCommand {
    Quit,
    Help,
    DescribeTable(String),
    ListTables,
    ListSchemas,
    ListIndexes,
    ListRoles,
    SetFormat(OutputFormat),
    Timing,
    Copy { table: String, file_path: String, direction: CopyDirection, format: CopyFormat },
}

#[derive(Debug, Clone)]
pub enum CopyDirection {
    Import, // FROM file
    Export, // TO file
}

#[derive(Debug, Clone)]
pub enum CopyFormat {
    Csv,
    Json,
}

impl MetaCommand {
    pub fn parse(line: &str) -> Option<Self> {
        let trimmed = line.trim();
        
        if !trimmed.starts_with('\\') {
            return None;
        }

        let parts: Vec<&str> = trimmed.split_whitespace().collect();
        
        match parts.get(0) {
            Some(&"\\q") | Some(&"\\quit") => Some(MetaCommand::Quit),
            Some(&"\\h") | Some(&"\\help") => Some(MetaCommand::Help),
            Some(&"\\d") => {
                if let Some(table_name) = parts.get(1) {
                    Some(MetaCommand::DescribeTable(table_name.to_string()))
                } else {
                    Some(MetaCommand::ListTables)
                }
            }
            Some(&"\\dt") => Some(MetaCommand::ListTables),
            Some(&"\\ds") => Some(MetaCommand::ListSchemas),
            Some(&"\\di") => Some(MetaCommand::ListIndexes),
            Some(&"\\du") => Some(MetaCommand::ListRoles),
            Some(&"\\f") => {
                if let Some(format_str) = parts.get(1) {
                    match *format_str {
                        "table" => Some(MetaCommand::SetFormat(OutputFormat::Table)),
                        "json" => Some(MetaCommand::SetFormat(OutputFormat::Json)),
                        "csv" => Some(MetaCommand::SetFormat(OutputFormat::Csv)),
                        _ => None,
                    }
                } else {
                    None
                }
            }
            Some(&"\\timing") => Some(MetaCommand::Timing),
            Some(&"\\copy") => {
                // Parse: \copy table_name TO/FROM 'file_path'
                // Format: \copy users TO '/tmp/users.csv'
                if parts.len() < 4 {
                    return None;
                }

                let table = parts[1].to_string();
                let direction_str = parts[2];
                let file_path = parts[3..].join(" ").trim_matches('\'').trim_matches('"').to_string();

                let direction = match direction_str.to_uppercase().as_str() {
                    "TO" => CopyDirection::Export,
                    "FROM" => CopyDirection::Import,
                    _ => return None,
                };

                // Infer format from file extension
                let format = if file_path.ends_with(".json") {
                    CopyFormat::Json
                } else {
                    CopyFormat::Csv // Default to CSV
                };

                Some(MetaCommand::Copy { table, file_path, direction, format })
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_quit() {
        assert!(matches!(MetaCommand::parse("\\q"), Some(MetaCommand::Quit)));
        assert!(matches!(MetaCommand::parse("\\quit"), Some(MetaCommand::Quit)));
    }

    #[test]
    fn test_parse_help() {
        assert!(matches!(MetaCommand::parse("\\h"), Some(MetaCommand::Help)));
        assert!(matches!(MetaCommand::parse("\\help"), Some(MetaCommand::Help)));
    }

    #[test]
    fn test_parse_list_tables() {
        assert!(matches!(MetaCommand::parse("\\d"), Some(MetaCommand::ListTables)));
        assert!(matches!(MetaCommand::parse("\\dt"), Some(MetaCommand::ListTables)));
    }

    #[test]
    fn test_parse_describe_table() {
        if let Some(MetaCommand::DescribeTable(name)) = MetaCommand::parse("\\d users") {
            assert_eq!(name, "users");
        } else {
            panic!("Failed to parse describe table command");
        }
    }

    #[test]
    fn test_parse_timing() {
        assert!(matches!(MetaCommand::parse("\\timing"), Some(MetaCommand::Timing)));
    }

    #[test]
    fn test_parse_list_schemas() {
        assert!(matches!(MetaCommand::parse("\\ds"), Some(MetaCommand::ListSchemas)));
    }

    #[test]
    fn test_parse_list_indexes() {
        assert!(matches!(MetaCommand::parse("\\di"), Some(MetaCommand::ListIndexes)));
    }

    #[test]
    fn test_parse_list_roles() {
        assert!(matches!(MetaCommand::parse("\\du"), Some(MetaCommand::ListRoles)));
    }

    #[test]
    fn test_parse_set_format() {
        assert!(matches!(MetaCommand::parse("\\f table"), Some(MetaCommand::SetFormat(OutputFormat::Table))));
        assert!(matches!(MetaCommand::parse("\\f json"), Some(MetaCommand::SetFormat(OutputFormat::Json))));
        assert!(matches!(MetaCommand::parse("\\f csv"), Some(MetaCommand::SetFormat(OutputFormat::Csv))));
    }

    #[test]
    fn test_non_meta_command() {
        assert!(MetaCommand::parse("SELECT * FROM users").is_none());
    }

    #[test]
    fn test_parse_copy_export_csv() {
        if let Some(MetaCommand::Copy { table, file_path, direction, format }) =
            MetaCommand::parse("\\copy users TO '/tmp/users.csv'") {
            assert_eq!(table, "users");
            assert_eq!(file_path, "/tmp/users.csv");
            assert!(matches!(direction, CopyDirection::Export));
            assert!(matches!(format, CopyFormat::Csv));
        } else {
            panic!("Failed to parse copy export CSV command");
        }
    }

    #[test]
    fn test_parse_copy_import_csv() {
        if let Some(MetaCommand::Copy { table, file_path, direction, format }) =
            MetaCommand::parse("\\copy users FROM '/tmp/users.csv'") {
            assert_eq!(table, "users");
            assert_eq!(file_path, "/tmp/users.csv");
            assert!(matches!(direction, CopyDirection::Import));
            assert!(matches!(format, CopyFormat::Csv));
        } else {
            panic!("Failed to parse copy import CSV command");
        }
    }

    #[test]
    fn test_parse_copy_export_json() {
        if let Some(MetaCommand::Copy { table, file_path, direction, format }) =
            MetaCommand::parse("\\copy users TO /tmp/users.json") {
            assert_eq!(table, "users");
            assert_eq!(file_path, "/tmp/users.json");
            assert!(matches!(direction, CopyDirection::Export));
            assert!(matches!(format, CopyFormat::Json));
        } else {
            panic!("Failed to parse copy export JSON command");
        }
    }
}
