#[derive(Debug, Clone)]
pub enum MetaCommand {
    Quit,
    Help,
    DescribeTable(String),
    ListTables,
    Timing,
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
            Some(&"\\timing") => Some(MetaCommand::Timing),
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
    fn test_non_meta_command() {
        assert!(MetaCommand::parse("SELECT * FROM users").is_none());
    }
}
