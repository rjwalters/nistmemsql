use std::{
    fs::File,
    io::{BufRead, BufReader, Write},
};

use crate::executor::QueryResult;

/// Data import/export utilities
pub struct DataIO;

impl DataIO {
    /// Export query results to CSV file
    pub fn export_csv(result: &QueryResult, file_path: &str) -> anyhow::Result<()> {
        let mut file = File::create(file_path)
            .map_err(|e| anyhow::anyhow!("Failed to create file '{}': {}", file_path, e))?;

        // Write header
        write_csv_row(&mut file, &result.columns)?;

        // Write rows
        for row in &result.rows {
            write_csv_row(&mut file, row)?;
        }

        println!("Exported {} rows to '{}'", result.row_count, file_path);
        Ok(())
    }

    /// Export query results to JSON file
    pub fn export_json(result: &QueryResult, file_path: &str) -> anyhow::Result<()> {
        let mut json_rows = Vec::new();
        for row in &result.rows {
            let mut json_obj = serde_json::Map::new();
            for (i, col) in result.columns.iter().enumerate() {
                if i < row.len() {
                    json_obj.insert(col.clone(), serde_json::Value::String(row[i].clone()));
                }
            }
            json_rows.push(serde_json::Value::Object(json_obj));
        }

        let json_output = serde_json::to_string_pretty(&json_rows)?;
        let mut file = File::create(file_path)
            .map_err(|e| anyhow::anyhow!("Failed to create file '{}': {}", file_path, e))?;
        file.write_all(json_output.as_bytes())?;

        println!("Exported {} rows to '{}'", result.row_count, file_path);
        Ok(())
    }

    /// Import CSV file and generate INSERT statements
    pub fn import_csv(file_path: &str, table_name: &str) -> anyhow::Result<Vec<String>> {
        let file = File::open(file_path)
            .map_err(|e| anyhow::anyhow!("Failed to open file '{}': {}", file_path, e))?;

        let reader = BufReader::new(file);
        let mut lines = reader.lines();
        let mut statements = Vec::new();

        // Read header
        let header_line = lines
            .next()
            .ok_or_else(|| anyhow::anyhow!("CSV file is empty"))?
            .map_err(|e| anyhow::anyhow!("Failed to read header: {}", e))?;

        let columns: Vec<&str> = header_line.split(',').collect();

        // Read data rows
        for (idx, line) in lines.enumerate() {
            let line =
                line.map_err(|e| anyhow::anyhow!("Failed to read line {}: {}", idx + 2, e))?;
            let values: Vec<&str> = line.split(',').collect();

            if values.len() != columns.len() {
                return Err(anyhow::anyhow!(
                    "Row {} has {} columns, expected {}",
                    idx + 2,
                    values.len(),
                    columns.len()
                ));
            }

            // Build INSERT statement
            let cols = columns.join(", ");
            let vals =
                values.iter().map(|v| format!("'{}'", v.trim())).collect::<Vec<_>>().join(", ");

            let stmt = format!("INSERT INTO {} ({}) VALUES ({});", table_name, cols, vals);
            statements.push(stmt);
        }

        println!("Generated {} INSERT statements from '{}'", statements.len(), file_path);
        Ok(statements)
    }
}

fn write_csv_row<W: Write>(writer: &mut W, values: &[String]) -> anyhow::Result<()> {
    let csv_line = values.iter().map(|v| escape_csv_value(v)).collect::<Vec<_>>().join(",");

    writeln!(writer, "{}", csv_line)?;
    Ok(())
}

fn escape_csv_value(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace("\"", "\"\""))
    } else {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_escape_csv_simple() {
        assert_eq!(escape_csv_value("hello"), "hello");
    }

    #[test]
    fn test_escape_csv_with_comma() {
        assert_eq!(escape_csv_value("hello,world"), "\"hello,world\"");
    }

    #[test]
    fn test_escape_csv_with_quotes() {
        assert_eq!(escape_csv_value("hello\"world"), "\"hello\"\"world\"");
    }

    #[test]
    fn test_escape_csv_with_newline() {
        assert_eq!(escape_csv_value("hello\nworld"), "\"hello\nworld\"");
    }
}
