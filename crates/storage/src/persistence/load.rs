// ============================================================================
// SQL Dump Loading (Load Operations)
// ============================================================================
//
// Provides utilities for reading SQL dump files. The actual parsing and
// execution must be done at a higher level (e.g., CLI) to avoid circular
// dependencies between storage, parser, and executor crates.

use crate::StorageError;
use std::fs;
use std::path::Path;

/// Read SQL dump file and return its contents
///
/// This is a utility function that reads the SQL dump file.
/// The caller is responsible for parsing and executing the statements.
///
/// # Arguments
/// * `path` - Path to the SQL dump file
///
/// # Returns
/// * `Result<String, StorageError>` - SQL file contents or error
///
/// # Example
/// ```no_run
/// # use storage::read_sql_dump;
/// let sql_content = read_sql_dump("database.sql").unwrap();
/// // Parse and execute statements using parser and executor
/// ```
pub fn read_sql_dump<P: AsRef<Path>>(path: P) -> Result<String, StorageError> {
    let path_ref = path.as_ref();

    // Check if file exists
    if !path_ref.exists() {
        return Err(StorageError::NotImplemented(format!(
            "Database file does not exist: {}",
            path_ref.display()
        )));
    }

    // Read the entire file
    fs::read_to_string(path_ref)
        .map_err(|e| StorageError::NotImplemented(format!(
            "Failed to read database file: {}",
            e
        )))
}

/// Split SQL content into individual statements
///
/// Handles:
/// - Line comments (-- comment)
/// - Statement terminators (;)
/// - String literals (preserves semicolons inside strings)
///
/// This is a public utility function that can be used by CLI and other
/// higher-level components to split SQL dumps into executable statements.
pub fn split_sql_statements(sql: &str) -> Result<Vec<String>, StorageError> {
    let mut statements = Vec::new();
    let mut current_stmt = String::new();
    let mut in_string = false;
    let mut prev_char = ' ';

    for line in sql.lines() {
        // Handle line comments
        let mut line_content = line;
        if !in_string {
            if let Some(comment_pos) = line.find("--") {
                // Check if -- is inside a string by counting quotes before it
                let before_comment = &line[..comment_pos];
                let quote_count = before_comment.chars().filter(|&c| c == '\'').count();
                if quote_count % 2 == 0 {
                    // Even number of quotes = not in string, so this is a real comment
                    line_content = &line[..comment_pos];
                }
            }
        }

        // Process character by character to handle strings and semicolons
        for c in line_content.chars() {
            if c == '\'' && prev_char != '\\' {
                in_string = !in_string;
            }

            if c == ';' && !in_string {
                // Statement terminator found
                if !current_stmt.trim().is_empty() {
                    statements.push(current_stmt.trim().to_string());
                    current_stmt.clear();
                }
            } else {
                current_stmt.push(c);
            }

            prev_char = c;
        }

        // Add newline if we're continuing a statement
        if !current_stmt.is_empty() {
            current_stmt.push('\n');
        }
    }

    // Add final statement if there is one (statement without trailing semicolon)
    if !current_stmt.trim().is_empty() {
        statements.push(current_stmt.trim().to_string());
    }

    Ok(statements)
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_sql_statements_simple() {
        let sql = "SELECT 1; SELECT 2; SELECT 3;";
        let stmts = split_sql_statements(sql).unwrap();
        assert_eq!(stmts.len(), 3);
        assert_eq!(stmts[0], "SELECT 1");
        assert_eq!(stmts[1], "SELECT 2");
        assert_eq!(stmts[2], "SELECT 3");
    }

    #[test]
    fn test_split_sql_statements_with_comments() {
        let sql = "-- Comment\nSELECT 1; -- inline comment\nSELECT 2;";
        let stmts = split_sql_statements(sql).unwrap();
        assert_eq!(stmts.len(), 2);
        assert_eq!(stmts[0], "SELECT 1");
        assert_eq!(stmts[1], "SELECT 2");
    }

    #[test]
    fn test_split_sql_statements_with_strings() {
        let sql = "INSERT INTO t VALUES ('a;b'); INSERT INTO t VALUES ('c');";
        let stmts = split_sql_statements(sql).unwrap();
        assert_eq!(stmts.len(), 2);
        assert_eq!(stmts[0], "INSERT INTO t VALUES ('a;b')");
        assert_eq!(stmts[1], "INSERT INTO t VALUES ('c')");
    }

    #[test]
    fn test_split_sql_statements_multiline() {
        let sql = "CREATE TABLE t (\n  id INTEGER,\n  name VARCHAR\n);\nINSERT INTO t VALUES (1, 'test');";
        let stmts = split_sql_statements(sql).unwrap();
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("CREATE TABLE"));
        assert!(stmts[1].contains("INSERT INTO"));
    }
}
