use std::{
    fs,
    io::{self, Read},
};

use crate::{
    vibesql_executor::SqlExecutor,
    formatter::{OutputFormat, ResultFormatter},
};

/// Script executor - runs multiple SQL statements from files or stdin
pub struct ScriptExecutor {
    executor: SqlExecutor,
    formatter: ResultFormatter,
    verbose: bool,
}

impl ScriptExecutor {
    pub fn new(
        database: Option<String>,
        verbose: bool,
        format: Option<OutputFormat>,
    ) -> anyhow::Result<Self> {
        let executor = SqlExecutor::new(database)?;
        let mut formatter = ResultFormatter::new();

        if let Some(fmt) = format {
            formatter.set_format(fmt);
        }

        Ok(ScriptExecutor { executor, formatter, verbose })
    }

    /// Execute SQL from a file
    pub fn execute_file(&mut self, file_path: &str) -> anyhow::Result<()> {
        let contents = fs::read_to_string(file_path)
            .map_err(|e| anyhow::anyhow!("Failed to read file '{}': {}", file_path, e))?;

        self.execute_script(&contents)
    }

    /// Execute SQL from stdin
    pub fn execute_stdin(&mut self) -> anyhow::Result<()> {
        let mut contents = String::new();
        io::stdin()
            .read_to_string(&mut contents)
            .map_err(|e| anyhow::anyhow!("Failed to read from stdin: {}", e))?;

        self.execute_script(&contents)
    }

    /// Execute a script (multiple SQL statements)
    pub fn execute_script(&mut self, script: &str) -> anyhow::Result<()> {
        // Split script into individual statements
        // Simple approach: split by semicolon (doesn't handle all edge cases)
        let statements = parse_statements(script);

        if statements.is_empty() {
            if self.verbose {
                println!("No SQL statements found in script");
            }
            return Ok(());
        }

        let mut success_count = 0;
        let mut error_count = 0;

        for (idx, stmt) in statements.iter().enumerate() {
            if self.verbose {
                println!("Executing statement {} of {}...", idx + 1, statements.len());
            }

            match self.executor.execute(stmt) {
                Ok(result) => {
                    self.formatter.print_result(&result);
                    success_count += 1;
                }
                Err(e) => {
                    eprintln!("Error executing statement {}: {}", idx + 1, e);
                    error_count += 1;
                    // Continue executing remaining statements
                }
            }
        }

        // Summary
        if self.verbose || error_count > 0 {
            println!("\n=== Script Execution Summary ===");
            println!("Total statements: {}", statements.len());
            println!("Successful: {}", success_count);
            println!("Failed: {}", error_count);
        }

        if error_count > 0 {
            Err(anyhow::anyhow!("{} statements failed", error_count))
        } else {
            Ok(())
        }
    }
}

/// Parse SQL script into individual statements
///
/// This is a simple implementation that:
/// 1. Removes comments (lines starting with -- or blocks with /* */)
/// 2. Splits on semicolons
/// 3. Filters empty statements
///
/// A more robust implementation would need a proper SQL tokenizer
/// to handle semicolons inside strings, comments, etc.
fn parse_statements(script: &str) -> Vec<String> {
    // First, remove comment lines
    let no_comments = script
        .lines()
        .filter(|line| {
            let trimmed = line.trim();
            !trimmed.is_empty() && !trimmed.starts_with("--") && !trimmed.starts_with("/*")
        })
        .collect::<Vec<_>>()
        .join("\n");

    // Then split on semicolons
    no_comments
        .split(';')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_single_statement() {
        let script = "SELECT * FROM users;";
        let stmts = parse_statements(script);
        assert_eq!(stmts.len(), 1);
        assert_eq!(stmts[0], "SELECT * FROM users");
    }

    #[test]
    fn test_parse_multiple_statements() {
        let script = "CREATE TABLE users (id INT); INSERT INTO users VALUES (1);";
        let stmts = parse_statements(script);
        assert_eq!(stmts.len(), 2);
        assert_eq!(stmts[0], "CREATE TABLE users (id INT)");
        assert_eq!(stmts[1], "INSERT INTO users VALUES (1)");
    }

    #[test]
    fn test_parse_with_whitespace() {
        let script = "  SELECT 1;  \n  SELECT 2;  ";
        let stmts = parse_statements(script);
        assert_eq!(stmts.len(), 2);
        assert_eq!(stmts[0], "SELECT 1");
        assert_eq!(stmts[1], "SELECT 2");
    }

    #[test]
    fn test_parse_with_comments() {
        let script = "-- This is a comment\nSELECT 1;";
        let stmts = parse_statements(script);
        // Comment lines starting with -- are filtered out, leaving only SELECT 1
        assert_eq!(stmts.len(), 1);
        assert_eq!(stmts[0], "SELECT 1");
    }

    #[test]
    fn test_parse_empty_script() {
        let script = "";
        let stmts = parse_statements(script);
        assert_eq!(stmts.len(), 0);
    }
}
