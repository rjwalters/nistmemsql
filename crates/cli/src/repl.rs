use rustyline::{error::ReadlineError, DefaultEditor};

use crate::{
    commands::MetaCommand,
    executor::SqlExecutor,
    formatter::{OutputFormat, ResultFormatter},
};

pub struct Repl {
    executor: SqlExecutor,
    editor: DefaultEditor,
    formatter: ResultFormatter,
    database_path: Option<String>,
}

impl Repl {
    pub fn new(database: Option<String>, format: Option<OutputFormat>) -> anyhow::Result<Self> {
        let database_path = database.clone();
        let executor = SqlExecutor::new(database)?;
        let editor = DefaultEditor::new()?;
        let mut formatter = ResultFormatter::new();

        if let Some(fmt) = format {
            formatter.set_format(fmt);
        }

        Ok(Repl { executor, editor, formatter, database_path })
    }

    pub fn run(&mut self) -> anyhow::Result<()> {
        self.print_banner();

        loop {
            let prompt = "vibesql> ";
            match self.editor.readline(prompt) {
                Ok(line) => {
                    if line.trim().is_empty() {
                        continue;
                    }

                    // Add to history
                    let _ = self.editor.add_history_entry(line.as_str());

                    // Check if it's a meta-command
                    if let Some(meta_cmd) = MetaCommand::parse(&line) {
                        match self.handle_meta_command(meta_cmd) {
                            Ok(should_exit) => {
                                if should_exit {
                                    break;
                                }
                            }
                            Err(e) => {
                                eprintln!("Error: {}", e);
                            }
                        }
                    } else {
                        // Execute as SQL
                        match self.executor.execute(&line) {
                            Ok(result) => {
                                self.formatter.print_result(&result);

                                // Auto-save if database path is provided and this was a
                                // modification
                                if let Some(ref path) = self.database_path {
                                    if is_modification_statement(&line) {
                                        if let Err(e) = self.executor.save_database(path) {
                                            eprintln!(
                                                "Warning: Failed to auto-save database: {}",
                                                e
                                            );
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("Error: {}", e);
                            }
                        }
                    }
                }
                Err(ReadlineError::Interrupted) => {
                    println!("^C");
                    continue;
                }
                Err(ReadlineError::Eof) => {
                    println!("\\quit");
                    break;
                }
                Err(err) => {
                    eprintln!("Error: {:?}", err);
                    break;
                }
            }
        }

        self.print_goodbye();
        Ok(())
    }

    fn handle_meta_command(&mut self, cmd: MetaCommand) -> anyhow::Result<bool> {
        match cmd {
            MetaCommand::Quit => {
                return Ok(true);
            }
            MetaCommand::Help => {
                self.print_help();
            }
            MetaCommand::DescribeTable(table_name) => {
                self.executor.describe_table(&table_name)?;
            }
            MetaCommand::ListTables => {
                self.executor.list_tables()?;
            }
            MetaCommand::ListSchemas => {
                println!("Schemas: public (default)");
            }
            MetaCommand::ListIndexes => {
                println!("No indexes defined.");
            }
            MetaCommand::ListRoles => {
                println!("Roles: superuser (current)");
            }
            MetaCommand::SetFormat(format) => {
                self.formatter.set_format(format);
                let format_name = match format {
                    crate::formatter::OutputFormat::Table => "table",
                    crate::formatter::OutputFormat::Json => "json",
                    crate::formatter::OutputFormat::Csv => "csv",
                };
                println!("Output format set to: {}", format_name);
            }
            MetaCommand::Timing => {
                self.executor.toggle_timing();
            }
            MetaCommand::Save(path) => {
                let save_path = path.or_else(|| self.database_path.clone());
                match save_path {
                    Some(ref p) => {
                        self.executor.save_database(p)?;
                        println!("Database saved to: {}", p);
                    }
                    None => {
                        eprintln!("Error: No database file specified. Use \\save <filename> or start with --database flag");
                    }
                }
            }
        }
        Ok(false)
    }

    fn print_banner(&self) {
        println!("VibeSQL v0.1.0 - SQL:1999 FULL Compliance Database");
        println!("Type \\help for help, \\quit to exit\n");
    }

    fn print_goodbye(&self) {
        println!("Goodbye!");
    }

    fn print_help(&self) {
        println!(
            "
Meta-commands:
  \\d [table]      - Describe table or list all tables
  \\dt             - List tables
  \\ds             - List schemas
  \\di             - List indexes
  \\du             - List roles/users
  \\f <format>     - Set output format (table, json, csv)
  \\timing         - Toggle query timing
  \\save [file]    - Save database to SQL dump file
  \\h, \\help      - Show this help
  \\q, \\quit      - Exit

Examples:
  CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100));
  INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');
  SELECT * FROM users;
  \\f json
  \\f csv
"
        );
    }
}

/// Check if a SQL statement is a modification (DDL/DML) that should trigger auto-save
fn is_modification_statement(sql: &str) -> bool {
    let upper = sql.trim().to_uppercase();
    upper.starts_with("CREATE ")
        || upper.starts_with("DROP ")
        || upper.starts_with("ALTER ")
        || upper.starts_with("INSERT ")
        || upper.starts_with("UPDATE ")
        || upper.starts_with("DELETE ")
}
