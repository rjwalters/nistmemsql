use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;

use crate::executor::SqlExecutor;
use crate::formatter::{ResultFormatter, OutputFormat};
use crate::commands::MetaCommand;

pub struct Repl {
    executor: SqlExecutor,
    editor: DefaultEditor,
    formatter: ResultFormatter,
}

impl Repl {
    pub fn new(database: Option<String>, format: Option<OutputFormat>) -> anyhow::Result<Self> {
        let executor = SqlExecutor::new(database)?;
        let editor = DefaultEditor::new()?;
        let mut formatter = ResultFormatter::new();
        
        if let Some(fmt) = format {
            formatter.set_format(fmt);
        }

        Ok(Repl {
            executor,
            editor,
            formatter,
        })
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
                self.executor.list_schemas()?;
            }
            MetaCommand::ListIndexes => {
                self.executor.list_indexes()?;
            }
            MetaCommand::ListRoles => {
                self.executor.list_roles()?;
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
        println!("
Meta-commands:
  \\d [table]      - Describe table or list all tables
  \\dt             - List tables
  \\ds             - List schemas
  \\di             - List indexes
  \\du             - List roles/users
  \\f <format>     - Set output format (table, json, csv)
  \\timing         - Toggle query timing
  \\h, \\help      - Show this help
  \\q, \\quit      - Exit

Examples:
  CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100));
  INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');
  SELECT * FROM users;
  \\f json
  \\f csv
");
    }
}
