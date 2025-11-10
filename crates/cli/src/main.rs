use clap::Parser;

mod commands;
mod config;
mod data_io;
mod error;
mod executor;
mod formatter;
mod repl;
mod script;

use config::Config;
use formatter::OutputFormat;
use repl::Repl;
use script::ScriptExecutor;

#[derive(Parser, Debug)]
#[command(name = "vibesql")]
#[command(version = "0.1.0")]
#[command(about = "VibeSQL - SQL:1999 FULL Compliance Database")]
#[command(long_about = "VibeSQL command-line interface

USAGE MODES:
  Interactive REPL:    vibesql [--database <FILE>]
  Execute Command:     vibesql -c \"SELECT * FROM users\"
  Execute File:        vibesql -f script.sql
  Execute from stdin:  cat data.sql | vibesql

INTERACTIVE REPL:
  When started without -c, -f, or piped input, VibeSQL enters an interactive
  REPL with readline support, command history, and meta-commands like:
    \\d [table]  - Describe table or list all tables
    \\dt         - List tables
    \\f <format> - Set output format
    \\copy       - Import/export CSV/JSON
    \\help       - Show all REPL commands

CONFIGURATION:
  Settings can be configured in ~/.vibesqlrc (TOML format):
    [display]
    format = \"table\"              # Default output format

    [database]
    default_path = \"~/data.db\"    # Default database file
    auto_save = true               # Auto-save on exit

    [history]
    file = \"~/.vibesql_history\"   # Command history file
    max_entries = 10000            # Max history entries

    [query]
    timeout_seconds = 0            # Query timeout (0 = no limit)

EXAMPLES:
  # Start interactive REPL with in-memory database
  vibesql

  # Use persistent database file
  vibesql --database mydata.db

  # Execute single command
  vibesql -c \"CREATE TABLE users (id INT, name VARCHAR(100))\"

  # Run SQL script file
  vibesql -f schema.sql -v

  # Import data from CSV
  echo \"\\\\copy users FROM 'data.csv'\" | vibesql --database mydata.db

  # Export query results as JSON
  vibesql -d mydata.db -c \"SELECT * FROM users\" --format json")]
struct Args {
    /// Database file path (if not specified, uses in-memory database)
    #[arg(short, long, value_name = "FILE")]
    database: Option<String>,

    /// Execute SQL commands from file
    #[arg(short, long, value_name = "FILE")]
    file: Option<String>,

    /// Execute SQL command directly and exit
    #[arg(short, long, value_name = "SQL")]
    command: Option<String>,

    /// Read SQL commands from stdin (auto-detected when piped)
    #[arg(long)]
    stdin: bool,

    /// Show detailed output during file/stdin execution
    #[arg(short, long)]
    verbose: bool,

    /// Output format for query results
    #[arg(long, value_parser = ["table", "json", "csv", "markdown", "html"], value_name = "FORMAT")]
    format: Option<String>,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Load configuration from ~/.vibesqlrc
    let config = Config::load().unwrap_or_else(|e| {
        eprintln!("Warning: Could not load config file: {}", e);
        Config::default()
    });

    // Use command-line format if provided, otherwise use config default
    let format =
        args.format.as_deref().and_then(parse_format).or_else(|| config.get_output_format());

    // Use command-line database if provided, otherwise use config default
    let database = args.database.or(config.database.default_path.clone());

    if let Some(cmd) = args.command {
        // Execute command mode
        execute_command(&cmd, database, format)?;
    } else if let Some(file_path) = args.file {
        // Execute file mode
        execute_file(&file_path, database, args.verbose, format)?;
    } else if args.stdin || is_stdin_piped() {
        // Execute from stdin
        execute_stdin(database, args.verbose, format)?;
    } else {
        // Interactive REPL mode
        let mut repl = Repl::new(database, format)?;
        repl.run()?;
    }

    Ok(())
}

fn parse_format(format_str: &str) -> Option<OutputFormat> {
    match format_str {
        "table" => Some(OutputFormat::Table),
        "json" => Some(OutputFormat::Json),
        "csv" => Some(OutputFormat::Csv),
        "markdown" => Some(OutputFormat::Markdown),
        "html" => Some(OutputFormat::Html),
        _ => None,
    }
}

fn execute_command(
    cmd: &str,
    database: Option<String>,
    format: Option<OutputFormat>,
) -> anyhow::Result<()> {
    let mut executor = ScriptExecutor::new(database, false, format)?;
    executor.execute_script(cmd)?;
    Ok(())
}

fn execute_file(
    path: &str,
    database: Option<String>,
    verbose: bool,
    format: Option<OutputFormat>,
) -> anyhow::Result<()> {
    let mut executor = ScriptExecutor::new(database, verbose, format)?;
    executor.execute_file(path)?;
    Ok(())
}

fn execute_stdin(
    database: Option<String>,
    verbose: bool,
    format: Option<OutputFormat>,
) -> anyhow::Result<()> {
    let mut executor = ScriptExecutor::new(database, verbose, format)?;
    executor.execute_stdin()?;
    Ok(())
}

fn is_stdin_piped() -> bool {
    // Check if stdin is a pipe/file (not a terminal)
    !atty::is(atty::Stream::Stdin)
}
