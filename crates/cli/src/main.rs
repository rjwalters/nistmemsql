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
#[command(about = "VibeSQL command-line interface", long_about = None)]
struct Args {
    /// Interactive database file path (optional for in-memory database)
    #[arg(short, long)]
    database: Option<String>,

    /// Execute SQL from file
    #[arg(short, long)]
    file: Option<String>,

    /// Execute SQL command directly
    #[arg(short, long)]
    command: Option<String>,

    /// Read SQL from stdin
    #[arg(long)]
    stdin: bool,

    /// Verbose output for batch execution
    #[arg(short, long)]
    verbose: bool,

    /// Output format (table, json, csv)
    #[arg(long, value_parser = ["table", "json", "csv"])]
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
