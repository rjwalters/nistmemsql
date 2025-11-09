use clap::Parser;

mod repl;
mod executor;
mod formatter;
mod commands;
mod error;
mod script;
mod data_io;

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
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    if let Some(cmd) = args.command {
        // Execute command mode
        execute_command(&cmd, args.database)?;
    } else if let Some(file_path) = args.file {
        // Execute file mode
        execute_file(&file_path, args.database, args.verbose)?;
    } else if args.stdin || is_stdin_piped() {
        // Execute from stdin
        execute_stdin(args.database, args.verbose)?;
    } else {
        // Interactive REPL mode
        let mut repl = Repl::new(args.database)?;
        repl.run()?;
    }

    Ok(())
}

fn execute_command(cmd: &str, database: Option<String>) -> anyhow::Result<()> {
    let mut executor = ScriptExecutor::new(database, false)?;
    executor.execute_script(cmd)?;
    Ok(())
}

fn execute_file(path: &str, database: Option<String>, verbose: bool) -> anyhow::Result<()> {
    let mut executor = ScriptExecutor::new(database, verbose)?;
    executor.execute_file(path)?;
    Ok(())
}

fn execute_stdin(database: Option<String>, verbose: bool) -> anyhow::Result<()> {
    let mut executor = ScriptExecutor::new(database, verbose)?;
    executor.execute_stdin()?;
    Ok(())
}

fn is_stdin_piped() -> bool {
    // Check if stdin is a pipe/file (not a terminal)
    !atty::is(atty::Stream::Stdin)
}
