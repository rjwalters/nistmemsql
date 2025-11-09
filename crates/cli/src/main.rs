use clap::Parser;

mod repl;
mod executor;
mod formatter;
mod commands;
mod error;

use repl::Repl;

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
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    if let Some(cmd) = args.command {
        // Execute command mode
        execute_command(&cmd)?;
    } else if let Some(file_path) = args.file {
        // Execute file mode
        execute_file(&file_path)?;
    } else {
        // Interactive REPL mode
        let mut repl = Repl::new(args.database)?;
        repl.run()?;
    }

    Ok(())
}

fn execute_command(cmd: &str) -> anyhow::Result<()> {
    // TODO: Implement command execution
    println!("Command execution not yet implemented: {}", cmd);
    Ok(())
}

fn execute_file(path: &str) -> anyhow::Result<()> {
    // TODO: Implement file execution
    println!("File execution not yet implemented: {}", path);
    Ok(())
}
