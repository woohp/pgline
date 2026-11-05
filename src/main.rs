mod app;
mod cli;
mod commands;
mod connection;
mod copy_preflight;
mod error;
mod executor;
mod metadata;
mod output;
mod repl;
mod scanner;
#[cfg(test)]
mod test_support;
mod transaction;

use std::io::Write;

use clap::Parser;

use crate::{app::App, cli::Cli, error::Result};

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        // The reader went away, as with `| head` or quitting the pager; there
        // is nobody left to report to.
        if matches!(
            error,
            crate::error::AppError::StdoutClosed | crate::error::AppError::PagerClosed
        ) {
            return;
        }
        // Server errors already start with their severity, as in psql.
        let prefix = match &error {
            crate::error::AppError::Postgres(error) if error.as_db_error().is_some() => "",
            _ => "error: ",
        };
        let _ = writeln!(
            std::io::stderr().lock(),
            "{prefix}{}",
            output::safe_terminal_lines(&error.to_string())
        );
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let cli = Cli::parse();
    let database = connection::connect(&cli).await?;
    App::new(cli, database).run().await
}
