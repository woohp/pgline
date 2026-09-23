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
        let _ = writeln!(
            std::io::stderr().lock(),
            "error: {}",
            output::safe_terminal_text(&error.to_string())
        );
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let cli = Cli::parse();
    let database = connection::connect(&cli).await?;
    App::new(cli, database).run().await
}
