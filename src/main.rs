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
#[cfg(test)]
mod test_support;
mod transaction;

use std::io::{IsTerminal, Write};

use clap::Parser;

use crate::{app::App, cli::Cli, error::Result};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_target(false)
        .without_time()
        .with_ansi(std::io::stderr().is_terminal())
        .with_writer(std::io::stderr)
        .init();
    if let Err(error) = run().await {
        if matches!(error, crate::error::AppError::StdoutClosed) {
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
    App::new(&cli, database).run(&cli).await
}
