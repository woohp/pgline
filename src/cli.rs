use std::path::PathBuf;

use clap::{Parser, ValueEnum};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, ValueEnum)]
pub enum OutputFormat {
    #[default]
    Table,
    Vertical,
    Csv,
    Tsv,
}

#[derive(Debug, Parser)]
#[command(
    version,
    about = "A fast, interactive PostgreSQL client",
    disable_help_flag = true
)]
pub struct Cli {
    /// Print help
    #[arg(long, action = clap::ArgAction::Help)]
    pub help: Option<bool>,

    /// Database name, PostgreSQL URI, or libpq-style connection string
    pub connection: Option<String>,

    #[arg(short = 'h', long)]
    pub host: Option<String>,

    #[arg(short = 'p', long)]
    pub port: Option<u16>,

    #[arg(short = 'U', long, alias = "username")]
    pub user: Option<String>,

    #[arg(short = 'd', long)]
    pub database: Option<String>,

    /// Prompt for a password before connecting
    #[arg(short = 'W', long)]
    pub password: bool,

    /// Never prompt for a password
    #[arg(short = 'w', long, conflicts_with = "password")]
    pub no_password: bool,

    /// Execute SQL and exit
    #[arg(short = 'c', long)]
    pub execute: Option<String>,

    /// Execute SQL from a file and exit
    #[arg(short = 'f', long, conflicts_with = "execute")]
    pub file: Option<PathBuf>,

    #[arg(long, value_enum, default_value_t)]
    pub format: OutputFormat,

    /// Maximum rows retained for display per result set; 0 means unlimited
    #[arg(long, default_value_t = 1000)]
    pub row_limit: usize,

    /// Maximum characters retained per field; defaults to 500 for human output and unlimited for CSV/TSV
    #[arg(long)]
    pub max_field_width: Option<usize>,

    #[arg(short = 'x', long)]
    pub expanded: bool,

    #[arg(long)]
    pub timing: bool,

    #[arg(long)]
    pub no_pager: bool,

    #[arg(long)]
    pub no_color: bool,

    #[arg(long)]
    pub history_file: Option<PathBuf>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_pg_style_connection_options() {
        let cli = Cli::try_parse_from([
            "pgline",
            "-h",
            "db.example",
            "-p",
            "5433",
            "-U",
            "ada",
            "-d",
            "app",
        ])
        .unwrap();
        assert_eq!(cli.host.as_deref(), Some("db.example"));
        assert_eq!(cli.port, Some(5433));
        assert_eq!(cli.user.as_deref(), Some("ada"));
        assert_eq!(cli.database.as_deref(), Some("app"));
    }

    #[test]
    fn field_width_is_opt_in_at_the_cli_layer() {
        let defaults = Cli::try_parse_from(["pgline"]).unwrap();
        assert_eq!(defaults.max_field_width, None);
        let explicit = Cli::try_parse_from(["pgline", "--max-field-width", "42"]).unwrap();
        assert_eq!(explicit.max_field_width, Some(42));
    }

    #[test]
    fn rejects_conflicting_password_modes() {
        assert!(Cli::try_parse_from(["pgline", "-W", "-w"]).is_err());
    }
}
