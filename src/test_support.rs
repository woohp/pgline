use clap::Parser;

use crate::{cli::Cli, connection::Database};

pub(crate) fn database_url() -> String {
    std::env::var("PGLINE_TEST_URL").expect("PGLINE_TEST_URL is required")
}

pub(crate) fn cli(extra_arguments: &[&str]) -> Cli {
    let url = database_url();
    let mut arguments = vec!["pgline", url.as_str()];
    arguments.extend_from_slice(extra_arguments);
    Cli::try_parse_from(arguments).expect("PGLINE_TEST_URL must be a valid connection string")
}

pub(crate) async fn connect() -> Database {
    connect_with_cli(&[]).await.1
}

pub(crate) async fn connect_with_cli(extra_arguments: &[&str]) -> (Cli, Database) {
    let cli = cli(extra_arguments);
    let database = crate::connection::connect(&cli)
        .await
        .expect("test database connection failed");
    (cli, database)
}
