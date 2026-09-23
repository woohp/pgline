use std::time::Duration;

use clap::Parser;
use tokio_postgres::Client;

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

/// Fails if the connection can no longer run a trivial query, which is how
/// the tests check that cancellation and error handling left it reusable.
pub(crate) async fn assert_connection_usable(client: &Client) {
    let value: i32 = client
        .query_one("SELECT 1", &[])
        .await
        .expect("connection is no longer usable")
        .get(0);
    assert_eq!(value, 1);
}

pub(crate) async fn backend_pid(client: &Client) -> i32 {
    client
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0)
}

/// Polls `observer` until backend `pid` is actively running a query whose text
/// contains `marker`.
pub(crate) async fn wait_until_query_active(observer: &Client, pid: i32, marker: &str) {
    loop {
        let active: bool = observer
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity \
                 WHERE pid = $1 AND state = 'active' AND position($2 in query) > 0)",
                &[&pid, &marker],
            )
            .await
            .unwrap()
            .get(0);
        if active {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
