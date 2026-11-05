use tokio_postgres::error::DbError;

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("{}", postgres_message(.0))]
    Postgres(#[from] tokio_postgres::Error),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("stdout closed")]
    StdoutClosed,

    /// The user quit the pager before all output was written.
    #[error("pager closed")]
    PagerClosed,

    #[error("output writer stopped unexpectedly")]
    OutputSinkClosed,

    #[error("unsupported feature: {0}")]
    Unsupported(String),

    #[error("terminal editor error: {0}")]
    Reedline(#[from] reedline::ReedlineError),

    #[error("background task failed: {0}")]
    Join(#[from] tokio::task::JoinError),

    #[error("invalid connection settings: {0}")]
    Connection(String),

    #[error("internal error: {0}")]
    Internal(String),

    #[error("TLS setup failed: {0}")]
    Tls(String),

    #[error("query cancellation failed: {0}")]
    Cancellation(String),

    #[error("pager exited unsuccessfully: {0}")]
    PagerExit(std::process::ExitStatus),

    #[error("editor command is empty or invalid")]
    InvalidEditor,

    #[error("pager command is empty or invalid")]
    InvalidPager,

    #[error("could not start pager {pager}: {source}")]
    PagerStart {
        pager: String,
        source: std::io::Error,
    },

    #[error("{0}")]
    InvalidCommand(String),
}

impl AppError {
    /// The server rejected a query but the connection is still usable, so an
    /// interactive caller can report the error and keep the session.
    pub fn as_recoverable_db_error(&self) -> Option<&DbError> {
        match self {
            AppError::Postgres(error) if !error.is_closed() => error.as_db_error(),
            _ => None,
        }
    }

    /// Whether this is an error the server reported, whose message already
    /// starts with its severity, such as `ERROR:` or `FATAL:`.
    pub fn is_db_error(&self) -> bool {
        matches!(self, AppError::Postgres(error) if error.as_db_error().is_some())
    }
}

/// tokio-postgres displays every server error as just "db error" and leaves
/// out the cause of client-side failures, so build the message from the parts.
fn postgres_message(error: &tokio_postgres::Error) -> String {
    if let Some(db_error) = error.as_db_error() {
        return db_error.to_string();
    }
    match std::error::Error::source(error) {
        Some(source) => format!("PostgreSQL error: {error}: {source}"),
        None => format!("PostgreSQL error: {error}"),
    }
}

pub type Result<T> = std::result::Result<T, AppError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore = "requires PGLINE_TEST_URL"]
    async fn server_errors_show_the_server_message() {
        let database = crate::test_support::connect().await;
        let error = database
            .client
            .batch_execute(
                "CREATE TEMP TABLE pgline_error_message (id int PRIMARY KEY); \
                 INSERT INTO pgline_error_message VALUES (1), (1)",
            )
            .await
            .unwrap_err();
        let error = AppError::from(error);
        assert!(error.is_db_error());
        assert_eq!(
            error.to_string(),
            "ERROR: duplicate key value violates unique constraint \"pgline_error_message_pkey\"\n\
             DETAIL: Key (id)=(1) already exists."
        );
    }
}
