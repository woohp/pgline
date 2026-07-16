#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("PostgreSQL error: {0}")]
    Postgres(#[from] tokio_postgres::Error),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("stdout closed")]
    StdoutClosed,

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

    #[error("{0}")]
    InvalidCommand(String),
}

pub type Result<T> = std::result::Result<T, AppError>;
