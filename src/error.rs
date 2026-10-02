//! Redacted, classified errors for ingestion and delivery recovery.

/// A failure whose category determines whether publication can be retried.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid configuration: {0}")]
    Config(String),
    #[error("feed validation failed: {0}")]
    Feed(String),
    #[error("media preparation failed: {0}")]
    Media(String),
    #[error("HTTP {status}")]
    Http {
        status: u16,
        retry_after: Option<u64>,
    },
    #[error("network request failed: {0}")]
    Transport(String),
    #[error("protocol validation failed: {0}")]
    Protocol(String),
    #[error("account validation failed: {0}")]
    Account(String),
    #[error("remote record conflict: {0}")]
    Conflict(String),
    #[error("state validation failed: {0}")]
    State(String),
    #[error("database operation failed: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("file operation failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON validation failed: {0}")]
    Json(#[from] serde_json::Error),
}

/// The application's typed result.
pub type Result<T> = std::result::Result<T, Error>;
