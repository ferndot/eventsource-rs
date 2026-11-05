//! Errors raised by the `PostgreSQL` adapters.
//!
//! They reach callers boxed inside the core `Backend` error variants; downcast
//! to [`PostgresError`] to tell a database failure from corrupt stored data.

use eventsource_core::error::{
    AtomicAppendError, CheckpointError, DeadLetterError, OutboxError, ReadModelError,
    SnapshotError, StoreError,
};
use eventsource_core::lock::LockError;

/// What went wrong inside the `PostgreSQL` adapters.
#[derive(Debug, thiserror::Error)]
pub enum PostgresError {
    /// `PostgreSQL` reported a failure (I/O, constraint, ...).
    #[error("postgres failure")]
    Postgres(#[from] tokio_postgres::Error),
    /// The schema name is not a plain identifier (`[A-Za-z0-9_]+`).
    #[error("invalid schema name {0:?}")]
    InvalidSchema(String),
    /// A stored row could not be turned back into a domain value.
    #[error("corrupt stored data in column {column:?}: {detail}")]
    Corrupt {
        /// The column that held the bad value.
        column: &'static str,
        /// Why the value was rejected.
        detail: String,
    },
}

impl PostgresError {
    // Takes the owned error so it composes directly with `map_err`.
    #[allow(clippy::needless_pass_by_value)]
    pub(crate) fn corrupt(column: &'static str, detail: impl ToString) -> Self {
        Self::Corrupt {
            column,
            detail: detail.to_string(),
        }
    }
}

impl From<PostgresError> for StoreError {
    fn from(error: PostgresError) -> Self {
        Self::Backend(Box::new(error))
    }
}

impl From<PostgresError> for SnapshotError {
    fn from(error: PostgresError) -> Self {
        match error {
            PostgresError::Corrupt { .. } => Self::Corrupt(error.to_string()),
            _ => Self::Backend(Box::new(error)),
        }
    }
}

impl From<PostgresError> for CheckpointError {
    fn from(error: PostgresError) -> Self {
        Self::Backend(Box::new(error))
    }
}

impl From<PostgresError> for DeadLetterError {
    fn from(error: PostgresError) -> Self {
        Self::Backend(Box::new(error))
    }
}

impl From<PostgresError> for LockError {
    fn from(error: PostgresError) -> Self {
        Self::Backend(Box::new(error))
    }
}

impl From<PostgresError> for ReadModelError {
    fn from(error: PostgresError) -> Self {
        Self::Backend(Box::new(error))
    }
}

impl From<PostgresError> for OutboxError {
    fn from(error: PostgresError) -> Self {
        Self::Backend(Box::new(error))
    }
}

impl From<PostgresError> for AtomicAppendError {
    fn from(error: PostgresError) -> Self {
        Self::Store(error.into())
    }
}
