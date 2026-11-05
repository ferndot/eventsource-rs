//! Errors raised by the `SQLite` adapters.
//!
//! They reach callers boxed inside the core `Backend` error variants; downcast
//! to [`SqliteError`] to tell a database failure from corrupt stored data.

use eventsource_core::error::{
    AtomicAppendError, CheckpointError, DeadLetterError, OutboxError, ReadModelError,
    SnapshotError, StoreError,
};
use eventsource_core::lock::LockError;

/// What went wrong inside the `SQLite` adapters.
#[derive(Debug, thiserror::Error)]
pub enum SqliteError {
    /// `SQLite` reported a failure (I/O, locking, constraint, ...).
    #[error("sqlite failure")]
    Sqlite(#[from] rusqlite::Error),
    /// A stored row could not be turned back into a domain value.
    #[error("corrupt stored data in column {column:?}: {detail}")]
    Corrupt {
        /// The column that held the bad value.
        column: &'static str,
        /// Why the value was rejected.
        detail: String,
    },
}

impl SqliteError {
    // Takes the owned error so it composes directly with `map_err`.
    #[allow(clippy::needless_pass_by_value)]
    pub(crate) fn corrupt(column: &'static str, detail: impl ToString) -> Self {
        Self::Corrupt {
            column,
            detail: detail.to_string(),
        }
    }
}

impl From<SqliteError> for StoreError {
    fn from(error: SqliteError) -> Self {
        Self::Backend(Box::new(error))
    }
}

impl From<SqliteError> for SnapshotError {
    fn from(error: SqliteError) -> Self {
        match error {
            SqliteError::Corrupt { .. } => Self::Corrupt(error.to_string()),
            SqliteError::Sqlite(_) => Self::Backend(Box::new(error)),
        }
    }
}

impl From<SqliteError> for CheckpointError {
    fn from(error: SqliteError) -> Self {
        Self::Backend(Box::new(error))
    }
}

impl From<SqliteError> for DeadLetterError {
    fn from(error: SqliteError) -> Self {
        Self::Backend(Box::new(error))
    }
}

impl From<SqliteError> for LockError {
    fn from(error: SqliteError) -> Self {
        Self::Backend(Box::new(error))
    }
}

impl From<SqliteError> for ReadModelError {
    fn from(error: SqliteError) -> Self {
        Self::Backend(Box::new(error))
    }
}

impl From<SqliteError> for OutboxError {
    fn from(error: SqliteError) -> Self {
        Self::Backend(Box::new(error))
    }
}

impl From<SqliteError> for AtomicAppendError {
    fn from(error: SqliteError) -> Self {
        Self::Store(error.into())
    }
}
