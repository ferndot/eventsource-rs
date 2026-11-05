//! `SQLite`-backed outbox store.

use std::path::Path;
use std::sync::{Mutex, MutexGuard, PoisonError};

use rusqlite::{Connection, Row, params};

use eventsource_core::StreamId;
use eventsource_core::error::OutboxError;
use eventsource_core::outbox::{
    NewOutboxMessage, OutboxId, OutboxMessage, OutboxStatus, OutboxStore,
};

use crate::db;
use crate::error::SqliteError;

const PENDING: &str = "pending";
const PUBLISHED: &str = "published";

/// A durable [`OutboxStore`] on a `SQLite` file.
///
/// It may share a file with a [`SqliteEventStore`](crate::SqliteEventStore).
/// To enqueue messages in the same transaction as an append, use the event
/// store's `append_with_outbox` instead of [`OutboxStore::enqueue`].
pub struct SqliteOutboxStore {
    conn: Mutex<Connection>,
}

impl SqliteOutboxStore {
    /// Opens the database at `path`, creating the file and schema if needed.
    ///
    /// # Errors
    /// [`OutboxError::Backend`] if the file cannot be opened or initialised.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, OutboxError> {
        let conn = db::open(path.as_ref())?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    fn lock(&self) -> MutexGuard<'_, Connection> {
        self.conn.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Inserts `message` as pending on `conn` (or a transaction deref'd to it).
pub(crate) fn insert(
    conn: &Connection,
    message: &NewOutboxMessage,
) -> Result<OutboxId, OutboxError> {
    message.validate()?;
    conn.execute(
        "INSERT INTO outbox (destination, payload, stream_id) VALUES (?1, ?2, ?3)",
        params![
            message.destination,
            message.payload.to_string(),
            message.stream.as_ref().map(ToString::to_string),
        ],
    )
    .map_err(SqliteError::from)?;
    let id = u64::try_from(conn.last_insert_rowid()).map_err(|e| SqliteError::corrupt("id", e))?;
    Ok(OutboxId::new(id))
}

fn to_i64(id: OutboxId) -> i64 {
    i64::try_from(id.get()).unwrap_or(i64::MAX)
}

fn message_from_row(row: &Row<'_>) -> Result<OutboxMessage, SqliteError> {
    let id: i64 = row.get("id")?;
    let payload: String = row.get("payload")?;
    let stream: Option<String> = row.get("stream_id")?;
    let status: String = row.get("status")?;
    Ok(OutboxMessage {
        id: OutboxId::new(u64::try_from(id).map_err(|e| SqliteError::corrupt("id", e))?),
        destination: row.get("destination")?,
        payload: serde_json::from_str(&payload).map_err(|e| SqliteError::corrupt("payload", e))?,
        stream: stream
            .map(|text| text.parse::<StreamId>())
            .transpose()
            .map_err(|e| SqliteError::corrupt("stream_id", e))?,
        attempts: row.get("attempts")?,
        status: match status.as_str() {
            PENDING => OutboxStatus::Pending,
            PUBLISHED => OutboxStatus::Published,
            other => return Err(SqliteError::corrupt("status", other)),
        },
    })
}

impl OutboxStore for SqliteOutboxStore {
    async fn enqueue(&self, message: NewOutboxMessage) -> Result<OutboxId, OutboxError> {
        insert(&self.lock(), &message)
    }

    async fn fetch_pending(&self, limit: usize) -> Result<Vec<OutboxMessage>, OutboxError> {
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let conn = self.lock();
        let mut statement = conn
            .prepare_cached(
                "SELECT id, destination, payload, stream_id, status, attempts FROM outbox \
                 WHERE status = ?1 ORDER BY id LIMIT ?2",
            )
            .map_err(SqliteError::from)?;
        let mut rows = statement
            .query(params![PENDING, limit])
            .map_err(SqliteError::from)?;
        let mut messages = Vec::new();
        while let Some(row) = rows.next().map_err(SqliteError::from)? {
            messages.push(message_from_row(row)?);
        }
        Ok(messages)
    }

    async fn mark_published(&self, id: OutboxId) -> Result<(), OutboxError> {
        self.lock()
            .execute(
                "UPDATE outbox SET status = ?1 WHERE id = ?2",
                params![PUBLISHED, to_i64(id)],
            )
            .map_err(SqliteError::from)?;
        Ok(())
    }

    async fn mark_failed(&self, id: OutboxId, error: &str) -> Result<(), OutboxError> {
        self.lock()
            .execute(
                "UPDATE outbox SET attempts = attempts + 1, last_error = ?1 WHERE id = ?2",
                params![error, to_i64(id)],
            )
            .map_err(SqliteError::from)?;
        Ok(())
    }
}
