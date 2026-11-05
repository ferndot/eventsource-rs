//! `SQLite`-backed dead-letter store.

use std::path::Path;
use std::sync::{Mutex, MutexGuard, PoisonError};

use chrono::{DateTime, SecondsFormat, Utc};
use rusqlite::{Connection, params};

use eventsource_core::dead_letter::{DeadLetterEntry, DeadLetterId, DeadLetterStore, ParkedEvent};
use eventsource_core::error::DeadLetterError;
use eventsource_core::event::EventRecord;
use eventsource_core::{EventEnvelope, Position, StreamId, Version};

use crate::db;
use crate::error::SqliteError;

/// A durable [`DeadLetterStore`] on a `SQLite` file.
///
/// It may share a file with a [`SqliteEventStore`](crate::SqliteEventStore) and
/// [`SqliteCheckpointStore`](crate::SqliteCheckpointStore), so parked events
/// live beside the events and checkpoints they came from.
pub struct SqliteDeadLetterStore {
    conn: Mutex<Connection>,
}

impl SqliteDeadLetterStore {
    /// Opens the database at `path`, creating the file and schema if needed.
    ///
    /// # Errors
    /// [`DeadLetterError::Backend`] if the file cannot be opened or initialised.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, DeadLetterError> {
        let conn = db::open(path.as_ref())?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    fn lock(&self) -> MutexGuard<'_, Connection> {
        self.conn.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn park_blocking(&self, entry: &DeadLetterEntry) -> Result<DeadLetterId, SqliteError> {
        let envelope = &entry.envelope;
        let record = serde_json::to_string(&envelope.record)
            .map_err(|e| SqliteError::corrupt("record", e))?;
        let conn = self.lock();
        let id: i64 = conn.query_row(
            "INSERT INTO dead_letters \
             (subscription, event_id, stream_id, stream_version, position_store, \
              position_token, stored_at, record, error, attempts) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10) \
             ON CONFLICT (subscription, event_id) DO UPDATE SET \
             error = excluded.error, attempts = excluded.attempts \
             RETURNING id",
            params![
                entry.subscription,
                envelope.record.meta.event_id.to_string(),
                envelope.stream.to_string(),
                to_i64(envelope.stream_version.get()),
                envelope.position.store_id(),
                to_i64(envelope.position.token()),
                envelope
                    .stored_at
                    .to_rfc3339_opts(SecondsFormat::Nanos, true),
                record,
                entry.error,
                i64::from(entry.attempts),
            ],
            |row| row.get(0),
        )?;
        to_id(id)
    }

    fn list_blocking(&self, subscription: &str) -> Result<Vec<ParkedEvent>, SqliteError> {
        let conn = self.lock();
        let mut statement = conn.prepare(
            "SELECT id, stream_id, stream_version, position_store, position_token, \
             stored_at, record, error, attempts \
             FROM dead_letters WHERE subscription = ?1 ORDER BY id",
        )?;
        let rows = statement.query_map([subscription], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, String>(7)?,
                row.get::<_, i64>(8)?,
            ))
        })?;
        let mut parked = Vec::new();
        for row in rows {
            let (id, stream, version, store, token, stored_at, record, error, attempts) = row?;
            let record: EventRecord =
                serde_json::from_str(&record).map_err(|e| SqliteError::corrupt("record", e))?;
            let stream: StreamId = stream
                .parse()
                .map_err(|e| SqliteError::corrupt("stream_id", e))?;
            parked.push(ParkedEvent {
                id: to_id(id)?,
                entry: DeadLetterEntry {
                    subscription: subscription.to_owned(),
                    envelope: EventEnvelope {
                        record,
                        stream,
                        stream_version: Version::new(
                            u64::try_from(version)
                                .map_err(|e| SqliteError::corrupt("stream_version", e))?,
                        ),
                        position: Position::new(
                            store,
                            u64::try_from(token)
                                .map_err(|e| SqliteError::corrupt("position_token", e))?,
                        ),
                        stored_at: DateTime::parse_from_rfc3339(&stored_at)
                            .map(|time| time.with_timezone(&Utc))
                            .map_err(|e| SqliteError::corrupt("stored_at", e))?,
                    },
                    error,
                    attempts: u32::try_from(attempts)
                        .map_err(|e| SqliteError::corrupt("attempts", e))?,
                },
            });
        }
        Ok(parked)
    }

    fn remove_blocking(&self, id: DeadLetterId) -> Result<bool, SqliteError> {
        let removed = self
            .lock()
            .execute("DELETE FROM dead_letters WHERE id = ?1", [to_i64(id.get())])?;
        Ok(removed > 0)
    }
}

impl DeadLetterStore for SqliteDeadLetterStore {
    async fn park(&self, entry: DeadLetterEntry) -> Result<DeadLetterId, DeadLetterError> {
        Ok(self.park_blocking(&entry)?)
    }

    async fn list(&self, subscription: &str) -> Result<Vec<ParkedEvent>, DeadLetterError> {
        Ok(self.list_blocking(subscription)?)
    }

    async fn remove(&self, id: DeadLetterId) -> Result<bool, DeadLetterError> {
        Ok(self.remove_blocking(id)?)
    }
}

/// `SQLite` integers are signed; clamp instead of wrapping.
fn to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn to_id(value: i64) -> Result<DeadLetterId, SqliteError> {
    u64::try_from(value)
        .map(DeadLetterId::new)
        .map_err(|e| SqliteError::corrupt("id", e))
}
