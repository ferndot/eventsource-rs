//! `SQLite`-backed snapshot store.

use std::path::Path;
use std::sync::{Mutex, MutexGuard, PoisonError};

use chrono::{DateTime, SecondsFormat, Utc};
use rusqlite::{Connection, OptionalExtension, params};

use eventsource_core::StreamId;
use eventsource_core::Version;
use eventsource_core::error::SnapshotError;
use eventsource_core::snapshot::{Snapshot, SnapshotStore};

use crate::db;
use crate::error::SqliteError;

/// A durable [`SnapshotStore`] on a `SQLite` file.
///
/// It may share a file with a [`SqliteEventStore`](crate::SqliteEventStore), so
/// an aggregate's events and its snapshots live in one database. One row is
/// kept per stream: the newest snapshot saved.
pub struct SqliteSnapshotStore {
    conn: Mutex<Connection>,
}

impl SqliteSnapshotStore {
    /// Opens the database at `path`, creating the file and schema if needed.
    ///
    /// # Errors
    /// [`SnapshotError::Backend`] if the file cannot be opened or initialised.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, SnapshotError> {
        let conn = db::open(path.as_ref())?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    fn lock(&self) -> MutexGuard<'_, Connection> {
        self.conn.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn save_blocking(&self, snapshot: &Snapshot) -> Result<(), SqliteError> {
        // Newest wins: an equal or newer version replaces the row, an older one is a no-op.
        self.lock().execute(
            "INSERT INTO snapshots \
             (stream_id, version, schema_version, state, created_at) \
             VALUES (?1, ?2, ?3, ?4, ?5) \
             ON CONFLICT (stream_id) DO UPDATE SET \
             version = excluded.version, \
             schema_version = excluded.schema_version, \
             state = excluded.state, \
             created_at = excluded.created_at \
             WHERE excluded.version >= snapshots.version",
            params![
                snapshot.stream.to_string(),
                i64::try_from(snapshot.version.get()).unwrap_or(i64::MAX),
                i64::from(snapshot.schema_version),
                snapshot.state.to_string(),
                snapshot
                    .created_at
                    .to_rfc3339_opts(SecondsFormat::Nanos, true),
            ],
        )?;
        Ok(())
    }

    fn load_blocking(&self, stream: &StreamId) -> Result<Option<Snapshot>, SqliteError> {
        let row = self
            .lock()
            .query_row(
                "SELECT version, schema_version, state, created_at \
                 FROM snapshots WHERE stream_id = ?1",
                [stream.to_string()],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                },
            )
            .optional()?;
        let Some((version, schema_version, state, created_at)) = row else {
            return Ok(None);
        };
        Ok(Some(Snapshot {
            stream: stream.clone(),
            version: u64::try_from(version)
                .map(Version::new)
                .map_err(|e| SqliteError::corrupt("version", e))?,
            schema_version: u32::try_from(schema_version)
                .map_err(|e| SqliteError::corrupt("schema_version", e))?,
            state: serde_json::from_str(&state).map_err(|e| SqliteError::corrupt("state", e))?,
            created_at: DateTime::parse_from_rfc3339(&created_at)
                .map(|time| time.with_timezone(&Utc))
                .map_err(|e| SqliteError::corrupt("created_at", e))?,
        }))
    }
}

impl SnapshotStore for SqliteSnapshotStore {
    async fn save(&self, snapshot: Snapshot) -> Result<(), SnapshotError> {
        Ok(self.save_blocking(&snapshot)?)
    }

    async fn load(&self, stream: &StreamId) -> Result<Option<Snapshot>, SnapshotError> {
        Ok(self.load_blocking(stream)?)
    }
}
