//! `SQLite`-backed checkpoint store.

use std::path::Path;
use std::sync::{Mutex, MutexGuard, PoisonError};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

use eventsource_core::Position;
use eventsource_core::checkpoint::{Checkpoint, CheckpointStore};
use eventsource_core::error::CheckpointError;

use crate::db;
use crate::error::SqliteError;

/// A durable [`CheckpointStore`] on a `SQLite` file.
///
/// It may share a file with a [`SqliteEventStore`](crate::SqliteEventStore), so
/// a projection can keep its read model and checkpoint in one database.
pub struct SqliteCheckpointStore {
    conn: Mutex<Connection>,
}

impl SqliteCheckpointStore {
    /// Opens the database at `path`, creating the file and schema if needed.
    ///
    /// # Errors
    /// [`CheckpointError::Backend`] if the file cannot be opened or initialised.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, CheckpointError> {
        let conn = db::open(path.as_ref())?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    fn lock(&self) -> MutexGuard<'_, Connection> {
        self.conn.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn load_blocking(&self, name: &str) -> Result<Option<Checkpoint>, SqliteError> {
        let conn = self.lock();
        let row = conn
            .query_row(
                "SELECT position_store, position_token, last_event_id, events_processed \
                 FROM checkpoints WHERE name = ?1",
                [name],
                |row| {
                    Ok((
                        row.get::<_, Option<String>>(0)?,
                        row.get::<_, Option<i64>>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )
            .optional()?;
        let Some((store, token, last_event_id, processed)) = row else {
            return Ok(None);
        };
        let position = match (store, token) {
            (Some(store), Some(token)) => {
                let token =
                    u64::try_from(token).map_err(|e| SqliteError::corrupt("position_token", e))?;
                Some(Position::new(store, token))
            }
            (None, None) => None,
            _ => {
                return Err(SqliteError::corrupt(
                    "position_token",
                    "position store and token must be set together",
                ));
            }
        };
        Ok(Some(Checkpoint {
            position,
            last_event_id: last_event_id
                .map(|text| text.parse())
                .transpose()
                .map_err(|e| SqliteError::corrupt("last_event_id", e))?,
            events_processed: u64::try_from(processed)
                .map_err(|e| SqliteError::corrupt("events_processed", e))?,
        }))
    }

    fn save_blocking(&self, name: &str, checkpoint: &Checkpoint) -> Result<(), CheckpointError> {
        let mut conn = self.lock();
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(SqliteError::from)?;
        let stored: Option<i64> = tx
            .query_row(
                "SELECT events_processed FROM checkpoints WHERE name = ?1",
                [name],
                |row| row.get(0),
            )
            .optional()
            .map_err(SqliteError::from)?;
        let attempted = to_i64(checkpoint.events_processed);
        if let Some(stored) = stored
            && attempted < stored
        {
            return Err(CheckpointError::Regressed {
                name: name.to_owned(),
                stored: u64::try_from(stored).unwrap_or(0),
                attempted: checkpoint.events_processed,
            });
        }
        tx.execute(
            "INSERT INTO checkpoints \
             (name, position_store, position_token, last_event_id, events_processed) \
             VALUES (?1, ?2, ?3, ?4, ?5) \
             ON CONFLICT (name) DO UPDATE SET \
             position_store = excluded.position_store, \
             position_token = excluded.position_token, \
             last_event_id = excluded.last_event_id, \
             events_processed = excluded.events_processed",
            params![
                name,
                checkpoint.position.as_ref().map(Position::store_id),
                checkpoint.position.as_ref().map(|p| to_i64(p.token())),
                checkpoint.last_event_id.map(|id| id.to_string()),
                attempted,
            ],
        )
        .map_err(SqliteError::from)?;
        tx.commit().map_err(SqliteError::from)?;
        Ok(())
    }
}

impl CheckpointStore for SqliteCheckpointStore {
    async fn load(&self, name: &str) -> Result<Option<Checkpoint>, CheckpointError> {
        Ok(self.load_blocking(name)?)
    }

    async fn save(&self, name: &str, checkpoint: Checkpoint) -> Result<(), CheckpointError> {
        self.save_blocking(name, &checkpoint)
    }
}

/// `SQLite` integers are signed; clamp instead of wrapping.
fn to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}
