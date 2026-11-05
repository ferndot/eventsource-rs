//! `SQLite`-backed read model store.

use std::path::Path;
use std::sync::{Mutex, MutexGuard, PoisonError};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde_json::Value;

use eventsource_core::error::ReadModelError;
use eventsource_core::projection::ReadModelChange;
use eventsource_core::readmodel::ReadModelStore;

use crate::db;
use crate::error::SqliteError;

const UPSERT: &str = "INSERT INTO readmodels (model, id, document) VALUES (?1, ?2, ?3) \
     ON CONFLICT (model, id) DO UPDATE SET document = excluded.document";
const DELETE: &str = "DELETE FROM readmodels WHERE model = ?1 AND id = ?2";

/// A durable [`ReadModelStore`] on a `SQLite` file.
///
/// It may share a file with a [`SqliteEventStore`](crate::SqliteEventStore) and
/// a [`SqliteCheckpointStore`](crate::SqliteCheckpointStore). Documents live in
/// the `readmodels` table, keyed by model and id, stored as JSON text.
pub struct SqliteReadModelStore {
    conn: Mutex<Connection>,
}

impl SqliteReadModelStore {
    /// Opens the database at `path`, creating the file and schema if needed.
    ///
    /// # Errors
    /// [`ReadModelError::Backend`] if the file cannot be opened or initialised.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ReadModelError> {
        let conn = db::open(path.as_ref())?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    fn lock(&self) -> MutexGuard<'_, Connection> {
        self.conn.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn get_blocking(&self, model: &str, id: &str) -> Result<Option<Value>, SqliteError> {
        let conn = self.lock();
        let text: Option<String> = conn
            .query_row(
                "SELECT document FROM readmodels WHERE model = ?1 AND id = ?2",
                params![model, id],
                |row| row.get(0),
            )
            .optional()?;
        text.map(|text| {
            serde_json::from_str(&text).map_err(|e| SqliteError::corrupt("document", e))
        })
        .transpose()
    }
}

impl ReadModelStore for SqliteReadModelStore {
    async fn upsert(&self, model: &str, id: &str, document: Value) -> Result<(), ReadModelError> {
        self.lock()
            .execute(UPSERT, params![model, id, document.to_string()])
            .map_err(SqliteError::from)?;
        Ok(())
    }

    async fn get(&self, model: &str, id: &str) -> Result<Option<Value>, ReadModelError> {
        Ok(self.get_blocking(model, id)?)
    }

    async fn delete(&self, model: &str, id: &str) -> Result<(), ReadModelError> {
        self.lock()
            .execute(DELETE, params![model, id])
            .map_err(SqliteError::from)?;
        Ok(())
    }

    /// Writes the whole batch in one transaction: all of it lands, or none of it.
    async fn apply_changes(&self, changes: &[ReadModelChange]) -> Result<(), ReadModelError> {
        let mut conn = self.lock();
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(SqliteError::from)?;
        for change in changes {
            match change {
                ReadModelChange::Upsert {
                    model,
                    id,
                    document,
                } => tx.execute(UPSERT, params![model, id, document.to_string()]),
                ReadModelChange::Delete { model, id } => tx.execute(DELETE, params![model, id]),
            }
            .map_err(SqliteError::from)?;
        }
        tx.commit().map_err(SqliteError::from)?;
        Ok(())
    }
}
