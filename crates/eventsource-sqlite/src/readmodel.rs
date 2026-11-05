//! `SQLite`-backed read model store.

use std::path::Path;
use std::sync::{Mutex, MutexGuard, PoisonError};

use rusqlite::{Connection, OptionalExtension, Row, TransactionBehavior, params};
use serde_json::Value;

use eventsource_core::error::ReadModelError;
use eventsource_core::projection::ReadModelChange;
use eventsource_core::readmodel::{ReadModelStore, StoredDocument};

use crate::db;
use crate::error::SqliteError;

const UPSERT: &str = "INSERT INTO readmodels (model, id, document) VALUES (?1, ?2, ?3) \
     ON CONFLICT (model, id) DO UPDATE SET document = excluded.document, version = version + 1";
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
                "SELECT document FROM readmodels WHERE model = ?1 AND id = ?2 AND deleted = 0",
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

const STORED_COLUMNS: &str = "id, version, deleted, document";

fn stored(row: &Row<'_>) -> rusqlite::Result<Result<StoredDocument, SqliteError>> {
    let version: i64 = row.get(1)?;
    let text: String = row.get(3)?;
    Ok(build_stored(row.get(0)?, version, row.get(2)?, &text))
}

fn build_stored(
    id: String,
    version: i64,
    deleted: bool,
    text: &str,
) -> Result<StoredDocument, SqliteError> {
    Ok(StoredDocument {
        id,
        version: u64::try_from(version).map_err(|e| SqliteError::corrupt("version", e))?,
        deleted,
        document: serde_json::from_str(text).map_err(|e| SqliteError::corrupt("document", e))?,
    })
}

impl SqliteReadModelStore {
    /// Flips the deleted flag, bumping the version; `false` if nothing was in the other state.
    fn set_deleted(&self, model: &str, id: &str, deleted: bool) -> Result<bool, SqliteError> {
        let changed = self.lock().execute(
            "UPDATE readmodels SET deleted = ?3, version = version + 1 \
             WHERE model = ?1 AND id = ?2 AND deleted = ?4",
            params![model, id, deleted, !deleted],
        )?;
        Ok(changed > 0)
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

    async fn put_if_version(
        &self,
        model: &str,
        id: &str,
        document: Value,
        expected_version: u64,
    ) -> Result<u64, ReadModelError> {
        let mut conn = self.lock();
        // Immediate: take the write lock before reading, so no other handle slips in between.
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(SqliteError::from)?;
        let current: Option<i64> = tx
            .query_row(
                "SELECT version FROM readmodels WHERE model = ?1 AND id = ?2",
                params![model, id],
                |row| row.get(0),
            )
            .optional()
            .map_err(SqliteError::from)?;
        let actual = current
            .map(u64::try_from)
            .transpose()
            .map_err(|e| SqliteError::corrupt("version", e))?
            .unwrap_or(0);
        if actual != expected_version {
            return Err(ReadModelError::VersionConflict {
                model: model.to_owned(),
                id: id.to_owned(),
                expected: expected_version,
                actual,
            });
        }
        tx.execute(UPSERT, params![model, id, document.to_string()])
            .map_err(SqliteError::from)?;
        tx.commit().map_err(SqliteError::from)?;
        Ok(actual + 1)
    }

    async fn fetch(&self, model: &str, id: &str) -> Result<Option<StoredDocument>, ReadModelError> {
        let conn = self.lock();
        let row = conn
            .query_row(
                &format!("SELECT {STORED_COLUMNS} FROM readmodels WHERE model = ?1 AND id = ?2"),
                params![model, id],
                stored,
            )
            .optional()
            .map_err(SqliteError::from)?;
        Ok(row.transpose()?)
    }

    async fn soft_delete(&self, model: &str, id: &str) -> Result<bool, ReadModelError> {
        Ok(self.set_deleted(model, id, true)?)
    }

    async fn restore(&self, model: &str, id: &str) -> Result<bool, ReadModelError> {
        Ok(self.set_deleted(model, id, false)?)
    }

    async fn list(&self, model: &str) -> Result<Vec<StoredDocument>, ReadModelError> {
        let conn = self.lock();
        let mut statement = conn
            .prepare(&format!(
                "SELECT {STORED_COLUMNS} FROM readmodels WHERE model = ?1 ORDER BY id"
            ))
            .map_err(SqliteError::from)?;
        let rows = statement
            .query_map(params![model], stored)
            .map_err(SqliteError::from)?;
        let mut documents = Vec::new();
        for row in rows {
            documents.push(row.map_err(SqliteError::from)??);
        }
        Ok(documents)
    }
}
