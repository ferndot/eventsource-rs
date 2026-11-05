//! `PostgreSQL`-backed read model store.

use futures::lock::Mutex;
use serde_json::Value;
use tokio_postgres::{Client, Row};

use eventsource_core::error::ReadModelError;
use eventsource_core::projection::ReadModelChange;
use eventsource_core::readmodel::{ReadModelStore, StoredDocument};

use crate::db;
use crate::error::PostgresError;

const UPSERT: &str = "INSERT INTO readmodels (model, id, document) VALUES ($1, $2, $3) \
     ON CONFLICT (model, id) DO UPDATE SET document = excluded.document, \
     version = readmodels.version + 1";
const DELETE: &str = "DELETE FROM readmodels WHERE model = $1 AND id = $2";

/// A durable [`ReadModelStore`] on a `PostgreSQL` schema.
///
/// It may share a schema with a [`PostgresEventStore`](crate::PostgresEventStore) and
/// a [`PostgresCheckpointStore`](crate::PostgresCheckpointStore). Documents live in
/// the `readmodels` table, keyed by model and id, stored as JSON text.
pub struct PostgresReadModelStore {
    client: Mutex<Client>,
}

impl PostgresReadModelStore {
    /// Connects to `url`, creating `schema` and its tables if needed.
    ///
    /// # Errors
    /// [`ReadModelError::Backend`] if the connection or initialisation fails.
    pub async fn open(url: &str, schema: &str) -> Result<Self, ReadModelError> {
        let client = db::connect(url, schema).await?;
        Ok(Self {
            client: Mutex::new(client),
        })
    }

    async fn get_inner(&self, model: &str, id: &str) -> Result<Option<Value>, PostgresError> {
        let row = self
            .client
            .lock()
            .await
            .query_opt(
                "SELECT document FROM readmodels WHERE model = $1 AND id = $2 AND NOT deleted",
                &[&model, &id],
            )
            .await?;
        row.map(|row| {
            serde_json::from_str(&row.get::<_, String>(0))
                .map_err(|e| PostgresError::corrupt("document", e))
        })
        .transpose()
    }
}

const STORED_COLUMNS: &str = "id, version, deleted, document";

fn stored(row: &Row) -> Result<StoredDocument, PostgresError> {
    Ok(StoredDocument {
        id: row.get(0),
        version: u64::try_from(row.get::<_, i64>(1))
            .map_err(|e| PostgresError::corrupt("version", e))?,
        deleted: row.get(2),
        document: serde_json::from_str(&row.get::<_, String>(3))
            .map_err(|e| PostgresError::corrupt("document", e))?,
    })
}

impl PostgresReadModelStore {
    /// Flips the deleted flag, bumping the version; `false` if nothing was in the other state.
    async fn set_deleted(
        &self,
        model: &str,
        id: &str,
        deleted: bool,
    ) -> Result<bool, PostgresError> {
        let changed = self
            .client
            .lock()
            .await
            .execute(
                "UPDATE readmodels SET deleted = $3, version = version + 1 \
                 WHERE model = $1 AND id = $2 AND deleted = $4",
                &[&model, &id, &deleted, &!deleted],
            )
            .await?;
        Ok(changed > 0)
    }
}

impl ReadModelStore for PostgresReadModelStore {
    async fn upsert(&self, model: &str, id: &str, document: Value) -> Result<(), ReadModelError> {
        self.client
            .lock()
            .await
            .execute(UPSERT, &[&model, &id, &document.to_string()])
            .await
            .map_err(PostgresError::from)?;
        Ok(())
    }

    async fn get(&self, model: &str, id: &str) -> Result<Option<Value>, ReadModelError> {
        Ok(self.get_inner(model, id).await?)
    }

    async fn delete(&self, model: &str, id: &str) -> Result<(), ReadModelError> {
        self.client
            .lock()
            .await
            .execute(DELETE, &[&model, &id])
            .await
            .map_err(PostgresError::from)?;
        Ok(())
    }

    /// Writes the whole batch in one transaction: all of it lands, or none of it.
    async fn apply_changes(&self, changes: &[ReadModelChange]) -> Result<(), ReadModelError> {
        let mut client = self.client.lock().await;
        let tx = client.transaction().await.map_err(PostgresError::from)?;
        for change in changes {
            match change {
                ReadModelChange::Upsert {
                    model,
                    id,
                    document,
                } => {
                    tx.execute(UPSERT, &[model, id, &document.to_string()])
                        .await
                }
                ReadModelChange::Delete { model, id } => tx.execute(DELETE, &[model, id]).await,
            }
            .map_err(PostgresError::from)?;
        }
        tx.commit().await.map_err(PostgresError::from)?;
        Ok(())
    }

    async fn put_if_version(
        &self,
        model: &str,
        id: &str,
        document: Value,
        expected_version: u64,
    ) -> Result<u64, ReadModelError> {
        let client = self.client.lock().await;
        let text = document.to_string();
        // One statement decides the write, so racing writers cannot both pass the check.
        let written = match (expected_version, i64::try_from(expected_version)) {
            (0, _) => client
                .execute(
                    "INSERT INTO readmodels (model, id, document) VALUES ($1, $2, $3) \
                     ON CONFLICT (model, id) DO NOTHING",
                    &[&model, &id, &text],
                )
                .await
                .map_err(PostgresError::from)?,
            (_, Ok(expected)) => client
                .execute(
                    "UPDATE readmodels SET document = $3, version = version + 1 \
                     WHERE model = $1 AND id = $2 AND version = $4",
                    &[&model, &id, &text, &expected],
                )
                .await
                .map_err(PostgresError::from)?,
            // Beyond any stored version.
            (_, Err(_)) => 0,
        };
        if written > 0 {
            return Ok(expected_version + 1);
        }
        let actual = client
            .query_opt(
                "SELECT version FROM readmodels WHERE model = $1 AND id = $2",
                &[&model, &id],
            )
            .await
            .map_err(PostgresError::from)?
            .map(|row| u64::try_from(row.get::<_, i64>(0)))
            .transpose()
            .map_err(|e| PostgresError::corrupt("version", e))?
            .unwrap_or(0);
        Err(ReadModelError::VersionConflict {
            model: model.to_owned(),
            id: id.to_owned(),
            expected: expected_version,
            actual,
        })
    }

    async fn fetch(&self, model: &str, id: &str) -> Result<Option<StoredDocument>, ReadModelError> {
        let row = self
            .client
            .lock()
            .await
            .query_opt(
                &format!("SELECT {STORED_COLUMNS} FROM readmodels WHERE model = $1 AND id = $2"),
                &[&model, &id],
            )
            .await
            .map_err(PostgresError::from)?;
        Ok(row.as_ref().map(stored).transpose()?)
    }

    async fn soft_delete(&self, model: &str, id: &str) -> Result<bool, ReadModelError> {
        Ok(self.set_deleted(model, id, true).await?)
    }

    async fn restore(&self, model: &str, id: &str) -> Result<bool, ReadModelError> {
        Ok(self.set_deleted(model, id, false).await?)
    }

    async fn list(&self, model: &str) -> Result<Vec<StoredDocument>, ReadModelError> {
        // COLLATE "C": byte-wise, whatever the database collation.
        let rows = self
            .client
            .lock()
            .await
            .query(
                &format!(
                    "SELECT {STORED_COLUMNS} FROM readmodels WHERE model = $1 \
                     ORDER BY id COLLATE \"C\""
                ),
                &[&model],
            )
            .await
            .map_err(PostgresError::from)?;
        Ok(rows.iter().map(stored).collect::<Result<_, _>>()?)
    }
}
