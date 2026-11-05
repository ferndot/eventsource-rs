//! `PostgreSQL`-backed read model store.

use futures::lock::Mutex;
use serde_json::Value;
use tokio_postgres::Client;

use eventsource_core::error::ReadModelError;
use eventsource_core::projection::ReadModelChange;
use eventsource_core::readmodel::ReadModelStore;

use crate::db;
use crate::error::PostgresError;

const UPSERT: &str = "INSERT INTO readmodels (model, id, document) VALUES ($1, $2, $3) \
     ON CONFLICT (model, id) DO UPDATE SET document = excluded.document";
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
                "SELECT document FROM readmodels WHERE model = $1 AND id = $2",
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
                } => tx.execute(UPSERT, &[model, id, &document.to_string()]).await,
                ReadModelChange::Delete { model, id } => tx.execute(DELETE, &[model, id]).await,
            }
            .map_err(PostgresError::from)?;
        }
        tx.commit().await.map_err(PostgresError::from)?;
        Ok(())
    }
}
