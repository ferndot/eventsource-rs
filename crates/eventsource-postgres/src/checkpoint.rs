//! `PostgreSQL`-backed checkpoint store.

use futures::lock::Mutex;
use tokio_postgres::Client;

use eventsource_core::Position;
use eventsource_core::checkpoint::{Checkpoint, CheckpointStore};
use eventsource_core::error::CheckpointError;

use crate::db;
use crate::error::PostgresError;

/// A durable [`CheckpointStore`] on a `PostgreSQL` schema.
///
/// It may share a schema with a [`PostgresEventStore`](crate::PostgresEventStore),
/// so a projection can keep its read model and checkpoint in one database.
pub struct PostgresCheckpointStore {
    client: Mutex<Client>,
}

impl PostgresCheckpointStore {
    /// Connects to `url`, creating `schema` and its tables if needed.
    ///
    /// # Errors
    /// [`CheckpointError::Backend`] if the connection or initialisation fails.
    pub async fn open(url: &str, schema: &str) -> Result<Self, CheckpointError> {
        let client = db::connect(url, schema).await?;
        Ok(Self {
            client: Mutex::new(client),
        })
    }

    async fn load_inner(&self, name: &str) -> Result<Option<Checkpoint>, PostgresError> {
        let client = self.client.lock().await;
        let row = client
            .query_opt(
                "SELECT position_store, position_token, last_event_id, events_processed \
                 FROM checkpoints WHERE name = $1",
                &[&name],
            )
            .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let store: Option<String> = row.get(0);
        let token: Option<i64> = row.get(1);
        let last_event_id: Option<String> = row.get(2);
        let processed: i64 = row.get(3);
        let position = match (store, token) {
            (Some(store), Some(token)) => {
                let token =
                    u64::try_from(token).map_err(|e| PostgresError::corrupt("position_token", e))?;
                Some(Position::new(store, token))
            }
            (None, None) => None,
            _ => {
                return Err(PostgresError::corrupt(
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
                .map_err(|e| PostgresError::corrupt("last_event_id", e))?,
            events_processed: u64::try_from(processed)
                .map_err(|e| PostgresError::corrupt("events_processed", e))?,
        }))
    }

    async fn save_inner(&self, name: &str, checkpoint: &Checkpoint) -> Result<(), CheckpointError> {
        let client = self.client.lock().await;
        // One statement, so the regression check and the write cannot be split
        // by another connection: a row that fails the WHERE is left untouched.
        let written = client
            .execute(
                "INSERT INTO checkpoints \
                 (name, position_store, position_token, last_event_id, events_processed) \
                 VALUES ($1, $2, $3, $4, $5) \
                 ON CONFLICT (name) DO UPDATE SET \
                 position_store = excluded.position_store, \
                 position_token = excluded.position_token, \
                 last_event_id = excluded.last_event_id, \
                 events_processed = excluded.events_processed \
                 WHERE excluded.events_processed >= checkpoints.events_processed",
                &[
                    &name,
                    &checkpoint.position.as_ref().map(Position::store_id),
                    &checkpoint.position.as_ref().map(|p| to_i64(p.token())),
                    &checkpoint.last_event_id.map(|id| id.to_string()),
                    &to_i64(checkpoint.events_processed),
                ],
            )
            .await
            .map_err(PostgresError::from)?;
        if written == 0 {
            let stored: i64 = client
                .query_one(
                    "SELECT events_processed FROM checkpoints WHERE name = $1",
                    &[&name],
                )
                .await
                .map_err(PostgresError::from)?
                .get(0);
            return Err(CheckpointError::Regressed {
                name: name.to_owned(),
                stored: u64::try_from(stored).unwrap_or(0),
                attempted: checkpoint.events_processed,
            });
        }
        Ok(())
    }
}

impl CheckpointStore for PostgresCheckpointStore {
    async fn load(&self, name: &str) -> Result<Option<Checkpoint>, CheckpointError> {
        Ok(self.load_inner(name).await?)
    }

    async fn save(&self, name: &str, checkpoint: Checkpoint) -> Result<(), CheckpointError> {
        self.save_inner(name, &checkpoint).await
    }
}

/// `PostgreSQL` integers are signed; clamp instead of wrapping.
fn to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}
