//! `PostgreSQL`-backed snapshot store.

use chrono::{DateTime, SecondsFormat, Utc};
use futures::lock::Mutex;
use tokio_postgres::Client;

use eventsource_core::StreamId;
use eventsource_core::Version;
use eventsource_core::error::SnapshotError;
use eventsource_core::snapshot::{Snapshot, SnapshotStore};

use crate::db;
use crate::error::PostgresError;

/// A durable [`SnapshotStore`] on a `PostgreSQL` schema.
///
/// It may share a schema with a [`PostgresEventStore`](crate::PostgresEventStore), so
/// an aggregate's events and its snapshots live in one database. One row is
/// kept per stream: the newest snapshot saved.
pub struct PostgresSnapshotStore {
    client: Mutex<Client>,
}

impl PostgresSnapshotStore {
    /// Connects to `url`, creating `schema` and its tables if needed.
    ///
    /// # Errors
    /// [`SnapshotError::Backend`] if the connection or initialisation fails.
    pub async fn open(url: &str, schema: &str) -> Result<Self, SnapshotError> {
        let client = db::connect(url, schema).await?;
        Ok(Self {
            client: Mutex::new(client),
        })
    }

    async fn save_inner(&self, snapshot: &Snapshot) -> Result<(), PostgresError> {
        // Newest wins: an equal or newer version replaces the row, an older one is a no-op.
        self.client
            .lock()
            .await
            .execute(
                "INSERT INTO snapshots \
                 (stream_id, version, schema_version, state, created_at) \
                 VALUES ($1, $2, $3, $4, $5) \
                 ON CONFLICT (stream_id) DO UPDATE SET \
                 version = excluded.version, \
                 schema_version = excluded.schema_version, \
                 state = excluded.state, \
                 created_at = excluded.created_at \
                 WHERE excluded.version >= snapshots.version",
                &[
                    &snapshot.stream.to_string(),
                    &i64::try_from(snapshot.version.get()).unwrap_or(i64::MAX),
                    &i64::from(snapshot.schema_version),
                    &snapshot.state.to_string(),
                    &snapshot
                        .created_at
                        .to_rfc3339_opts(SecondsFormat::Nanos, true),
                ],
            )
            .await?;
        Ok(())
    }

    async fn load_inner(&self, stream: &StreamId) -> Result<Option<Snapshot>, PostgresError> {
        let row = self
            .client
            .lock()
            .await
            .query_opt(
                "SELECT version, schema_version, state, created_at \
                 FROM snapshots WHERE stream_id = $1",
                &[&stream.to_string()],
            )
            .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let (version, schema_version): (i64, i64) = (row.get(0), row.get(1));
        let (state, created_at): (String, String) = (row.get(2), row.get(3));
        Ok(Some(Snapshot {
            stream: stream.clone(),
            version: u64::try_from(version)
                .map(Version::new)
                .map_err(|e| PostgresError::corrupt("version", e))?,
            schema_version: u32::try_from(schema_version)
                .map_err(|e| PostgresError::corrupt("schema_version", e))?,
            state: serde_json::from_str(&state).map_err(|e| PostgresError::corrupt("state", e))?,
            created_at: DateTime::parse_from_rfc3339(&created_at)
                .map(|time| time.with_timezone(&Utc))
                .map_err(|e| PostgresError::corrupt("created_at", e))?,
        }))
    }
}

impl SnapshotStore for PostgresSnapshotStore {
    async fn save(&self, snapshot: Snapshot) -> Result<(), SnapshotError> {
        Ok(self.save_inner(&snapshot).await?)
    }

    async fn load(&self, stream: &StreamId) -> Result<Option<Snapshot>, SnapshotError> {
        Ok(self.load_inner(stream).await?)
    }
}
