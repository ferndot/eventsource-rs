//! `PostgreSQL`-backed outbox store.

use futures::lock::Mutex;
use tokio_postgres::{Client, GenericClient, Row};

use eventsource_core::StreamId;
use eventsource_core::error::OutboxError;
use eventsource_core::outbox::{
    NewOutboxMessage, OutboxId, OutboxMessage, OutboxStatus, OutboxStore,
};

use crate::db;
use crate::error::PostgresError;

const PENDING: &str = "pending";
const PUBLISHED: &str = "published";

/// A durable [`OutboxStore`] on a `PostgreSQL` schema.
///
/// It may share a schema with a [`PostgresEventStore`](crate::PostgresEventStore).
/// To enqueue messages in the same transaction as an append, use the event
/// store's `append_with_outbox` instead of [`OutboxStore::enqueue`].
pub struct PostgresOutboxStore {
    client: Mutex<Client>,
}

impl PostgresOutboxStore {
    /// Connects to `url`, creating `schema` and its tables if needed.
    ///
    /// # Errors
    /// [`OutboxError::Backend`] if the connection or initialisation fails.
    pub async fn open(url: &str, schema: &str) -> Result<Self, OutboxError> {
        let client = db::connect(url, schema).await?;
        Ok(Self {
            client: Mutex::new(client),
        })
    }
}

/// Inserts `message` as pending on `client` (a connection or a transaction).
pub(crate) async fn insert(
    client: &impl GenericClient,
    message: &NewOutboxMessage,
) -> Result<OutboxId, OutboxError> {
    message.validate()?;
    let row = client
        .query_one(
            "INSERT INTO outbox (destination, payload, stream_id) VALUES ($1, $2, $3) \
             RETURNING id",
            &[
                &message.destination,
                &message.payload.to_string(),
                &message.stream.as_ref().map(ToString::to_string),
            ],
        )
        .await
        .map_err(PostgresError::from)?;
    let id = u64::try_from(row.get::<_, i64>(0)).map_err(|e| PostgresError::corrupt("id", e))?;
    Ok(OutboxId::new(id))
}

fn to_i64(id: OutboxId) -> i64 {
    i64::try_from(id.get()).unwrap_or(i64::MAX)
}

fn message_from_row(row: &Row) -> Result<OutboxMessage, PostgresError> {
    let payload: String = row.get("payload");
    let stream: Option<String> = row.get("stream_id");
    let status: String = row.get("status");
    Ok(OutboxMessage {
        id: OutboxId::new(
            u64::try_from(row.get::<_, i64>("id")).map_err(|e| PostgresError::corrupt("id", e))?,
        ),
        destination: row.get("destination"),
        payload: serde_json::from_str(&payload).map_err(|e| PostgresError::corrupt("payload", e))?,
        stream: stream
            .map(|text| text.parse::<StreamId>())
            .transpose()
            .map_err(|e| PostgresError::corrupt("stream_id", e))?,
        attempts: u32::try_from(row.get::<_, i64>("attempts"))
            .map_err(|e| PostgresError::corrupt("attempts", e))?,
        status: match status.as_str() {
            PENDING => OutboxStatus::Pending,
            PUBLISHED => OutboxStatus::Published,
            other => return Err(PostgresError::corrupt("status", other)),
        },
    })
}

impl OutboxStore for PostgresOutboxStore {
    async fn enqueue(&self, message: NewOutboxMessage) -> Result<OutboxId, OutboxError> {
        insert(&*self.client.lock().await, &message).await
    }

    async fn fetch_pending(&self, limit: usize) -> Result<Vec<OutboxMessage>, OutboxError> {
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let rows = self
            .client
            .lock()
            .await
            .query(
                "SELECT id, destination, payload, stream_id, status, attempts FROM outbox \
                 WHERE status = $1 ORDER BY id LIMIT $2",
                &[&PENDING, &limit],
            )
            .await
            .map_err(PostgresError::from)?;
        Ok(rows
            .iter()
            .map(message_from_row)
            .collect::<Result<Vec<_>, _>>()?)
    }

    async fn mark_published(&self, id: OutboxId) -> Result<(), OutboxError> {
        self.client
            .lock()
            .await
            .execute(
                "UPDATE outbox SET status = $1 WHERE id = $2",
                &[&PUBLISHED, &to_i64(id)],
            )
            .await
            .map_err(PostgresError::from)?;
        Ok(())
    }

    async fn mark_failed(&self, id: OutboxId, error: &str) -> Result<(), OutboxError> {
        self.client
            .lock()
            .await
            .execute(
                "UPDATE outbox SET attempts = attempts + 1, last_error = $1 WHERE id = $2",
                &[&error, &to_i64(id)],
            )
            .await
            .map_err(PostgresError::from)?;
        Ok(())
    }
}
