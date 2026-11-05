//! `PostgreSQL`-backed dead-letter store.

use chrono::{DateTime, SecondsFormat, Utc};
use futures::lock::Mutex;
use tokio_postgres::Client;

use eventsource_core::dead_letter::{DeadLetterEntry, DeadLetterId, DeadLetterStore, ParkedEvent};
use eventsource_core::error::DeadLetterError;
use eventsource_core::event::EventRecord;
use eventsource_core::{EventEnvelope, Position, StreamId, Version};

use crate::db;
use crate::error::PostgresError;

/// A durable [`DeadLetterStore`] on a `PostgreSQL` schema.
///
/// It may share a schema with a [`PostgresEventStore`](crate::PostgresEventStore) and
/// [`PostgresCheckpointStore`](crate::PostgresCheckpointStore), so parked events
/// live beside the events and checkpoints they came from.
pub struct PostgresDeadLetterStore {
    client: Mutex<Client>,
}

impl PostgresDeadLetterStore {
    /// Connects to `url`, creating `schema` and its tables if needed.
    ///
    /// # Errors
    /// [`DeadLetterError::Backend`] if the connection or initialisation fails.
    pub async fn open(url: &str, schema: &str) -> Result<Self, DeadLetterError> {
        let client = db::connect(url, schema).await?;
        Ok(Self {
            client: Mutex::new(client),
        })
    }

    async fn park_inner(&self, entry: &DeadLetterEntry) -> Result<DeadLetterId, PostgresError> {
        let envelope = &entry.envelope;
        let record = serde_json::to_string(&envelope.record)
            .map_err(|e| PostgresError::corrupt("record", e))?;
        let row = self
            .client
            .lock()
            .await
            .query_one(
                "INSERT INTO dead_letters \
                 (subscription, event_id, stream_id, stream_version, position_store, \
                  position_token, stored_at, record, error, attempts) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) \
                 ON CONFLICT (subscription, event_id) DO UPDATE SET \
                 error = excluded.error, attempts = excluded.attempts \
                 RETURNING id",
                &[
                    &entry.subscription,
                    &envelope.record.meta.event_id.to_string(),
                    &envelope.stream.to_string(),
                    &to_i64(envelope.stream_version.get()),
                    &envelope.position.store_id(),
                    &to_i64(envelope.position.token()),
                    &envelope
                        .stored_at
                        .to_rfc3339_opts(SecondsFormat::Nanos, true),
                    &record,
                    &entry.error,
                    &i64::from(entry.attempts),
                ],
            )
            .await?;
        to_id(row.get(0))
    }

    async fn list_inner(&self, subscription: &str) -> Result<Vec<ParkedEvent>, PostgresError> {
        let rows = self
            .client
            .lock()
            .await
            .query(
                "SELECT id, stream_id, stream_version, position_store, position_token, \
                 stored_at, record, error, attempts \
                 FROM dead_letters WHERE subscription = $1 ORDER BY id",
                &[&subscription],
            )
            .await?;
        let mut parked = Vec::with_capacity(rows.len());
        for row in rows {
            let record: EventRecord = serde_json::from_str(&row.get::<_, String>("record"))
                .map_err(|e| PostgresError::corrupt("record", e))?;
            let stream: StreamId = row
                .get::<_, String>("stream_id")
                .parse()
                .map_err(|e| PostgresError::corrupt("stream_id", e))?;
            parked.push(ParkedEvent {
                id: to_id(row.get("id"))?,
                entry: DeadLetterEntry {
                    subscription: subscription.to_owned(),
                    envelope: EventEnvelope {
                        record,
                        stream,
                        stream_version: Version::new(
                            u64::try_from(row.get::<_, i64>("stream_version"))
                                .map_err(|e| PostgresError::corrupt("stream_version", e))?,
                        ),
                        position: Position::new(
                            row.get::<_, String>("position_store"),
                            u64::try_from(row.get::<_, i64>("position_token"))
                                .map_err(|e| PostgresError::corrupt("position_token", e))?,
                        ),
                        stored_at: DateTime::parse_from_rfc3339(&row.get::<_, String>("stored_at"))
                            .map(|time| time.with_timezone(&Utc))
                            .map_err(|e| PostgresError::corrupt("stored_at", e))?,
                    },
                    error: row.get("error"),
                    attempts: u32::try_from(row.get::<_, i64>("attempts"))
                        .map_err(|e| PostgresError::corrupt("attempts", e))?,
                },
            });
        }
        Ok(parked)
    }

    async fn remove_inner(&self, id: DeadLetterId) -> Result<bool, PostgresError> {
        let removed = self
            .client
            .lock()
            .await
            .execute(
                "DELETE FROM dead_letters WHERE id = $1",
                &[&to_i64(id.get())],
            )
            .await?;
        Ok(removed > 0)
    }
}

impl DeadLetterStore for PostgresDeadLetterStore {
    async fn park(&self, entry: DeadLetterEntry) -> Result<DeadLetterId, DeadLetterError> {
        Ok(self.park_inner(&entry).await?)
    }

    async fn list(&self, subscription: &str) -> Result<Vec<ParkedEvent>, DeadLetterError> {
        Ok(self.list_inner(subscription).await?)
    }

    async fn remove(&self, id: DeadLetterId) -> Result<bool, DeadLetterError> {
        Ok(self.remove_inner(id).await?)
    }
}

/// `PostgreSQL` integers are signed; clamp instead of wrapping.
fn to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn to_id(value: i64) -> Result<DeadLetterId, PostgresError> {
    u64::try_from(value)
        .map(DeadLetterId::new)
        .map_err(|e| PostgresError::corrupt("id", e))
}
