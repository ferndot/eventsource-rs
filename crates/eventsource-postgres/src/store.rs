//! `PostgreSQL`-backed event store.

use std::num::NonZeroUsize;
use std::sync::Arc;

use chrono::{DateTime, SecondsFormat, Utc};
use futures::lock::{Mutex, MutexGuard};
use tokio_postgres::error::SqlState;
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, Row};

use eventsource_core::envelope::{
    AppendResult, EventEnvelope, FeedReadOptions, ReadDirection, StreamReadOptions,
};
use eventsource_core::error::{AtomicAppendError, PositionError, StoreError};
use eventsource_core::event::{EventMeta, EventRecord};
use eventsource_core::outbox::{NewOutboxMessage, OutboxAppend};
use eventsource_core::store::{AggregateStore, GlobalFeed};
use eventsource_core::{EventRegistry, ExpectedVersion, Position, StreamId, Version};

use crate::db;
use crate::error::PostgresError;

const SELECT_COLUMNS: &str = "position, stream_id, stream_version, event_id, event_type, \
    schema_version, occurred_at, correlation_id, causation_id, tenant_id, actor_id, payload, \
    stored_at";

/// A durable [`AggregateStore`] and [`GlobalFeed`] on a `PostgreSQL` schema.
///
/// Each handle owns one connection behind a lock; calls run to completion.
/// Several handles (or processes) may share one schema: every append runs in a
/// transaction that first takes a schema-wide advisory lock, so writers are
/// serialized by `PostgreSQL`, exactly as `SQLite` serializes them.
pub struct PostgresEventStore {
    store_id: String,
    registry: Option<Arc<EventRegistry>>,
    client: Mutex<Client>,
}

impl PostgresEventStore {
    /// Connects to `url`, creating `schema` and its tables if needed.
    ///
    /// # Errors
    /// [`StoreError::Backend`] if the connection or initialisation fails, or
    /// `schema` is not a plain identifier.
    pub async fn open(url: &str, schema: &str) -> Result<Self, StoreError> {
        let client = db::connect(url, schema).await?;
        let store_id = db::store_id(&client).await?;
        Ok(Self {
            store_id,
            registry: None,
            client: Mutex::new(client),
        })
    }

    /// Rejects appends of event types not in `registry`, and reports stored
    /// events of unknown types as [`StoreError::UnregisteredEventType`] on read.
    #[must_use]
    pub fn with_registry(mut self, registry: Arc<EventRegistry>) -> Self {
        self.registry = Some(registry);
        self
    }

    fn check_registered(&self, event_type: &str) -> Result<(), StoreError> {
        match &self.registry {
            Some(registry) if !registry.contains(event_type) => {
                Err(StoreError::UnregisteredEventType(event_type.to_owned()))
            }
            _ => Ok(()),
        }
    }

    async fn lock(&self) -> MutexGuard<'_, Client> {
        self.client.lock().await
    }

    fn position(&self, token: i64) -> Result<Position, PostgresError> {
        let token = u64::try_from(token).map_err(|e| PostgresError::corrupt("position", e))?;
        Ok(Position::new(self.store_id.clone(), token))
    }

    async fn append_inner(
        &self,
        stream: &StreamId,
        events: Vec<EventRecord>,
        expected: ExpectedVersion,
        messages: &[NewOutboxMessage],
    ) -> Result<AppendResult, AtomicAppendError> {
        if events.is_empty() {
            return Err(StoreError::EmptyAppend.into());
        }
        for record in &events {
            self.check_registered(&record.event_type)?;
        }
        let stream_key = stream.to_string();

        let mut client = self.lock().await;
        let tx = client.transaction().await.map_err(PostgresError::from)?;
        // One writer per schema at a time, until commit. This makes the version
        // check and the insert one atomic step, and it makes positions (taken
        // from a sequence) become visible in order, so a feed reader never
        // skips an event that commits late.
        tx.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended('eventsource:append:' || current_schema(), 0))",
            &[],
        )
        .await
        .map_err(PostgresError::from)?;

        let current = tx
            .query_opt(
                "SELECT version FROM streams WHERE stream_id = $1",
                &[&stream_key],
            )
            .await
            .map_err(PostgresError::from)?
            .map_or(Ok(Version::ZERO), |row| to_version(row.get(0), "version"))?;
        if !expected.is_satisfied_by(current) {
            return Err(StoreError::ConcurrencyConflict {
                stream: stream.clone(),
                expected,
                actual: current,
            }
            .into());
        }

        let newest: Option<String> = tx
            .query_opt(
                "SELECT stored_at FROM events ORDER BY position DESC LIMIT 1",
                &[],
            )
            .await
            .map_err(PostgresError::from)?
            .map(|row| row.get(0));
        let mut stored_at = Utc::now();
        if let Some(newest) = newest {
            stored_at = stored_at.max(parse_time(&newest, "stored_at")?);
        }
        let stored_at_text = format_time(stored_at);

        let new_version = current.advance(events.len() as u64);
        tx.execute(
            "INSERT INTO streams (stream_id, version) VALUES ($1, $2) \
             ON CONFLICT (stream_id) DO UPDATE SET version = excluded.version",
            &[&stream_key, &to_i64(new_version.get())],
        )
        .await
        .map_err(PostgresError::from)?;

        let insert = tx
            .prepare(
                "INSERT INTO events (event_id, stream_id, category, stream_version, \
                 event_type, schema_version, occurred_at, correlation_id, causation_id, \
                 tenant_id, actor_id, payload, stored_at) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) \
                 RETURNING position",
            )
            .await
            .map_err(PostgresError::from)?;
        let mut envelopes = Vec::with_capacity(events.len());
        let mut version = current;
        for record in events {
            version = version.next();
            let meta = &record.meta;
            let outcome = tx
                .query_one(
                    &insert,
                    &[
                        &meta.event_id.to_string(),
                        &stream_key,
                        &stream.category().as_str(),
                        &to_i64(version.get()),
                        &record.event_type,
                        &i64::from(record.schema_version),
                        &format_time(meta.occurred_at),
                        &meta.correlation_id.to_string(),
                        &meta.causation_id.map(|id| id.to_string()),
                        &meta.tenant_id.map(|id| id.to_string()),
                        &meta.actor_id,
                        &record.payload.to_string(),
                        &stored_at_text,
                    ],
                )
                .await;
            let row = match outcome {
                Ok(row) => row,
                Err(error) if error.code() == Some(&SqlState::UNIQUE_VIOLATION) => {
                    return Err(StoreError::DuplicateEvent(meta.event_id).into());
                }
                Err(other) => return Err(StoreError::from(PostgresError::from(other)).into()),
            };
            envelopes.push(EventEnvelope {
                stream: stream.clone(),
                stream_version: version,
                position: self.position(row.get(0))?,
                stored_at,
                record,
            });
        }
        // Same transaction as the events: if a message is rejected, dropping
        // `tx` rolls the events back.
        for message in messages {
            crate::outbox::insert(&tx, message).await?;
        }
        tx.commit().await.map_err(PostgresError::from)?;

        Ok(AppendResult {
            stream: stream.clone(),
            new_version,
            envelopes,
        })
    }

    async fn query_envelopes(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<EventEnvelope>, StoreError> {
        let rows = {
            let client = self.lock().await;
            client.query(sql, params).await.map_err(PostgresError::from)?
        };
        let envelopes = rows
            .iter()
            .map(|row| self.envelope_from_row(row))
            .collect::<Result<Vec<_>, PostgresError>>()?;
        for envelope in &envelopes {
            self.check_registered(&envelope.record.event_type)?;
        }
        Ok(envelopes)
    }

    fn envelope_from_row(&self, row: &Row) -> Result<EventEnvelope, PostgresError> {
        let stream: StreamId = row
            .get::<_, String>("stream_id")
            .parse()
            .map_err(|e| PostgresError::corrupt("stream_id", e))?;
        let payload: String = row.get("payload");
        let schema_version: i64 = row.get("schema_version");
        let record = EventRecord {
            meta: EventMeta {
                event_id: parse_id(row, "event_id")?,
                occurred_at: parse_time(&row.get::<_, String>("occurred_at"), "occurred_at")?,
                correlation_id: parse_id(row, "correlation_id")?,
                causation_id: row
                    .get::<_, Option<String>>("causation_id")
                    .map(|text| text.parse())
                    .transpose()
                    .map_err(|e| PostgresError::corrupt("causation_id", e))?,
                tenant_id: row
                    .get::<_, Option<String>>("tenant_id")
                    .map(|text| text.parse())
                    .transpose()
                    .map_err(|e| PostgresError::corrupt("tenant_id", e))?,
                actor_id: row.get("actor_id"),
            },
            event_type: row.get("event_type"),
            schema_version: u32::try_from(schema_version)
                .map_err(|e| PostgresError::corrupt("schema_version", e))?,
            payload: serde_json::from_str(&payload)
                .map_err(|e| PostgresError::corrupt("payload", e))?,
        };
        Ok(EventEnvelope {
            record,
            stream,
            stream_version: to_version(row.get("stream_version"), "stream_version")?,
            position: self.position(row.get("position"))?,
            stored_at: parse_time(&row.get::<_, String>("stored_at"), "stored_at")?,
        })
    }
}

impl AggregateStore for PostgresEventStore {
    async fn append(
        &self,
        stream: &StreamId,
        events: Vec<EventRecord>,
        expected: ExpectedVersion,
    ) -> Result<AppendResult, StoreError> {
        self.append_inner(stream, events, expected, &[])
            .await
            .map_err(|error| match error {
                AtomicAppendError::Store(error) => error,
                // No messages were passed, so the outbox cannot have failed.
                AtomicAppendError::Outbox(error) => StoreError::Backend(Box::new(error)),
            })
    }

    async fn read_stream(
        &self,
        stream: &StreamId,
        options: StreamReadOptions,
    ) -> Result<Vec<EventEnvelope>, StoreError> {
        let order = match options.direction {
            ReadDirection::Forward => "ASC",
            ReadDirection::Backward => "DESC",
        };
        let sql = format!(
            "SELECT {SELECT_COLUMNS} FROM events \
             WHERE stream_id = $1 AND stream_version >= $2 AND stream_version <= $3 \
             ORDER BY stream_version {order} LIMIT $4"
        );
        self.query_envelopes(
            &sql,
            &[
                &stream.to_string(),
                &options.from.map_or(0, |v| to_i64(v.get())),
                &options.to.map_or(i64::MAX, |v| to_i64(v.get())),
                &limit_param(options.limit),
            ],
        )
        .await
    }

    async fn stream_version(&self, stream: &StreamId) -> Result<Version, StoreError> {
        let client = self.lock().await;
        let row = client
            .query_opt(
                "SELECT version FROM streams WHERE stream_id = $1",
                &[&stream.to_string()],
            )
            .await
            .map_err(PostgresError::from)?;
        Ok(row.map_or(Ok(Version::ZERO), |row| to_version(row.get(0), "version"))?)
    }
}

impl OutboxAppend for PostgresEventStore {
    async fn append_with_outbox(
        &self,
        stream: &StreamId,
        events: Vec<EventRecord>,
        expected: ExpectedVersion,
        messages: Vec<NewOutboxMessage>,
    ) -> Result<AppendResult, AtomicAppendError> {
        self.append_inner(stream, events, expected, &messages).await
    }
}

impl GlobalFeed for PostgresEventStore {
    async fn read_all(
        &self,
        from: Option<&Position>,
        options: FeedReadOptions,
    ) -> Result<Vec<EventEnvelope>, StoreError> {
        let after = match from {
            Some(position) => {
                if position.store_id() != self.store_id {
                    return Err(PositionError::Foreign {
                        left: self.store_id.clone(),
                        right: position.store_id().to_owned(),
                    }
                    .into());
                }
                to_i64(position.token())
            }
            None => 0,
        };
        let sql = format!(
            "SELECT {SELECT_COLUMNS} FROM events \
             WHERE position > $1 \
             AND ($2::text IS NULL OR tenant_id = $2) \
             AND ($3::text IS NULL OR category = $3) \
             ORDER BY position ASC LIMIT $4"
        );
        self.query_envelopes(
            &sql,
            &[
                &after,
                &options.tenant_id.map(|t| t.to_string()),
                &options.category.as_ref().map(|c| c.as_str().to_owned()),
                &limit_param(options.limit),
            ],
        )
        .await
    }

    async fn current_position(&self) -> Result<Option<Position>, StoreError> {
        let client = self.lock().await;
        let row = client
            .query_one("SELECT MAX(position) FROM events", &[])
            .await
            .map_err(PostgresError::from)?;
        let head: Option<i64> = row.get(0);
        Ok(head.map(|token| self.position(token)).transpose()?)
    }
}

/// `PostgreSQL` integers are signed; clamp instead of wrapping.
fn to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

/// A `LIMIT` value; `NULL` means no limit in `PostgreSQL`.
fn limit_param(limit: Option<NonZeroUsize>) -> Option<i64> {
    limit.map(|n| i64::try_from(n.get()).unwrap_or(i64::MAX))
}

fn to_version(value: i64, column: &'static str) -> Result<Version, PostgresError> {
    u64::try_from(value)
        .map(Version::new)
        .map_err(|e| PostgresError::corrupt(column, e))
}

fn format_time(time: DateTime<Utc>) -> String {
    time.to_rfc3339_opts(SecondsFormat::Nanos, true)
}

fn parse_time(text: &str, column: &'static str) -> Result<DateTime<Utc>, PostgresError> {
    DateTime::parse_from_rfc3339(text)
        .map(|time| time.with_timezone(&Utc))
        .map_err(|e| PostgresError::corrupt(column, e))
}

fn parse_id<T>(row: &Row, column: &'static str) -> Result<T, PostgresError>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    row.get::<_, String>(column)
        .parse()
        .map_err(|e: T::Err| PostgresError::corrupt(column, e))
}
