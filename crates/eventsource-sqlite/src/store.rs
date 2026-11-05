//! `SQLite`-backed event store.

use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use chrono::{DateTime, SecondsFormat, Utc};
use rusqlite::{Connection, OptionalExtension, Row, TransactionBehavior, params};

use eventsource_core::envelope::{
    AppendResult, EventEnvelope, FeedReadOptions, ReadDirection, StreamReadOptions,
};
use eventsource_core::error::{AtomicAppendError, PositionError, StoreError};
use eventsource_core::event::{EventMeta, EventRecord};
use eventsource_core::outbox::{NewOutboxMessage, OutboxAppend};
use eventsource_core::store::{AggregateStore, GlobalFeed};
use eventsource_core::{EventRegistry, ExpectedVersion, Position, StreamId, Version};

use crate::db;
use crate::error::SqliteError;

const SELECT_COLUMNS: &str = "position, stream_id, stream_version, event_id, event_type, \
    schema_version, occurred_at, correlation_id, causation_id, tenant_id, actor_id, payload, \
    stored_at";

/// A durable [`AggregateStore`] and [`GlobalFeed`] on a `SQLite` file.
///
/// Each handle owns one connection behind a [`Mutex`]; calls run to completion
/// without awaiting. Several handles (or processes) may share one file: every
/// append runs in a `BEGIN IMMEDIATE` transaction, so writers are serialized by
/// `SQLite` and the version check and the insert are one atomic step.
pub struct SqliteEventStore {
    store_id: String,
    registry: Option<Arc<EventRegistry>>,
    conn: Mutex<Connection>,
}

impl SqliteEventStore {
    /// Opens the database at `path`, creating the file and schema if needed.
    ///
    /// # Errors
    /// [`StoreError::Backend`] if the file cannot be opened or initialised.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let conn = db::open(path.as_ref())?;
        let store_id = db::store_id(&conn)?;
        Ok(Self {
            store_id,
            registry: None,
            conn: Mutex::new(conn),
        })
    }

    /// Rejects appends of event types not in `registry`, and reports stored
    /// events of unknown types as [`StoreError::UnregisteredEventType`] on read.
    #[must_use]
    pub fn with_registry(mut self, registry: Arc<EventRegistry>) -> Self {
        self.registry = Some(registry);
        self
    }

    /// Fails if a registry is set and `event_type` is not in it.
    fn check_registered(&self, event_type: &str) -> Result<(), StoreError> {
        match &self.registry {
            Some(registry) if !registry.contains(event_type) => {
                Err(StoreError::UnregisteredEventType(event_type.to_owned()))
            }
            _ => Ok(()),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Connection> {
        // Transactions roll back on drop, so a panic elsewhere leaves no half-written state.
        self.conn.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn position(&self, token: i64) -> Result<Position, SqliteError> {
        let token = u64::try_from(token).map_err(|e| SqliteError::corrupt("position", e))?;
        Ok(Position::new(self.store_id.clone(), token))
    }

    /// The time to stamp new events with. It never decreases: clamp to the newest stored value.
    fn next_stored_at(tx: &rusqlite::Transaction<'_>) -> Result<DateTime<Utc>, SqliteError> {
        let newest: Option<String> = tx
            .query_row(
                "SELECT stored_at FROM events ORDER BY position DESC LIMIT 1",
                [],
                |row| row.get(0),
            )
            .optional()?;
        let mut stored_at = Utc::now();
        if let Some(newest) = newest {
            stored_at = stored_at.max(parse_time(&newest, "stored_at")?);
        }
        Ok(stored_at)
    }

    fn append_blocking(
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

        let mut conn = self.lock();
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(SqliteError::from)?;

        let current = tx
            .query_row(
                "SELECT version FROM streams WHERE stream_id = ?1",
                [&stream_key],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(SqliteError::from)?
            .map_or(Ok(Version::ZERO), |v| to_version(v, "version"))?;
        if !expected.is_satisfied_by(current) {
            return Err(StoreError::ConcurrencyConflict {
                stream: stream.clone(),
                expected,
                actual: current,
            }
            .into());
        }

        let stored_at = Self::next_stored_at(&tx)?;
        let stored_at_text = format_time(stored_at);

        // The stream row must exist before events reference it.
        let new_version = current.advance(events.len() as u64);
        tx.execute(
            "INSERT INTO streams (stream_id, version) VALUES (?1, ?2) \
             ON CONFLICT (stream_id) DO UPDATE SET version = excluded.version",
            params![stream_key, to_i64(new_version.get())],
        )
        .map_err(SqliteError::from)?;

        let mut envelopes = Vec::with_capacity(events.len());
        {
            let mut insert = tx
                .prepare_cached(
                    "INSERT INTO events (event_id, stream_id, category, stream_version, \
                     event_type, schema_version, occurred_at, correlation_id, causation_id, \
                     tenant_id, actor_id, payload, stored_at) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                )
                .map_err(SqliteError::from)?;
            let mut version = current;
            for record in events {
                version = version.next();
                let meta = &record.meta;
                let outcome = insert.execute(params![
                    meta.event_id.to_string(),
                    stream_key,
                    stream.category().as_str(),
                    to_i64(version.get()),
                    record.event_type,
                    record.schema_version,
                    format_time(meta.occurred_at),
                    meta.correlation_id.to_string(),
                    meta.causation_id.map(|id| id.to_string()),
                    meta.tenant_id.map(|id| id.to_string()),
                    meta.actor_id,
                    record.payload.to_string(),
                    stored_at_text,
                ]);
                match outcome {
                    Ok(_) => {}
                    // Inside an immediate transaction the only constraint that can
                    // fail on this insert is the unique event id.
                    Err(rusqlite::Error::SqliteFailure(code, _))
                        if code.code == rusqlite::ErrorCode::ConstraintViolation =>
                    {
                        return Err(StoreError::DuplicateEvent(meta.event_id).into());
                    }
                    Err(other) => return Err(StoreError::from(SqliteError::from(other)).into()),
                }
                envelopes.push(EventEnvelope {
                    stream: stream.clone(),
                    stream_version: version,
                    position: self.position(tx.last_insert_rowid())?,
                    stored_at,
                    record,
                });
            }
        }
        // Same transaction as the events: if a message is rejected, dropping
        // `tx` below rolls the events back.
        for message in messages {
            crate::outbox::insert(&tx, message)?;
        }
        tx.commit().map_err(SqliteError::from)?;

        Ok(AppendResult {
            stream: stream.clone(),
            new_version,
            envelopes,
        })
    }

    fn query_envelopes(
        &self,
        sql: &str,
        params: &[&dyn rusqlite::ToSql],
    ) -> Result<Vec<EventEnvelope>, StoreError> {
        let conn = self.lock();
        let mut statement = conn.prepare_cached(sql).map_err(SqliteError::from)?;
        let rows = statement
            .query_and_then(params, |row| self.envelope_from_row(row))
            .map_err(SqliteError::from)?;
        let envelopes = rows.collect::<Result<Vec<_>, SqliteError>>()?;
        for envelope in &envelopes {
            self.check_registered(&envelope.record.event_type)?;
        }
        Ok(envelopes)
    }

    fn envelope_from_row(&self, row: &Row<'_>) -> Result<EventEnvelope, SqliteError> {
        let stream: StreamId = row
            .get::<_, String>("stream_id")?
            .parse()
            .map_err(|e| SqliteError::corrupt("stream_id", e))?;
        let payload: String = row.get("payload")?;
        let record = EventRecord {
            meta: EventMeta {
                event_id: parse_id(row, "event_id")?,
                occurred_at: parse_time(&row.get::<_, String>("occurred_at")?, "occurred_at")?,
                correlation_id: parse_id(row, "correlation_id")?,
                causation_id: row
                    .get::<_, Option<String>>("causation_id")?
                    .map(|text| text.parse())
                    .transpose()
                    .map_err(|e| SqliteError::corrupt("causation_id", e))?,
                tenant_id: row
                    .get::<_, Option<String>>("tenant_id")?
                    .map(|text| text.parse())
                    .transpose()
                    .map_err(|e| SqliteError::corrupt("tenant_id", e))?,
                actor_id: row.get("actor_id")?,
            },
            event_type: row.get("event_type")?,
            schema_version: row.get("schema_version")?,
            payload: serde_json::from_str(&payload)
                .map_err(|e| SqliteError::corrupt("payload", e))?,
        };
        Ok(EventEnvelope {
            record,
            stream,
            stream_version: to_version(row.get("stream_version")?, "stream_version")?,
            position: self.position(row.get("position")?)?,
            stored_at: parse_time(&row.get::<_, String>("stored_at")?, "stored_at")?,
        })
    }
}

impl AggregateStore for SqliteEventStore {
    async fn append(
        &self,
        stream: &StreamId,
        events: Vec<EventRecord>,
        expected: ExpectedVersion,
    ) -> Result<AppendResult, StoreError> {
        self.append_blocking(stream, events, expected, &[])
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
             WHERE stream_id = ?1 AND stream_version >= ?2 AND stream_version <= ?3 \
             ORDER BY stream_version {order} LIMIT ?4"
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
    }

    async fn stream_version(&self, stream: &StreamId) -> Result<Version, StoreError> {
        let conn = self.lock();
        let version = conn
            .query_row(
                "SELECT version FROM streams WHERE stream_id = ?1",
                [stream.to_string()],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(SqliteError::from)?;
        Ok(version.map_or(Ok(Version::ZERO), |v| to_version(v, "version"))?)
    }
}

impl OutboxAppend for SqliteEventStore {
    async fn append_with_outbox(
        &self,
        stream: &StreamId,
        events: Vec<EventRecord>,
        expected: ExpectedVersion,
        messages: Vec<NewOutboxMessage>,
    ) -> Result<AppendResult, AtomicAppendError> {
        self.append_blocking(stream, events, expected, &messages)
    }
}

impl GlobalFeed for SqliteEventStore {
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
             WHERE position > ?1 \
             AND (?2 IS NULL OR tenant_id = ?2) \
             AND (?3 IS NULL OR category = ?3) \
             ORDER BY position ASC LIMIT ?4"
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
    }

    async fn current_position(&self) -> Result<Option<Position>, StoreError> {
        let conn = self.lock();
        let head: Option<i64> = conn
            .query_row("SELECT MAX(position) FROM events", [], |row| row.get(0))
            .map_err(SqliteError::from)?;
        Ok(head.map(|token| self.position(token)).transpose()?)
    }
}

/// `SQLite` integers are signed; clamp instead of wrapping.
fn to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

/// A `LIMIT` value; `-1` means no limit in `SQLite`.
fn limit_param(limit: Option<NonZeroUsize>) -> i64 {
    limit.map_or(-1, |n| i64::try_from(n.get()).unwrap_or(i64::MAX))
}

fn to_version(value: i64, column: &'static str) -> Result<Version, SqliteError> {
    u64::try_from(value)
        .map(Version::new)
        .map_err(|e| SqliteError::corrupt(column, e))
}

fn format_time(time: DateTime<Utc>) -> String {
    time.to_rfc3339_opts(SecondsFormat::Nanos, true)
}

fn parse_time(text: &str, column: &'static str) -> Result<DateTime<Utc>, SqliteError> {
    DateTime::parse_from_rfc3339(text)
        .map(|time| time.with_timezone(&Utc))
        .map_err(|e| SqliteError::corrupt(column, e))
}

fn parse_id<T>(row: &Row<'_>, column: &'static str) -> Result<T, SqliteError>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    row.get::<_, String>(column)?
        .parse()
        .map_err(|e: T::Err| SqliteError::corrupt(column, e))
}
