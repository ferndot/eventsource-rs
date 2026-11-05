//! Connection setup and schema shared by the `PostgreSQL` adapters.

use tokio_postgres::{Client, NoTls};

use crate::error::PostgresError;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS streams (
    stream_id TEXT PRIMARY KEY,
    version   BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
    position       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id       TEXT   NOT NULL UNIQUE,
    stream_id      TEXT   NOT NULL REFERENCES streams (stream_id),
    category       TEXT   NOT NULL,
    stream_version BIGINT NOT NULL,
    event_type     TEXT   NOT NULL,
    schema_version BIGINT NOT NULL,
    occurred_at    TEXT   NOT NULL,
    correlation_id TEXT   NOT NULL,
    causation_id   TEXT,
    tenant_id      TEXT,
    actor_id       TEXT,
    payload        TEXT   NOT NULL,
    stored_at      TEXT   NOT NULL,
    UNIQUE (stream_id, stream_version)
);
CREATE INDEX IF NOT EXISTS events_category ON events (category, position);
CREATE INDEX IF NOT EXISTS events_tenant ON events (tenant_id, position);
";

/// Opens a connection that works inside `schema`, creating the schema and tables if needed.
///
/// Initialisation runs under a database-wide advisory lock, because concurrent
/// `CREATE ... IF NOT EXISTS` statements can otherwise collide in the catalog.
pub(crate) async fn connect(url: &str, schema: &str) -> Result<Client, PostgresError> {
    if schema.is_empty() || !schema.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(PostgresError::InvalidSchema(schema.to_owned()));
    }
    let (client, connection) = tokio_postgres::connect(url, NoTls).await?;
    // The driver ends when the client is dropped; a failure surfaces on the next query.
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!(
            "BEGIN;
             SELECT pg_advisory_xact_lock(hashtextextended('eventsource:init', 0));
             CREATE SCHEMA IF NOT EXISTS \"{schema}\";
             SET search_path TO \"{schema}\";
             {SCHEMA}
             COMMIT;"
        ))
        .await?;
    Ok(client)
}

/// The id that scopes every [`Position`](eventsource_core::Position) of the schema.
///
/// It is minted once and stored, so every handle on the same schema, and every
/// reopen, agrees on it.
pub(crate) async fn store_id(client: &Client) -> Result<String, PostgresError> {
    client
        .execute(
            "INSERT INTO meta (key, value) VALUES ('store_id', $1) ON CONFLICT (key) DO NOTHING",
            &[&uuid::Uuid::new_v4().to_string()],
        )
        .await?;
    let row = client
        .query_one("SELECT value FROM meta WHERE key = 'store_id'", &[])
        .await?;
    Ok(row.get(0))
}
