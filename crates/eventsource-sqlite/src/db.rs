//! Connection setup and schema shared by the `SQLite` adapters.

use std::path::Path;
use std::time::Duration;

use rusqlite::Connection;

use crate::error::SqliteError;

/// How long a connection waits for another writer before giving up.
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS streams (
    stream_id TEXT PRIMARY KEY,
    version   INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
    position       INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id       TEXT    NOT NULL UNIQUE,
    stream_id      TEXT    NOT NULL REFERENCES streams (stream_id),
    category       TEXT    NOT NULL,
    stream_version INTEGER NOT NULL,
    event_type     TEXT    NOT NULL,
    schema_version INTEGER NOT NULL,
    occurred_at    TEXT    NOT NULL,
    correlation_id TEXT    NOT NULL,
    causation_id   TEXT,
    tenant_id      TEXT,
    actor_id       TEXT,
    payload        TEXT    NOT NULL,
    stored_at      TEXT    NOT NULL,
    UNIQUE (stream_id, stream_version)
);
CREATE INDEX IF NOT EXISTS events_category ON events (category, position);
CREATE INDEX IF NOT EXISTS events_tenant ON events (tenant_id, position);
CREATE TABLE IF NOT EXISTS checkpoints (
    name             TEXT PRIMARY KEY,
    position_store   TEXT,
    position_token   INTEGER,
    last_event_id    TEXT,
    events_processed INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS snapshots (
    stream_id      TEXT PRIMARY KEY,
    version        INTEGER NOT NULL,
    schema_version INTEGER NOT NULL,
    state          TEXT    NOT NULL,
    created_at     TEXT    NOT NULL
);
CREATE TABLE IF NOT EXISTS locks (
    name       TEXT PRIMARY KEY,
    owner      TEXT    NOT NULL,
    expires_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS readmodels (
    model    TEXT NOT NULL,
    id       TEXT NOT NULL,
    document TEXT NOT NULL,
    version  INTEGER NOT NULL DEFAULT 1,
    deleted  INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (model, id)
);
CREATE TABLE IF NOT EXISTS dead_letters (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    subscription   TEXT    NOT NULL,
    event_id       TEXT    NOT NULL,
    stream_id      TEXT    NOT NULL,
    stream_version INTEGER NOT NULL,
    position_store TEXT    NOT NULL,
    position_token INTEGER NOT NULL,
    stored_at      TEXT    NOT NULL,
    record         TEXT    NOT NULL,
    error          TEXT    NOT NULL,
    attempts       INTEGER NOT NULL,
    UNIQUE (subscription, event_id)
);
CREATE TABLE IF NOT EXISTS outbox (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    destination TEXT    NOT NULL,
    payload     TEXT    NOT NULL,
    stream_id   TEXT,
    status      TEXT    NOT NULL DEFAULT 'pending',
    attempts    INTEGER NOT NULL DEFAULT 0,
    last_error  TEXT
);
CREATE INDEX IF NOT EXISTS outbox_pending ON outbox (status, id);
";

/// Opens `path`, creating the file and schema if needed.
///
/// Uses WAL so readers do not block the writer, and a busy timeout so
/// concurrent writers queue instead of failing at once.
pub(crate) fn open(path: &Path) -> Result<Connection, SqliteError> {
    let conn = Connection::open(path)?;
    conn.busy_timeout(BUSY_TIMEOUT)?;
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "foreign_keys", true)?;
    conn.execute_batch(SCHEMA)?;
    upgrade_readmodels(&conn)?;
    Ok(conn)
}

/// The id that scopes every [`Position`](eventsource_core::Position) of the database in `conn`.
///
/// It is minted once and stored in the file, so every handle on the same file,
/// and every reopen, agrees on it.
pub(crate) fn store_id(conn: &Connection) -> Result<String, SqliteError> {
    conn.execute(
        "INSERT OR IGNORE INTO meta (key, value) VALUES ('store_id', ?1)",
        [uuid::Uuid::new_v4().to_string()],
    )?;
    Ok(
        conn.query_row("SELECT value FROM meta WHERE key = 'store_id'", [], |row| {
            row.get(0)
        })?,
    )
}

/// Adds the version and soft-delete columns to a `readmodels` table made before they existed.
fn upgrade_readmodels(conn: &Connection) -> Result<(), SqliteError> {
    let has_column = |name: &str| -> Result<bool, SqliteError> {
        Ok(conn.query_row(
            "SELECT COUNT(*) FROM pragma_table_info('readmodels') WHERE name = ?1",
            [name],
            |row| row.get::<_, i64>(0),
        )? > 0)
    };
    if !has_column("version")? {
        conn.execute_batch("ALTER TABLE readmodels ADD COLUMN version INTEGER NOT NULL DEFAULT 1")?;
    }
    if !has_column("deleted")? {
        conn.execute_batch("ALTER TABLE readmodels ADD COLUMN deleted INTEGER NOT NULL DEFAULT 0")?;
    }
    Ok(())
}
