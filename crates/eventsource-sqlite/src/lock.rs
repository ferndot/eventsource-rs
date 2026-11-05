//! `SQLite`-backed lock store.

use std::path::Path;
use std::sync::{Mutex, MutexGuard, PoisonError};

use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

use eventsource_core::Clock;
use eventsource_core::lock::{LockError, LockLease, LockName, LockOwner, LockStore, LockTtl};

use crate::db;
use crate::error::SqliteError;

/// A durable [`LockStore`] on a `SQLite` file.
///
/// Every decision runs in one `BEGIN IMMEDIATE` transaction, so handles on the
/// same file, in this or another process, never both win a free lock. Expiry
/// is judged against the injected [`Clock`]; processes sharing a file must
/// have reasonably synchronised clocks. Leases are stored with microsecond
/// precision.
pub struct SqliteLockStore<C> {
    clock: C,
    conn: Mutex<Connection>,
}

impl<C: Clock> SqliteLockStore<C> {
    /// Opens the database at `path`, creating the file and schema if needed.
    ///
    /// # Errors
    /// [`LockError::Backend`] if the file cannot be opened or initialised.
    pub fn open(path: impl AsRef<Path>, clock: C) -> Result<Self, LockError> {
        let conn = db::open(path.as_ref())?;
        Ok(Self {
            clock,
            conn: Mutex::new(conn),
        })
    }

    fn lock(&self) -> MutexGuard<'_, Connection> {
        self.conn.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn expiry(&self, now: DateTime<Utc>, ttl: LockTtl) -> DateTime<Utc> {
        let ttl = chrono::Duration::from_std(ttl.get()).unwrap_or(chrono::Duration::MAX);
        now.checked_add_signed(ttl).unwrap_or(DateTime::<Utc>::MAX_UTC)
    }

    fn acquire_blocking(
        &self,
        name: &LockName,
        owner: &LockOwner,
        ttl: LockTtl,
    ) -> Result<Option<LockLease>, SqliteError> {
        let now = self.clock.now();
        let expires_at = self.expiry(now, ttl);
        let mut conn = self.lock();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let held: Option<(String, i64)> = tx
            .query_row(
                "SELECT owner, expires_at FROM locks WHERE name = ?1",
                [name.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if let Some((holder, until)) = held
            && until > now.timestamp_micros()
            && holder != owner.as_str()
        {
            return Ok(None);
        }
        tx.execute(
            "INSERT INTO locks (name, owner, expires_at) VALUES (?1, ?2, ?3) \
             ON CONFLICT (name) DO UPDATE SET owner = excluded.owner, \
             expires_at = excluded.expires_at",
            params![name.as_str(), owner.as_str(), expires_at.timestamp_micros()],
        )?;
        tx.commit()?;
        Ok(Some(LockLease::new(name.clone(), owner.clone(), expires_at)))
    }
}

impl<C: Clock> LockStore for SqliteLockStore<C> {
    async fn try_acquire(
        &self,
        name: &LockName,
        owner: &LockOwner,
        ttl: LockTtl,
    ) -> Result<Option<LockLease>, LockError> {
        Ok(self.acquire_blocking(name, owner, ttl)?)
    }

    async fn release(&self, name: &LockName, owner: &LockOwner) -> Result<bool, LockError> {
        let now = self.clock.now().timestamp_micros();
        let conn = self.lock();
        let removed = conn
            .execute(
                "DELETE FROM locks WHERE name = ?1 AND owner = ?2 AND expires_at > ?3",
                params![name.as_str(), owner.as_str(), now],
            )
            .map_err(SqliteError::from)?;
        Ok(removed > 0)
    }

    async fn renew(
        &self,
        name: &LockName,
        owner: &LockOwner,
        ttl: LockTtl,
    ) -> Result<Option<LockLease>, LockError> {
        let now = self.clock.now();
        let expires_at = self.expiry(now, ttl);
        let conn = self.lock();
        let updated = conn
            .execute(
                "UPDATE locks SET expires_at = ?3 \
                 WHERE name = ?1 AND owner = ?2 AND expires_at > ?4",
                params![
                    name.as_str(),
                    owner.as_str(),
                    expires_at.timestamp_micros(),
                    now.timestamp_micros()
                ],
            )
            .map_err(SqliteError::from)?;
        Ok((updated > 0).then(|| LockLease::new(name.clone(), owner.clone(), expires_at)))
    }
}
