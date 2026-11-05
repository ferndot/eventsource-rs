//! `PostgreSQL`-backed lock store.

use chrono::{DateTime, Utc};
use futures::lock::Mutex;
use tokio_postgres::Client;

use eventsource_core::Clock;
use eventsource_core::lock::{LockError, LockLease, LockName, LockOwner, LockStore, LockTtl};

use crate::db;
use crate::error::PostgresError;

/// A durable [`LockStore`] on a `PostgreSQL` schema.
///
/// Expiry is judged against the injected [`Clock`], not the database clock;
/// processes sharing a schema must have reasonably synchronised clocks. Leases
/// are stored with microsecond precision.
pub struct PostgresLockStore<C> {
    clock: C,
    client: Mutex<Client>,
}

impl<C: Clock> PostgresLockStore<C> {
    /// Connects to `url`, creating `schema` and its tables if needed.
    ///
    /// # Errors
    /// [`LockError::Backend`] if the connection or initialisation fails.
    pub async fn open(url: &str, schema: &str, clock: C) -> Result<Self, LockError> {
        let client = db::connect(url, schema).await?;
        Ok(Self {
            clock,
            client: Mutex::new(client),
        })
    }

    async fn acquire_inner(
        &self,
        name: &LockName,
        owner: &LockOwner,
        ttl: LockTtl,
    ) -> Result<Option<LockLease>, PostgresError> {
        let now = self.clock.now();
        let expires_at = expiry(now, ttl);
        // One statement decides: the row is taken only if it is free, expired or
        // ours. A concurrent racer waits on the row lock and then re-checks the
        // WHERE against the winner's committed row, so exactly one wins.
        let taken = self
            .client
            .lock()
            .await
            .execute(
                "INSERT INTO locks (name, owner, expires_at) VALUES ($1, $2, $3) \
                 ON CONFLICT (name) DO UPDATE SET owner = excluded.owner, \
                 expires_at = excluded.expires_at \
                 WHERE locks.owner = excluded.owner OR locks.expires_at <= $4",
                &[
                    &name.as_str(),
                    &owner.as_str(),
                    &expires_at.timestamp_micros(),
                    &now.timestamp_micros(),
                ],
            )
            .await?;
        if taken == 0 {
            return Ok(None);
        }
        Ok(Some(LockLease::new(
            name.clone(),
            owner.clone(),
            expires_at,
        )))
    }
}

impl<C: Clock> LockStore for PostgresLockStore<C> {
    async fn try_acquire(
        &self,
        name: &LockName,
        owner: &LockOwner,
        ttl: LockTtl,
    ) -> Result<Option<LockLease>, LockError> {
        Ok(self.acquire_inner(name, owner, ttl).await?)
    }

    async fn release(&self, name: &LockName, owner: &LockOwner) -> Result<bool, LockError> {
        let now = self.clock.now().timestamp_micros();
        let removed = self
            .client
            .lock()
            .await
            .execute(
                "DELETE FROM locks WHERE name = $1 AND owner = $2 AND expires_at > $3",
                &[&name.as_str(), &owner.as_str(), &now],
            )
            .await
            .map_err(PostgresError::from)?;
        Ok(removed > 0)
    }

    async fn renew(
        &self,
        name: &LockName,
        owner: &LockOwner,
        ttl: LockTtl,
    ) -> Result<Option<LockLease>, LockError> {
        let now = self.clock.now();
        let expires_at = expiry(now, ttl);
        let updated = self
            .client
            .lock()
            .await
            .execute(
                "UPDATE locks SET expires_at = $3 \
                 WHERE name = $1 AND owner = $2 AND expires_at > $4",
                &[
                    &name.as_str(),
                    &owner.as_str(),
                    &expires_at.timestamp_micros(),
                    &now.timestamp_micros(),
                ],
            )
            .await
            .map_err(PostgresError::from)?;
        Ok((updated > 0).then(|| LockLease::new(name.clone(), owner.clone(), expires_at)))
    }
}

fn expiry(now: DateTime<Utc>, ttl: LockTtl) -> DateTime<Utc> {
    let ttl = chrono::Duration::from_std(ttl.get()).unwrap_or(chrono::Duration::MAX);
    now.checked_add_signed(ttl)
        .unwrap_or(DateTime::<Utc>::MAX_UTC)
}
