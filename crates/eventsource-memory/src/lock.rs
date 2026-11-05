//! In-memory lock store.

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};

use chrono::{DateTime, Utc};
use eventsource_core::Clock;
use eventsource_core::lock::{LockError, LockLease, LockName, LockOwner, LockStore, LockTtl};

/// A [`LockStore`] that keeps leases in process memory, timed by an injected [`Clock`].
///
/// It excludes only callers sharing this instance; it is for tests and
/// single-process use.
pub struct InMemoryLockStore<C> {
    clock: C,
    leases: Mutex<HashMap<String, (String, DateTime<Utc>)>>,
}

impl<C: Clock> InMemoryLockStore<C> {
    /// An empty store reading time from `clock`.
    #[must_use]
    pub fn new(clock: C) -> Self {
        Self {
            clock,
            leases: Mutex::new(HashMap::new()),
        }
    }

    fn expiry(&self, ttl: LockTtl) -> DateTime<Utc> {
        let ttl = chrono::Duration::from_std(ttl.get()).unwrap_or(chrono::Duration::MAX);
        self.clock
            .now()
            .checked_add_signed(ttl)
            .unwrap_or(DateTime::<Utc>::MAX_UTC)
    }
}

impl<C: Clock> LockStore for InMemoryLockStore<C> {
    async fn try_acquire(
        &self,
        name: &LockName,
        owner: &LockOwner,
        ttl: LockTtl,
    ) -> Result<Option<LockLease>, LockError> {
        let now = self.clock.now();
        let expires_at = self.expiry(ttl);
        let mut leases = self.leases.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some((holder, until)) = leases.get(name.as_str())
            && *until > now
            && holder != owner.as_str()
        {
            return Ok(None);
        }
        leases.insert(
            name.as_str().to_owned(),
            (owner.as_str().to_owned(), expires_at),
        );
        Ok(Some(LockLease::new(name.clone(), owner.clone(), expires_at)))
    }

    async fn release(&self, name: &LockName, owner: &LockOwner) -> Result<bool, LockError> {
        let now = self.clock.now();
        let mut leases = self.leases.lock().unwrap_or_else(PoisonError::into_inner);
        match leases.get(name.as_str()) {
            Some((holder, until)) if holder == owner.as_str() && *until > now => {
                leases.remove(name.as_str());
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    async fn renew(
        &self,
        _name: &LockName,
        _owner: &LockOwner,
        _ttl: LockTtl,
    ) -> Result<Option<LockLease>, LockError> {
        todo!("renew")
    }
}
