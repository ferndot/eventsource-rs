//! Leader election for a group of members, built on the lock port.
//!
//! Each member builds a [`Coordinator`] over the same [`LockStore`] and lock
//! name. The holder of the lock is the leader. Leadership is a lease: a leader
//! that stops calling [`Coordinator::try_become_leader`] loses it when the TTL
//! passes, and another member can take over.

use std::sync::{Arc, Mutex, PoisonError};

use chrono::{DateTime, Utc};

use crate::clock::Clock;
use crate::lock::{LockError, LockName, LockOwner, LockStore, LockTtl};

/// One member's handle on the leadership of a group.
pub struct Coordinator<S, C> {
    store: Arc<S>,
    clock: C,
    group: LockName,
    owner: LockOwner,
    ttl: LockTtl,
    lease_until: Mutex<Option<DateTime<Utc>>>,
}

impl<S: LockStore, C: Clock> Coordinator<S, C> {
    /// A coordinator for `owner` competing for `group` with leases of `ttl`.
    ///
    /// `clock` should be the clock the store uses, so that [`Self::is_leader`]
    /// and the store agree on when a lease ends.
    #[must_use]
    pub fn new(store: Arc<S>, clock: C, group: LockName, owner: LockOwner, ttl: LockTtl) -> Self {
        Self {
            store,
            clock,
            group,
            owner,
            ttl,
            lease_until: Mutex::new(None),
        }
    }

    /// Acquires the group lock, or extends it if this member already leads.
    ///
    /// Returns whether this member is the leader afterwards. Call it more
    /// often than the TTL to stay leader.
    ///
    /// # Errors
    /// [`LockError`] if the store fails. Leadership state is left unchanged.
    pub async fn try_become_leader(&self) -> Result<bool, LockError> {
        let lease = self
            .store
            .try_acquire(&self.group, &self.owner, self.ttl)
            .await?;
        let leader = lease.is_some();
        self.record(lease.map(|l| l.expires_at()));
        Ok(leader)
    }

    /// Whether this member holds an unexpired lease right now.
    ///
    /// Uses the expiry of the last granted lease and the clock; it does not
    /// call the store. No other member can hold the lock before that expiry.
    #[must_use]
    pub fn is_leader(&self) -> bool {
        let until = *self
            .lease_until
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        until.is_some_and(|until| until > self.clock.now())
    }

    /// Gives up leadership so another member can acquire immediately.
    ///
    /// Returns whether a live lease was released.
    ///
    /// # Errors
    /// [`LockError`] if the store fails.
    pub async fn resign(&self) -> Result<bool, LockError> {
        let released = self.store.release(&self.group, &self.owner).await?;
        self.record(None);
        Ok(released)
    }

    fn record(&self, until: Option<DateTime<Utc>>) {
        *self
            .lease_until
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = until;
    }
}
