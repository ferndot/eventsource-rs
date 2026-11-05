//! Named locks with expiry.
//!
//! A lock is claimed by an owner for a bounded time. Another owner is refused
//! until the lock is released or its lease expires, so a crashed holder cannot
//! block the work forever.

use std::future::Future;
use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::error::BackendError;

/// Why a lock value was rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum LockValueError {
    /// The lock name was empty or whitespace.
    #[error("lock name must not be empty")]
    EmptyName,
    /// The owner id was empty or whitespace.
    #[error("lock owner must not be empty")]
    EmptyOwner,
    /// The time to live was zero.
    #[error("lock ttl must be positive")]
    ZeroTtl,
}

/// The name of a lock. Never empty.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LockName(String);

impl LockName {
    /// Validates `name`.
    ///
    /// # Errors
    /// [`LockValueError::EmptyName`] if it is empty or only whitespace.
    pub fn new(name: impl Into<String>) -> Result<Self, LockValueError> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(LockValueError::EmptyName);
        }
        Ok(Self(name))
    }

    /// The name as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Identifies a claimant of locks, for example a process or relay id. Never empty.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LockOwner(String);

impl LockOwner {
    /// Validates `owner`.
    ///
    /// # Errors
    /// [`LockValueError::EmptyOwner`] if it is empty or only whitespace.
    pub fn new(owner: impl Into<String>) -> Result<Self, LockValueError> {
        let owner = owner.into();
        if owner.trim().is_empty() {
            return Err(LockValueError::EmptyOwner);
        }
        Ok(Self(owner))
    }

    /// The owner id as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// How long a lease lasts. Always positive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LockTtl(Duration);

impl LockTtl {
    /// Validates `ttl`.
    ///
    /// # Errors
    /// [`LockValueError::ZeroTtl`] if it is zero.
    pub fn new(ttl: Duration) -> Result<Self, LockValueError> {
        if ttl.is_zero() {
            return Err(LockValueError::ZeroTtl);
        }
        Ok(Self(ttl))
    }

    /// The duration.
    #[must_use]
    pub fn get(self) -> Duration {
        self.0
    }
}

/// A granted claim on a lock, valid until `expires_at`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockLease {
    name: LockName,
    owner: LockOwner,
    expires_at: DateTime<Utc>,
}

impl LockLease {
    /// A lease of `name` held by `owner` until `expires_at`.
    #[must_use]
    pub fn new(name: LockName, owner: LockOwner, expires_at: DateTime<Utc>) -> Self {
        Self {
            name,
            owner,
            expires_at,
        }
    }

    /// The locked name.
    #[must_use]
    pub fn name(&self) -> &LockName {
        &self.name
    }

    /// Who holds the lock.
    #[must_use]
    pub fn owner(&self) -> &LockOwner {
        &self.owner
    }

    /// The instant after which the lease no longer excludes others.
    #[must_use]
    pub fn expires_at(&self) -> DateTime<Utc> {
        self.expires_at
    }
}

/// Error from a lock store.
#[derive(Debug, thiserror::Error)]
pub enum LockError {
    /// The backend failed. The cause is available through `source()`.
    #[error("lock backend failed")]
    Backend(#[source] BackendError),
}

/// Exclusive, expiring claims on named resources.
///
/// A lease whose expiry is at or before the store's clock time no longer
/// excludes anyone. Refusals are values (`None` / `false`), not errors.
pub trait LockStore: Send + Sync {
    /// Claims `name` for `owner` for `ttl`.
    ///
    /// `Ok(None)` means another owner holds a live lease. If `owner` already
    /// holds it, the lease is extended, as with [`LockStore::renew`].
    fn try_acquire(
        &self,
        name: &LockName,
        owner: &LockOwner,
        ttl: LockTtl,
    ) -> impl Future<Output = Result<Option<LockLease>, LockError>> + Send;

    /// Releases `name` if `owner` holds it live. Returns whether it was released;
    /// a non-owner changes nothing.
    fn release(
        &self,
        name: &LockName,
        owner: &LockOwner,
    ) -> impl Future<Output = Result<bool, LockError>> + Send;

    /// Extends the live lease of `owner` on `name` to `now + ttl`.
    ///
    /// `Ok(None)` means `owner` does not hold a live lease (never held it, lost
    /// it to expiry, or someone else holds it).
    fn renew(
        &self,
        name: &LockName,
        owner: &LockOwner,
        ttl: LockTtl,
    ) -> impl Future<Output = Result<Option<LockLease>, LockError>> + Send;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn empty_name_is_rejected() {
        assert_eq!(LockName::new(""), Err(LockValueError::EmptyName));
        assert_eq!(LockName::new("   "), Err(LockValueError::EmptyName));
        assert_eq!(LockName::new("relay").expect("valid").as_str(), "relay");
    }

    #[test]
    fn empty_owner_is_rejected() {
        assert_eq!(LockOwner::new(""), Err(LockValueError::EmptyOwner));
        assert_eq!(LockOwner::new("pid-1").expect("valid").as_str(), "pid-1");
    }

    #[test]
    fn zero_ttl_is_rejected() {
        assert_eq!(LockTtl::new(Duration::ZERO), Err(LockValueError::ZeroTtl));
        assert_eq!(
            LockTtl::new(Duration::from_secs(5)).expect("valid").get(),
            Duration::from_secs(5)
        );
    }
}
