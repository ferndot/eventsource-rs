//! Named locks with expiry.
//!
//! A lock is claimed by an owner for a bounded time. Another owner is refused
//! until the lock is released or its lease expires, so a crashed holder cannot
//! block the work forever.

use std::time::Duration;

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
