//! Global-feed positions: opaque, ordered tokens scoped to one store.

use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

use crate::error::PositionError;

/// A position in one store's global feed.
///
/// Positions from the same store are totally ordered. Positions from different
/// stores have no meaningful order, so [`Position::try_cmp`] reports that as an
/// error instead of returning a misleading result. Callers should persist and
/// compare positions, never do arithmetic on them.
///
/// `PartialOrd` is deliberately not implemented: a comparison that can fail
/// must not look like an ordinary operator.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Position {
    store_id: String,
    token: u64,
}

impl Position {
    /// Builds a position. Adapters call this; applications normally receive positions from a store.
    #[must_use]
    pub fn new(store_id: impl Into<String>, token: u64) -> Self {
        Self {
            store_id: store_id.into(),
            token,
        }
    }

    /// The store this position belongs to.
    #[must_use]
    pub fn store_id(&self) -> &str {
        &self.store_id
    }

    /// The adapter-defined ordering token.
    #[must_use]
    pub const fn token(&self) -> u64 {
        self.token
    }

    /// Orders two positions from the same store.
    ///
    /// # Errors
    /// Returns [`PositionError::Foreign`] if the positions come from different stores.
    pub fn try_cmp(&self, other: &Self) -> Result<Ordering, PositionError> {
        if self.store_id != other.store_id {
            return Err(PositionError::Foreign {
                left: self.store_id.clone(),
                right: other.store_id.clone(),
            });
        }
        Ok(self.token.cmp(&other.token))
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use super::*;

    #[test]
    fn same_store_positions_order_by_token() {
        let early = Position::new("store", 1);
        let late = Position::new("store", 2);
        assert_eq!(early.try_cmp(&late), Ok(Ordering::Less));
        assert_eq!(late.try_cmp(&early), Ok(Ordering::Greater));
        assert_eq!(early.try_cmp(&early), Ok(Ordering::Equal));
    }

    #[test]
    fn foreign_store_positions_do_not_compare() {
        let ours = Position::new("a", 1);
        let theirs = Position::new("b", 1);
        assert!(matches!(
            ours.try_cmp(&theirs),
            Err(PositionError::Foreign { .. })
        ));
    }
}

#[cfg(test)]
mod more_tests {
    use std::cmp::Ordering;

    use super::*;

    #[test]
    fn ordering_is_by_token_and_transitive() {
        let a = Position::new("s", 1);
        let b = Position::new("s", 10);
        let c = Position::new("s", 100);
        assert_eq!(a.try_cmp(&b), Ok(Ordering::Less));
        assert_eq!(b.try_cmp(&c), Ok(Ordering::Less));
        assert_eq!(a.try_cmp(&c), Ok(Ordering::Less));
    }

    #[test]
    fn foreign_error_names_both_stores() {
        let err = Position::new("a", 1).try_cmp(&Position::new("b", 1));
        assert_eq!(
            err,
            Err(PositionError::Foreign {
                left: "a".to_owned(),
                right: "b".to_owned()
            })
        );
    }

    #[test]
    fn accessors_return_parts() {
        let p = Position::new("store-1", 42);
        assert_eq!(p.store_id(), "store-1");
        assert_eq!(p.token(), 42);
    }
}
