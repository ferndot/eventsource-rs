//! Strongly typed identifiers. Each id is a distinct type so they cannot be swapped.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

macro_rules! define_id {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(Uuid);

        impl $name {
            /// Generates a new time-ordered (`UUIDv7`) identifier.
            #[must_use]
            #[allow(clippy::new_without_default)] // Deliberately no Default: it would mint a random id silently.
            pub fn new() -> Self {
                Self(Uuid::now_v7())
            }

            /// Wraps an existing UUID.
            #[must_use]
            pub const fn from_uuid(uuid: Uuid) -> Self {
                Self(uuid)
            }

            /// Returns the underlying UUID.
            #[must_use]
            pub const fn as_uuid(&self) -> &Uuid {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }

        impl FromStr for $name {
            type Err = uuid::Error;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Uuid::parse_str(s).map(Self)
            }
        }
    };
}

define_id!(
    /// Identifies an aggregate instance.
    AggregateId
);
define_id!(
    /// Identifies a single event.
    EventId
);
define_id!(
    /// Identifies the tenant an event belongs to.
    TenantId
);
define_id!(
    /// Links related events across aggregates.
    CorrelationId
);
define_id!(
    /// Identifies the event that caused another event.
    CausationId
);
