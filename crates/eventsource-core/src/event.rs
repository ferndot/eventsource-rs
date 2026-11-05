//! Domain events and their stored form.

use std::fmt::Debug;

use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::DecodeError;
use crate::ids::{CausationId, CorrelationId, EventId, TenantId};

/// A domain event: an immutable fact that has happened.
///
/// Implement this on an enum with one variant per event kind. The enum is the
/// closed set of events an aggregate can produce, so `evolve` can match on it
/// exhaustively.
pub trait DomainEvent:
    Serialize + DeserializeOwned + Clone + Debug + Send + Sync + 'static
{
    /// Every wire name this enum can produce. Used by [`crate::EventRegistry`].
    fn event_types() -> &'static [&'static str];

    /// The wire name of this value's variant.
    fn event_type(&self) -> &'static str;

    /// Version of this enum's payload format. Bump it when the shape changes.
    ///
    /// Records written with a different version are rejected on decode until an
    /// upcaster exists to migrate them.
    #[must_use]
    fn schema_version() -> u32 {
        1
    }
}

/// Provenance attached to every stored event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventMeta {
    /// Unique id of this event.
    pub event_id: EventId,
    /// When the event occurred, in UTC.
    pub occurred_at: DateTime<Utc>,
    /// Links this event to related events across aggregates.
    pub correlation_id: CorrelationId,
    /// The event that caused this one, if any.
    pub causation_id: Option<CausationId>,
    /// Tenant the event belongs to, if the application is multi-tenant.
    pub tenant_id: Option<TenantId>,
    /// User or system that triggered the event.
    pub actor_id: Option<String>,
}

impl EventMeta {
    /// Metadata with a fresh event id, for an event that occurred at `occurred_at`.
    #[must_use]
    pub fn new(occurred_at: DateTime<Utc>, correlation_id: CorrelationId) -> Self {
        Self {
            event_id: EventId::new(),
            occurred_at,
            correlation_id,
            causation_id: None,
            tenant_id: None,
            actor_id: None,
        }
    }
}

/// An event in its serialized form, as it is written to and read from a store.
///
/// The payload is a JSON value. Adapters can persist it as text, JSONB, or
/// bytes without knowing the event type.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventRecord {
    /// Provenance of the event.
    pub meta: EventMeta,
    /// Wire name of the variant, e.g. `"OrderPlaced"`.
    pub event_type: String,
    /// Schema version of the payload, for future migrations.
    pub schema_version: u32,
    /// The serialized event.
    pub payload: Value,
}

impl EventRecord {
    /// A record with an explicit wire name and payload, for adapters and tests.
    #[must_use]
    pub fn new(meta: EventMeta, event_type: impl Into<String>, payload: Value) -> Self {
        Self {
            meta,
            event_type: event_type.into(),
            schema_version: 1,
            payload,
        }
    }

    /// Serializes a domain event with the given provenance.
    ///
    /// # Errors
    /// Returns an error if the event cannot be serialized to JSON.
    pub fn from_event<E: DomainEvent>(
        event: &E,
        meta: EventMeta,
    ) -> Result<Self, serde_json::Error> {
        Ok(Self {
            meta,
            event_type: event.event_type().to_owned(),
            schema_version: E::schema_version(),
            payload: serde_json::to_value(event)?,
        })
    }

    /// Deserializes the record into a domain event, checking it is consistent.
    ///
    /// # Errors
    /// - [`DecodeError::UnsupportedSchemaVersion`] if the record was written with another schema version.
    /// - [`DecodeError::Payload`] if the payload does not match `E`.
    /// - [`DecodeError::EventTypeMismatch`] if the stored event type differs from the variant the payload decodes to.
    pub fn decode<E: DomainEvent>(&self) -> Result<E, DecodeError> {
        let supported = E::schema_version();
        if self.schema_version != supported {
            return Err(DecodeError::UnsupportedSchemaVersion {
                found: self.schema_version,
                supported,
            });
        }
        let event = E::deserialize(&self.payload)?;
        if event.event_type() != self.event_type {
            return Err(DecodeError::EventTypeMismatch {
                stored: self.event_type.clone(),
                decoded: event.event_type().to_owned(),
            });
        }
        Ok(event)
    }
}
