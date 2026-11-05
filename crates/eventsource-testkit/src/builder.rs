//! Builds event records for test fixtures without restating plumbing.

use chrono::{DateTime, Utc};
use eventsource_core::{
    CausationId, CorrelationId, DomainEvent, EventId, EventMeta, EventRecord, TenantId,
};
use serde_json::Value;

/// Builds [`EventRecord`]s for tests.
///
/// Everything but the wire name has a default: a null payload, the current
/// time, a fresh correlation id. Each [`build`](Self::build) mints a fresh
/// event id unless one was pinned with [`event_id`](Self::event_id), so one
/// builder can stamp out many distinct events.
#[derive(Debug, Clone)]
pub struct EventBuilder {
    event_type: String,
    schema_version: u32,
    payload: Value,
    event_id: Option<EventId>,
    occurred_at: Option<DateTime<Utc>>,
    correlation_id: Option<CorrelationId>,
    causation_id: Option<CausationId>,
    tenant_id: Option<TenantId>,
    actor_id: Option<String>,
}

impl EventBuilder {
    /// A builder for records with the wire name `event_type` and a null payload.
    #[must_use]
    pub fn new(event_type: impl Into<String>) -> Self {
        Self {
            event_type: event_type.into(),
            schema_version: 1,
            payload: Value::Null,
            event_id: None,
            occurred_at: None,
            correlation_id: None,
            causation_id: None,
            tenant_id: None,
            actor_id: None,
        }
    }

    /// A builder for the record of a domain event: wire name, schema version and payload come from `event`.
    ///
    /// # Panics
    /// If the event cannot be serialized to JSON.
    #[must_use]
    pub fn of<E: DomainEvent>(event: &E) -> Self {
        Self {
            schema_version: E::schema_version(),
            payload: serde_json::to_value(event).expect("event serializes to JSON"),
            ..Self::new(event.event_type())
        }
    }

    /// Sets the JSON payload.
    #[must_use]
    pub fn payload(mut self, payload: Value) -> Self {
        self.payload = payload;
        self
    }

    /// Pins the event id, so every build carries it.
    #[must_use]
    pub const fn event_id(mut self, id: EventId) -> Self {
        self.event_id = Some(id);
        self
    }

    /// Sets when the event occurred.
    #[must_use]
    pub const fn occurred_at(mut self, at: DateTime<Utc>) -> Self {
        self.occurred_at = Some(at);
        self
    }

    /// Sets the correlation id.
    #[must_use]
    pub const fn correlation_id(mut self, id: CorrelationId) -> Self {
        self.correlation_id = Some(id);
        self
    }

    /// Sets the causation id.
    #[must_use]
    pub const fn causation_id(mut self, id: CausationId) -> Self {
        self.causation_id = Some(id);
        self
    }

    /// Sets the tenant.
    #[must_use]
    pub const fn tenant_id(mut self, id: TenantId) -> Self {
        self.tenant_id = Some(id);
        self
    }

    /// Sets the actor.
    #[must_use]
    pub fn actor_id(mut self, actor: impl Into<String>) -> Self {
        self.actor_id = Some(actor.into());
        self
    }

    /// Builds a record with a fresh event id (unless one was pinned).
    #[must_use]
    pub fn build(&self) -> EventRecord {
        let mut meta = EventMeta::new(
            self.occurred_at.unwrap_or_else(Utc::now),
            self.correlation_id.unwrap_or_else(CorrelationId::new),
        );
        if let Some(id) = self.event_id {
            meta.event_id = id;
        }
        meta.causation_id = self.causation_id;
        meta.tenant_id = self.tenant_id;
        meta.actor_id.clone_from(&self.actor_id);
        let mut record = EventRecord::new(meta, self.event_type.clone(), self.payload.clone());
        record.schema_version = self.schema_version;
        record
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn builder_produces_given_type_and_payload_with_fresh_ids() {
        let builder = EventBuilder::new("OrderPlaced").payload(json!({ "total": 5 }));
        let (a, b) = (builder.build(), builder.build());
        assert_eq!(a.event_type, "OrderPlaced");
        assert_eq!(a.payload, json!({ "total": 5 }));
        assert_eq!(b.payload, a.payload);
        assert_ne!(a.meta.event_id, b.meta.event_id);
    }
}
