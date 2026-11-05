//! The stream entry format.
//!
//! Each entry has three flat fields so operators can inspect it in `redis-cli`
//! (`event_id`, `event_type`, `payload`) and one `envelope` field that holds
//! everything needed to rebuild the envelope.

use eventsource_core::envelope::EventEnvelope;

/// The flat `(field, value)` pairs for one entry.
pub(crate) fn encode(envelope: &EventEnvelope) -> Vec<(&'static str, String)> {
    vec![
        ("event_id", envelope.record.meta.event_id.to_string()),
        ("event_type", envelope.record.event_type.clone()),
        ("payload", envelope.record.payload.to_string()),
    ]
}
