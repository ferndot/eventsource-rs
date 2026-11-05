//! The message body format: the envelope as JSON, without its source position.

use chrono::{DateTime, Utc};
use eventsource_core::envelope::EventEnvelope;
use eventsource_core::event::EventRecord;
use eventsource_core::position::Position;
use eventsource_core::stream_id::StreamId;
use eventsource_core::version::Version;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct WireEnvelope {
    record: EventRecord,
    stream: StreamId,
    stream_version: Version,
    stored_at: DateTime<Utc>,
}

pub(crate) fn encode(envelope: &EventEnvelope) -> Vec<u8> {
    let wire = WireEnvelope {
        record: envelope.record.clone(),
        stream: envelope.stream.clone(),
        stream_version: envelope.stream_version,
        stored_at: envelope.stored_at,
    };
    serde_json::to_vec(&wire).expect("envelope fields serialize to JSON")
}

pub(crate) fn decode(body: &[u8], position: Position) -> Result<EventEnvelope, serde_json::Error> {
    let wire: WireEnvelope = serde_json::from_slice(body)?;
    Ok(EventEnvelope {
        record: wire.record,
        stream: wire.stream,
        stream_version: wire.stream_version,
        position,
        stored_at: wire.stored_at,
    })
}
