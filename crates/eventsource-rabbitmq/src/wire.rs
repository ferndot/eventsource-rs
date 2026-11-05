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

#[cfg(test)]
mod tests {
    use super::*;
    use eventsource_core::event::EventMeta;
    use eventsource_core::ids::{AggregateId, CorrelationId};
    use eventsource_core::stream_id::Category;

    fn envelope() -> EventEnvelope {
        let meta = EventMeta::new(Utc::now(), CorrelationId::new());
        EventEnvelope {
            record: EventRecord::new(meta, "OrderPlaced", serde_json::json!({ "n": 7 })),
            stream: StreamId::new(AggregateId::new(), Category::new("Order").unwrap()),
            stream_version: Version::new(3),
            position: Position::new("source", 9),
            stored_at: Utc::now(),
        }
    }

    #[test]
    fn envelope_round_trips_with_the_consumer_position() {
        let original = envelope();
        let position = Position::new("rabbitmq:events", 5);
        let rebuilt = decode(&encode(&original), position.clone()).expect("valid");
        assert_eq!(rebuilt.position, position);
        assert_eq!(rebuilt.record, original.record);
        assert_eq!(rebuilt.stream, original.stream);
        assert_eq!(rebuilt.stream_version, original.stream_version);
        assert_eq!(rebuilt.stored_at, original.stored_at);
    }

    #[test]
    fn malformed_body_is_a_decode_error_not_a_panic() {
        for body in [&b"{not json"[..], b"", &[0xff, 0xfe, 0x00]] {
            let error = decode(body, Position::new("q", 1)).err().expect("must fail");
            assert!(error.is_syntax() || error.is_eof(), "{error}");
        }
    }

    #[test]
    fn missing_field_is_a_data_error_naming_the_field() {
        let mut value: serde_json::Value = serde_json::from_slice(&encode(&envelope())).unwrap();
        value.as_object_mut().unwrap().remove("record");
        let body = serde_json::to_vec(&value).unwrap();
        let error = decode(&body, Position::new("q", 1)).err().expect("must fail");
        assert!(error.is_data(), "{error}");
        assert!(error.to_string().contains("record"), "{error}");
    }
}
