//! The record value format: the envelope as JSON, without its source position.

use chrono::{DateTime, Utc};
use eventsource_core::envelope::EventEnvelope;
use eventsource_core::event::EventRecord;
use eventsource_core::position::Position;
use eventsource_core::stream_id::StreamId;
use eventsource_core::version::Version;
use serde::{Deserialize, Serialize};

/// An envelope without its source position, which means nothing in Kafka.
#[derive(Serialize, Deserialize)]
pub(crate) struct WireEnvelope {
    record: EventRecord,
    stream: StreamId,
    stream_version: Version,
    stored_at: DateTime<Utc>,
}

impl WireEnvelope {
    pub(crate) fn from_envelope(envelope: &EventEnvelope) -> Self {
        Self {
            record: envelope.record.clone(),
            stream: envelope.stream.clone(),
            stream_version: envelope.stream_version,
            stored_at: envelope.stored_at,
        }
    }

    pub(crate) fn into_envelope(self, position: Position) -> EventEnvelope {
        EventEnvelope {
            record: self.record,
            stream: self.stream,
            stream_version: self.stream_version,
            position,
            stored_at: self.stored_at,
        }
    }
}

/// The record value bytes for `envelope`.
pub(crate) fn encode(envelope: &EventEnvelope) -> Vec<u8> {
    serde_json::to_vec(&WireEnvelope::from_envelope(envelope))
        .expect("envelope fields serialize to JSON")
}

/// Why a record could not be turned into an envelope.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DecodeRecordError {
    /// The record has no value (a tombstone).
    #[error("record at partition {partition} offset {offset} has no value")]
    MissingValue {
        /// The partition it came from.
        partition: i32,
        /// Its offset in that partition.
        offset: i64,
    },
    /// The record value is not a valid envelope.
    #[error("record at partition {partition} offset {offset} has an invalid envelope: {source}")]
    InvalidEnvelope {
        /// The partition it came from.
        partition: i32,
        /// Its offset in that partition.
        offset: i64,
        /// What was wrong with it.
        source: serde_json::Error,
    },
}

pub(crate) fn decode(
    partition: i32,
    offset: i64,
    value: Option<&[u8]>,
) -> Result<WireEnvelope, DecodeRecordError> {
    let bytes = value.ok_or(DecodeRecordError::MissingValue { partition, offset })?;
    serde_json::from_slice(bytes).map_err(|source| DecodeRecordError::InvalidEnvelope {
        partition,
        offset,
        source,
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
        let bytes = encode(&original);
        let position = Position::new("kafka:events", 5);
        let rebuilt = decode(0, 0, Some(&bytes)).expect("valid").into_envelope(position.clone());
        assert_eq!(rebuilt.position, position);
        assert_eq!(rebuilt.record, original.record);
        assert_eq!(rebuilt.stream, original.stream);
        assert_eq!(rebuilt.stream_version, original.stream_version);
        assert_eq!(rebuilt.stored_at, original.stored_at);
    }

    #[test]
    fn tombstone_is_missing_value() {
        let error = decode(2, 41, None).err().expect("must fail");
        assert!(matches!(error, DecodeRecordError::MissingValue { partition: 2, offset: 41 }));
    }

    #[test]
    fn malformed_bytes_are_invalid_envelope_not_a_panic() {
        for bytes in [&b"{not json"[..], b"", &[0xff, 0xfe, 0x00]] {
            let error = decode(1, 7, Some(bytes)).err().expect("must fail");
            assert!(matches!(
                error,
                DecodeRecordError::InvalidEnvelope { partition: 1, offset: 7, .. }
            ));
        }
    }

    #[test]
    fn missing_field_is_invalid_envelope_naming_the_field() {
        let mut value: serde_json::Value = serde_json::from_slice(&encode(&envelope())).unwrap();
        value.as_object_mut().unwrap().remove("stored_at");
        let bytes = serde_json::to_vec(&value).unwrap();
        match decode(0, 3, Some(&bytes)).err().expect("must fail") {
            DecodeRecordError::InvalidEnvelope { source, offset: 3, .. } => {
                assert!(source.to_string().contains("stored_at"), "{source}");
            }
            other => panic!("unexpected {other:?}"),
        }
    }
}
