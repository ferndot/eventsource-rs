//! The stream entry format.
//!
//! Each entry has three flat fields so operators can inspect it in `redis-cli`
//! (`event_id`, `event_type`, `payload`) and one `envelope` field that holds
//! everything needed to rebuild the envelope. The reader trusts only `envelope`.

use chrono::{DateTime, Utc};
use eventsource_core::envelope::EventEnvelope;
use eventsource_core::event::EventRecord;
use eventsource_core::stream_id::StreamId;
use eventsource_core::version::Version;
use serde::{Deserialize, Serialize};

/// Name of the field that carries the full envelope.
pub(crate) const ENVELOPE_FIELD: &str = "envelope";

/// An envelope without its source position, which means nothing in Redis.
#[derive(Serialize, Deserialize)]
pub(crate) struct WireEnvelope {
    record: EventRecord,
    stream: StreamId,
    stream_version: Version,
    stored_at: DateTime<Utc>,
}

/// The `(field, value)` pairs for one entry.
pub(crate) fn encode(envelope: &EventEnvelope) -> Vec<(&'static str, String)> {
    let wire = WireEnvelope {
        record: envelope.record.clone(),
        stream: envelope.stream.clone(),
        stream_version: envelope.stream_version,
        stored_at: envelope.stored_at,
    };
    vec![
        ("event_id", envelope.record.meta.event_id.to_string()),
        ("event_type", envelope.record.event_type.clone()),
        ("payload", envelope.record.payload.to_string()),
        (
            ENVELOPE_FIELD,
            serde_json::to_string(&wire).expect("envelope fields serialize to JSON"),
        ),
    ]
}

/// Why a stream entry could not be turned into an envelope.
#[derive(Debug, thiserror::Error)]
pub enum DecodeEntryError {
    /// The entry has no `envelope` field.
    #[error("entry {id} has no envelope field")]
    MissingEnvelope {
        /// The Redis entry id.
        id: String,
    },
    /// The `envelope` field is not a valid envelope.
    #[error("entry {id} has an invalid envelope: {source}")]
    InvalidEnvelope {
        /// The Redis entry id.
        id: String,
        /// What was wrong with it.
        source: serde_json::Error,
    },
    /// The entry id is not `<ms>-<seq>`, or its sequence does not fit a position token.
    #[error("entry id {id:?} cannot be used as a position")]
    InvalidEntryId {
        /// The Redis entry id.
        id: String,
    },
}

/// Decodes the `envelope` field text of entry `id`.
pub(crate) fn decode(id: &str, envelope: Option<&str>) -> Result<WireEnvelope, DecodeEntryError> {
    let text = envelope.ok_or_else(|| DecodeEntryError::MissingEnvelope { id: id.to_owned() })?;
    serde_json::from_str(text).map_err(|source| DecodeEntryError::InvalidEnvelope {
        id: id.to_owned(),
        source,
    })
}

impl WireEnvelope {
    pub(crate) fn into_envelope(self, position: eventsource_core::position::Position) -> EventEnvelope {
        EventEnvelope {
            record: self.record,
            stream: self.stream,
            stream_version: self.stream_version,
            position,
            stored_at: self.stored_at,
        }
    }
}

const SEQ_BITS: u32 = 16;

/// Packs a Redis entry id `<ms>-<seq>` into an ordered token: `ms << 16 | seq`.
pub(crate) fn id_to_token(id: &str) -> Result<u64, DecodeEntryError> {
    let bad = || DecodeEntryError::InvalidEntryId { id: id.to_owned() };
    let (ms, seq) = id.split_once('-').ok_or_else(bad)?;
    let ms: u64 = ms.parse().map_err(|_| bad())?;
    let seq: u64 = seq.parse().map_err(|_| bad())?;
    if seq >> SEQ_BITS != 0 || ms >> (u64::BITS - SEQ_BITS) != 0 {
        return Err(bad());
    }
    Ok(ms << SEQ_BITS | seq)
}

/// The inverse of [`id_to_token`].
pub(crate) fn token_to_id(token: u64) -> String {
    format!("{}-{}", token >> SEQ_BITS, token & ((1 << SEQ_BITS) - 1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use eventsource_core::event::EventMeta;
    use eventsource_core::ids::{AggregateId, CorrelationId};
    use eventsource_core::position::Position;
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

    fn envelope_field(fields: &[(&'static str, String)]) -> String {
        fields
            .iter()
            .find(|(name, _)| *name == ENVELOPE_FIELD)
            .map(|(_, value)| value.clone())
            .expect("encode writes the envelope field")
    }

    #[test]
    fn envelope_round_trips_with_the_reader_position() {
        let original = envelope();
        let text = envelope_field(&encode(&original));
        let decoded = decode("1-0", Some(&text)).expect("valid envelope");
        let position = Position::new("redis:events", 42);
        let rebuilt = decoded.into_envelope(position.clone());
        assert_eq!(rebuilt.position, position);
        assert_eq!(rebuilt.record, original.record);
        assert_eq!(rebuilt.stream, original.stream);
        assert_eq!(rebuilt.stream_version, original.stream_version);
        assert_eq!(rebuilt.stored_at, original.stored_at);
    }

    #[test]
    fn flat_fields_mirror_the_record() {
        let original = envelope();
        let fields = encode(&original);
        let get = |name: &str| fields.iter().find(|(n, _)| *n == name).unwrap().1.clone();
        assert_eq!(get("event_id"), original.record.meta.event_id.to_string());
        assert_eq!(get("event_type"), "OrderPlaced");
        assert_eq!(get("payload"), r#"{"n":7}"#);
    }

    #[test]
    fn missing_envelope_field_is_a_typed_error() {
        let error = decode("5-1", None).err().expect("must fail");
        assert!(matches!(error, DecodeEntryError::MissingEnvelope { ref id } if id == "5-1"));
    }

    #[test]
    fn malformed_json_is_a_typed_error_not_a_panic() {
        let error = decode("5-1", Some("{not json")).err().expect("must fail");
        assert!(matches!(error, DecodeEntryError::InvalidEnvelope { ref id, .. } if id == "5-1"));
    }

    #[test]
    fn missing_inner_field_is_a_typed_error() {
        let text = envelope_field(&encode(&envelope()));
        let mut value: serde_json::Value = serde_json::from_str(&text).unwrap();
        value.as_object_mut().unwrap().remove("stream_version");
        let error = decode("5-1", Some(&value.to_string())).err().expect("must fail");
        match error {
            DecodeEntryError::InvalidEnvelope { source, .. } => {
                assert!(source.to_string().contains("stream_version"), "{source}");
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn entry_ids_round_trip_through_tokens() {
        let token = id_to_token("1700000000000-7").unwrap();
        assert_eq!(token_to_id(token), "1700000000000-7");
        assert!(id_to_token("1-0").unwrap() < id_to_token("1-1").unwrap());
        assert!(id_to_token("1-65535").unwrap() < id_to_token("2-0").unwrap());
    }

    #[test]
    fn bad_entry_ids_are_typed_errors() {
        for bad in ["", "17", "a-1", "1-b", "1-65536", "281474976710656-0", "-1-1"] {
            let error = id_to_token(bad).err().unwrap_or_else(|| panic!("{bad:?} accepted"));
            assert!(matches!(error, DecodeEntryError::InvalidEntryId { ref id } if id == bad));
        }
    }
}
