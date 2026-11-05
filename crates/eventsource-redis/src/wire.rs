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
