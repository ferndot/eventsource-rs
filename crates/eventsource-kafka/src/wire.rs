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
