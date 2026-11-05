//! External-crate check that the public error enums are `#[non_exhaustive]`.
//!
//! Each function lists every current variant and then a wildcard. From outside
//! the defining crate that wildcard is mandatory when the enum is
//! `#[non_exhaustive]`; without the attribute it is unreachable, which
//! `deny(unreachable_patterns)` turns into a compile error. Adding a variant to
//! one of these enums is therefore not a breaking change for downstream matches.
#![allow(missing_docs, dead_code)]
#![deny(unreachable_patterns)]

use eventsource_kafka::{ConsumeError, DecodeRecordError};

fn consume_error(error: &ConsumeError) -> &'static str {
    match error {
        ConsumeError::Kafka { .. } => "Kafka",
        ConsumeError::Decode { .. } => "Decode",
        _ => "future variant",
    }
}

fn decode_record_error(error: &DecodeRecordError) -> &'static str {
    match error {
        DecodeRecordError::MissingValue { .. } => "MissingValue",
        DecodeRecordError::InvalidEnvelope { .. } => "InvalidEnvelope",
        _ => "future variant",
    }
}

#[test]
fn wildcard_arms_compile() {
    // The functions above are the test; this keeps them referenced.
    let _ = (consume_error, decode_record_error,);
}
