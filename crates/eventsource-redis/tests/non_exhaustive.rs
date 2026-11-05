//! External-crate check that the public error enums are `#[non_exhaustive]`.
//!
//! Each function lists every current variant and then a wildcard. From outside
//! the defining crate that wildcard is mandatory when the enum is
//! `#[non_exhaustive]`; without the attribute it is unreachable, which
//! `deny(unreachable_patterns)` turns into a compile error. Adding a variant to
//! one of these enums is therefore not a breaking change for downstream matches.
#![allow(missing_docs, dead_code)]
#![deny(unreachable_patterns)]

use eventsource_redis::DecodeEntryError;

fn decode_entry_error(error: &DecodeEntryError) -> &'static str {
    match error {
        DecodeEntryError::MissingEnvelope { .. } => "MissingEnvelope",
        DecodeEntryError::InvalidEnvelope { .. } => "InvalidEnvelope",
        DecodeEntryError::InvalidEntryId { .. } => "InvalidEntryId",
        _ => "future variant",
    }
}

#[test]
fn wildcard_arms_compile() {
    // The functions above are the test; this keeps them referenced.
    let _ = (decode_entry_error,);
}
