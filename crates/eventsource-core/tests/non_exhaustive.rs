//! External-crate check that the public error enums are `#[non_exhaustive]`.
//!
//! Each function lists every current variant and then a wildcard. From outside
//! the defining crate that wildcard is mandatory when the enum is
//! `#[non_exhaustive]`; without the attribute it is unreachable, which
//! `deny(unreachable_patterns)` turns into a compile error. Adding a variant to
//! one of these enums is therefore not a breaking change for downstream matches.
#![allow(missing_docs, dead_code)]
#![deny(unreachable_patterns)]

use eventsource_core::dead_letter::RetryParkedError;
use eventsource_core::error::{StreamIdError, PositionError, RegistryError, StoreError, BusError, CheckpointError, DecodeError, DeadLetterError, OutboxError, AtomicAppendError, ReadModelError, SnapshotError, SnapshotPolicyError, RepositoryError, SubscriptionError};
use eventsource_core::lock::{LockError, LockValueError};

fn stream_id_error(error: &StreamIdError) -> &'static str {
    match error {
        StreamIdError::InvalidCategory { .. } => "InvalidCategory",
        StreamIdError::Malformed { .. } => "Malformed",
        StreamIdError::Uuid { .. } => "Uuid",
        _ => "future variant",
    }
}

fn position_error(error: &PositionError) -> &'static str {
    match error {
        PositionError::Foreign { .. } => "Foreign",
        _ => "future variant",
    }
}

fn registry_error(error: &RegistryError) -> &'static str {
    match error {
        RegistryError::DuplicateEventType { .. } => "DuplicateEventType",
        _ => "future variant",
    }
}

fn store_error(error: &StoreError) -> &'static str {
    match error {
        StoreError::ConcurrencyConflict { .. } => "ConcurrencyConflict",
        StoreError::DuplicateEvent { .. } => "DuplicateEvent",
        StoreError::EmptyAppend { .. } => "EmptyAppend",
        StoreError::UnregisteredEventType { .. } => "UnregisteredEventType",
        StoreError::Position { .. } => "Position",
        StoreError::Backend { .. } => "Backend",
        _ => "future variant",
    }
}

fn bus_error(error: &BusError) -> &'static str {
    match error {
        BusError::HandlerFailures { .. } => "HandlerFailures",
        BusError::Backend { .. } => "Backend",
        _ => "future variant",
    }
}

fn checkpoint_error(error: &CheckpointError) -> &'static str {
    match error {
        CheckpointError::Backend { .. } => "Backend",
        CheckpointError::Regressed { .. } => "Regressed",
        _ => "future variant",
    }
}

fn decode_error(error: &DecodeError) -> &'static str {
    match error {
        DecodeError::Payload { .. } => "Payload",
        DecodeError::EventTypeMismatch { .. } => "EventTypeMismatch",
        DecodeError::UnsupportedSchemaVersion { .. } => "UnsupportedSchemaVersion",
        _ => "future variant",
    }
}

fn dead_letter_error(error: &DeadLetterError) -> &'static str {
    match error {
        DeadLetterError::NotConfigured { .. } => "NotConfigured",
        DeadLetterError::Backend { .. } => "Backend",
        _ => "future variant",
    }
}

fn outbox_error(error: &OutboxError) -> &'static str {
    match error {
        OutboxError::Invalid { .. } => "Invalid",
        OutboxError::NotConfigured { .. } => "NotConfigured",
        OutboxError::Backend { .. } => "Backend",
        _ => "future variant",
    }
}

fn atomic_append_error(error: &AtomicAppendError) -> &'static str {
    match error {
        AtomicAppendError::Store { .. } => "Store",
        AtomicAppendError::Outbox { .. } => "Outbox",
        _ => "future variant",
    }
}

fn read_model_error(error: &ReadModelError) -> &'static str {
    match error {
        ReadModelError::Backend { .. } => "Backend",
        _ => "future variant",
    }
}

fn snapshot_error(error: &SnapshotError) -> &'static str {
    match error {
        SnapshotError::Corrupt { .. } => "Corrupt",
        SnapshotError::Backend { .. } => "Backend",
        _ => "future variant",
    }
}

fn snapshot_policy_error(error: &SnapshotPolicyError) -> &'static str {
    match error {
        SnapshotPolicyError::ZeroInterval { .. } => "ZeroInterval",
        _ => "future variant",
    }
}

fn repository_error<E: std::error::Error + 'static>(error: &RepositoryError<E>) -> &'static str {
    match error {
        RepositoryError::Domain { .. } => "Domain",
        RepositoryError::Store { .. } => "Store",
        RepositoryError::Decode { .. } => "Decode",
        RepositoryError::Encode { .. } => "Encode",
        RepositoryError::CorruptStream { .. } => "CorruptStream",
        RepositoryError::Outbox { .. } => "Outbox",
        RepositoryError::NotFound { .. } => "NotFound",
        RepositoryError::TenantMismatch { .. } => "TenantMismatch",
        _ => "future variant",
    }
}

fn subscription_error(error: &SubscriptionError) -> &'static str {
    match error {
        SubscriptionError::Handler { .. } => "Handler",
        SubscriptionError::DeadLetter { .. } => "DeadLetter",
        SubscriptionError::Store { .. } => "Store",
        SubscriptionError::Checkpoint { .. } => "Checkpoint",
        _ => "future variant",
    }
}

fn lock_value_error(error: &LockValueError) -> &'static str {
    match error {
        LockValueError::EmptyName { .. } => "EmptyName",
        LockValueError::EmptyOwner { .. } => "EmptyOwner",
        LockValueError::ZeroTtl { .. } => "ZeroTtl",
        _ => "future variant",
    }
}

fn lock_error(error: &LockError) -> &'static str {
    match error {
        LockError::Backend { .. } => "Backend",
        _ => "future variant",
    }
}

fn retry_parked_error(error: &RetryParkedError) -> &'static str {
    match error {
        RetryParkedError::Handler { .. } => "Handler",
        RetryParkedError::Store { .. } => "Store",
        _ => "future variant",
    }
}

#[test]
fn wildcard_arms_compile() {
    // The functions above are the test; this keeps them referenced.
    let _ = (stream_id_error, position_error, registry_error, store_error, bus_error, checkpoint_error, decode_error, dead_letter_error, outbox_error, atomic_append_error, read_model_error, snapshot_error, snapshot_policy_error, repository_error::<std::io::Error>, subscription_error, lock_value_error, lock_error, retry_parked_error,);
}
