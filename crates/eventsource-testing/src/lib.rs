#![allow(clippy::missing_panics_doc)] // Test helpers panic on failure by design.
//! Conformance suites for store adapters.
//!
//! A backend passes conformance by calling [`run_all`] with a factory that
//! returns a fresh, empty store. Each check runs against its own store, so
//! checks cannot affect one another.

use eventsource_core::envelope::{FeedReadOptions, ReadDirection, StreamReadOptions};
use eventsource_core::error::StoreError;
use eventsource_core::event::{EventMeta, EventRecord};
use eventsource_core::store::{AggregateStore, GlobalFeed};
use eventsource_core::{
    AggregateId, Category, CorrelationId, ExpectedVersion, Position, StreamId, TenantId, Version,
};

/// Builds an event record of `event_type` with a fresh id and a null payload.
#[must_use]
pub fn record(event_type: &str) -> EventRecord {
    EventRecord::new(
        EventMeta::new(chrono::Utc::now(), CorrelationId::new()),
        event_type,
        serde_json::Value::Null,
    )
}

fn stream(category: &str) -> StreamId {
    StreamId::new(
        AggregateId::new(),
        Category::new(category).expect("valid category"),
    )
}

/// Runs every conformance check, each against a fresh store from `make`.
pub async fn run_all<S, F>(make: F)
where
    S: AggregateStore + GlobalFeed,
    F: Fn() -> S,
{
    check_new_stream_starts_at_one(&make()).await;
    check_expected_exact(&make()).await;
    check_no_stream_and_stream_exists(&make()).await;
    check_duplicate_event_is_atomic(&make()).await;
    check_empty_append_rejected(&make()).await;
    check_stream_read_order_and_bounds(&make()).await;
    check_feed_resume_from_position(&make()).await;
    check_feed_rejects_foreign_position(&make()).await;
    check_feed_filters_and_limit(&make()).await;
    check_feed_paging_with_limit(&make()).await;
    check_concurrent_exact_zero_has_one_winner(&make()).await;
}

/// Feed reads filter by tenant and category, and `limit` caps the result.
pub async fn check_feed_filters_and_limit<S: AggregateStore + GlobalFeed>(store: &S) {
    let (mine, theirs) = (TenantId::new(), TenantId::new());
    let tagged = |event_type: &str, tenant: TenantId| {
        let mut record = record(event_type);
        record.meta.tenant_id = Some(tenant);
        record
    };
    store
        .append(
            &stream("Order"),
            vec![tagged("A", mine)],
            ExpectedVersion::Any,
        )
        .await
        .expect("A");
    store
        .append(
            &stream("Order"),
            vec![tagged("B", theirs)],
            ExpectedVersion::Any,
        )
        .await
        .expect("B");
    store
        .append(
            &stream("Invoice"),
            vec![tagged("C", mine)],
            ExpectedVersion::Any,
        )
        .await
        .expect("C");

    let types = |envelopes: Vec<eventsource_core::EventEnvelope>| -> Vec<String> {
        envelopes.into_iter().map(|e| e.record.event_type).collect()
    };
    let by_tenant = FeedReadOptions {
        tenant_id: Some(mine),
        ..Default::default()
    };
    let got = store.read_all(None, by_tenant).await.expect("tenant");
    assert_eq!(types(got), ["A", "C"]);

    let by_category = FeedReadOptions {
        category: Some(Category::new("Order").expect("valid")),
        ..Default::default()
    };
    let got = store.read_all(None, by_category).await.expect("category");
    assert_eq!(types(got), ["A", "B"]);

    let limited = FeedReadOptions {
        tenant_id: Some(mine),
        limit: std::num::NonZeroUsize::new(1),
        ..Default::default()
    };
    let got = store.read_all(None, limited).await.expect("limit");
    assert_eq!(types(got), ["A"]);
}

/// Appending to a new stream yields versions 1..=n with per-stream gapless numbering.
pub async fn check_new_stream_starts_at_one<S: AggregateStore>(store: &S) {
    let s = stream("Order");
    let result = store
        .append(
            &s,
            vec![record("A"), record("B")],
            ExpectedVersion::NoStream,
        )
        .await
        .expect("append to empty stream");
    assert_eq!(result.new_version, Version::new(2));
    let versions: Vec<u64> = result
        .envelopes
        .iter()
        .map(|e| e.stream_version.get())
        .collect();
    assert_eq!(versions, [1, 2]);
    assert_eq!(
        store.stream_version(&s).await.expect("version"),
        Version::new(2)
    );
}

/// `Exact(n)` succeeds when the stream has exactly `n` events and conflicts otherwise.
pub async fn check_expected_exact<S: AggregateStore>(store: &S) {
    let s = stream("Order");
    store
        .append(&s, vec![record("A")], ExpectedVersion::Exact(Version::ZERO))
        .await
        .expect("exact zero on new stream");
    let stale = store
        .append(&s, vec![record("B")], ExpectedVersion::Exact(Version::ZERO))
        .await;
    assert!(
        matches!(stale, Err(StoreError::ConcurrencyConflict { actual, .. }) if actual == Version::new(1)),
        "stale exact append must conflict with actual version 1, got {stale:?}"
    );
    store
        .append(
            &s,
            vec![record("B")],
            ExpectedVersion::Exact(Version::new(1)),
        )
        .await
        .expect("exact one after one event");
}

/// `NoStream` rejects an existing stream; `StreamExists` rejects a missing one.
pub async fn check_no_stream_and_stream_exists<S: AggregateStore>(store: &S) {
    let s = stream("Order");
    let missing = store
        .append(&s, vec![record("A")], ExpectedVersion::StreamExists)
        .await;
    assert!(matches!(
        missing,
        Err(StoreError::ConcurrencyConflict { .. })
    ));
    store
        .append(&s, vec![record("A")], ExpectedVersion::NoStream)
        .await
        .expect("no-stream on empty stream");
    let present = store
        .append(&s, vec![record("B")], ExpectedVersion::NoStream)
        .await;
    assert!(matches!(
        present,
        Err(StoreError::ConcurrencyConflict { .. })
    ));
    store
        .append(&s, vec![record("B")], ExpectedVersion::StreamExists)
        .await
        .expect("stream-exists on populated stream");
}

/// A duplicate event id, within a batch or across batches, rejects the whole append.
pub async fn check_duplicate_event_is_atomic<S: AggregateStore>(store: &S) {
    let s = stream("Order");
    let first = record("A");
    let dup_in_batch = store
        .append(&s, vec![first.clone(), first.clone()], ExpectedVersion::Any)
        .await;
    assert!(matches!(dup_in_batch, Err(StoreError::DuplicateEvent(_))));
    assert_eq!(
        store.stream_version(&s).await.expect("version"),
        Version::ZERO
    );

    store
        .append(&s, vec![first.clone()], ExpectedVersion::Any)
        .await
        .expect("first store");
    let dup_across = store
        .append(&s, vec![record("B"), first], ExpectedVersion::Any)
        .await;
    assert!(matches!(dup_across, Err(StoreError::DuplicateEvent(_))));
    assert_eq!(
        store.stream_version(&s).await.expect("version"),
        Version::new(1)
    );
}

/// An empty batch is rejected.
pub async fn check_empty_append_rejected<S: AggregateStore>(store: &S) {
    let result = store
        .append(&stream("Order"), Vec::new(), ExpectedVersion::Any)
        .await;
    assert!(matches!(result, Err(StoreError::EmptyAppend)));
}

/// Stream reads honour direction, inclusive `from`/`to`, and `limit`.
pub async fn check_stream_read_order_and_bounds<S: AggregateStore>(store: &S) {
    let s = stream("Order");
    store
        .append(
            &s,
            vec![record("A"), record("B"), record("C"), record("D")],
            ExpectedVersion::Any,
        )
        .await
        .expect("append four");

    let versions = |envelopes: Vec<eventsource_core::EventEnvelope>| -> Vec<u64> {
        envelopes.iter().map(|e| e.stream_version.get()).collect()
    };

    let all = store
        .read_stream(&s, StreamReadOptions::default())
        .await
        .expect("read");
    assert_eq!(versions(all), [1, 2, 3, 4]);

    let bounded = StreamReadOptions {
        from: Some(Version::new(2)),
        to: Some(Version::new(3)),
        ..Default::default()
    };
    let got = store.read_stream(&s, bounded).await.expect("read bounded");
    assert_eq!(versions(got), [2, 3], "from and to are inclusive");

    let backward = StreamReadOptions {
        direction: ReadDirection::Backward,
        limit: std::num::NonZeroUsize::new(2),
        ..Default::default()
    };
    let got = store
        .read_stream(&s, backward)
        .await
        .expect("read backward");
    assert_eq!(
        versions(got),
        [4, 3],
        "backward with limit returns the newest events"
    );

    let forward = StreamReadOptions {
        limit: std::num::NonZeroUsize::new(2),
        ..Default::default()
    };
    let got = store.read_stream(&s, forward).await.expect("read forward");
    assert_eq!(
        versions(got),
        [1, 2],
        "forward with limit returns the oldest events"
    );
}

/// The global feed is in append order, and resuming from a position skips what was seen.
pub async fn check_feed_resume_from_position<S: AggregateStore + GlobalFeed>(store: &S) {
    let s1 = stream("Order");
    let s2 = stream("Invoice");
    let first = store
        .append(&s1, vec![record("A")], ExpectedVersion::Any)
        .await
        .expect("first");
    store
        .append(&s2, vec![record("B")], ExpectedVersion::Any)
        .await
        .expect("second");
    store
        .append(&s1, vec![record("C")], ExpectedVersion::Any)
        .await
        .expect("third");

    let all = store
        .read_all(None, FeedReadOptions::default())
        .await
        .expect("feed");
    let types: Vec<&str> = all.iter().map(|e| e.record.event_type.as_str()).collect();
    assert_eq!(types, ["A", "B", "C"]);
    let positions: Vec<&Position> = all.iter().map(|e| &e.position).collect();
    assert!(positions.windows(2).all(|w| w[0].token() < w[1].token()));

    let resumed = store
        .read_all(
            Some(&first.envelopes[0].position),
            FeedReadOptions::default(),
        )
        .await
        .expect("resume");
    let types: Vec<&str> = resumed
        .iter()
        .map(|e| e.record.event_type.as_str())
        .collect();
    assert_eq!(types, ["B", "C"]);

    let head = store.current_position().await.expect("head");
    assert_eq!(head.as_ref(), all.last().map(|e| &e.position));
}

/// A position from another store cannot be used to read the feed.
pub async fn check_feed_rejects_foreign_position<S: AggregateStore + GlobalFeed>(store: &S) {
    let foreign = Position::new("some-other-store", 1);
    let result = store
        .read_all(Some(&foreign), FeedReadOptions::default())
        .await;
    assert!(matches!(result, Err(StoreError::Position(_))));
}

/// With `Exact(0)` from many writers at once, exactly one append succeeds.
pub async fn check_concurrent_exact_zero_has_one_winner<S: AggregateStore>(store: &S) {
    let s = stream("Order");
    let attempts =
        (0..8).map(|_| store.append(&s, vec![record("A")], ExpectedVersion::Exact(Version::ZERO)));
    let results = futures::future::join_all(attempts).await;
    let wins = results.iter().filter(|r| r.is_ok()).count();
    assert_eq!(wins, 1, "exactly one concurrent writer must win");
    assert_eq!(
        store.stream_version(&s).await.expect("version"),
        Version::new(1)
    );
}

/// Paging the feed with `limit` and resuming from each page's last position returns every event once.
pub async fn check_feed_paging_with_limit<S: AggregateStore + GlobalFeed>(store: &S) {
    let s = stream("Order");
    let names = ["A", "B", "C", "D", "E"];
    for name in names {
        store
            .append(&s, vec![record(name)], ExpectedVersion::Any)
            .await
            .expect("append");
    }

    let mut seen = Vec::new();
    let mut from: Option<Position> = None;
    loop {
        let options = FeedReadOptions {
            limit: std::num::NonZeroUsize::new(2),
            ..Default::default()
        };
        let page = store.read_all(from.as_ref(), options).await.expect("page");
        let Some(last) = page.last() else { break };
        from = Some(last.position.clone());
        seen.extend(page.iter().map(|e| e.record.event_type.clone()));
    }
    assert_eq!(seen, names);
}
