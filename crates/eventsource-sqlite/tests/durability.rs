//! Data written through one handle is visible to later and concurrent handles.

use std::sync::Barrier;

use futures::executor::block_on;

use eventsource_core::envelope::{FeedReadOptions, StreamReadOptions};
use eventsource_core::error::StoreError;
use eventsource_core::store::{AggregateStore, GlobalFeed};
use eventsource_core::{AggregateId, Category, ExpectedVersion, StreamId, Version};
use eventsource_sqlite::SqliteEventStore;
use eventsource_testing::record;

fn order_stream() -> StreamId {
    StreamId::new(
        AggregateId::new(),
        Category::new("Order").expect("category"),
    )
}

#[tokio::test]
async fn reopening_the_file_returns_the_same_events_and_positions() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("events.db");
    let stream = order_stream();

    let (written, head) = {
        let store = SqliteEventStore::open(&path).expect("open");
        store
            .append(
                &stream,
                vec![record("A"), record("B")],
                ExpectedVersion::NoStream,
            )
            .await
            .expect("append");
        let written = store
            .read_all(None, FeedReadOptions::default())
            .await
            .expect("feed");
        (written, store.current_position().await.expect("head"))
    };
    assert_eq!(written.len(), 2);

    let reopened = SqliteEventStore::open(&path).expect("reopen");
    let again = reopened
        .read_all(None, FeedReadOptions::default())
        .await
        .expect("feed after reopen");
    assert_eq!(again, written, "events, positions and timestamps survive");
    assert_eq!(reopened.current_position().await.expect("head"), head);
    assert_eq!(
        reopened.stream_version(&stream).await.expect("version"),
        Version::new(2)
    );
    let by_stream = reopened
        .read_stream(&stream, StreamReadOptions::default())
        .await
        .expect("stream");
    assert_eq!(by_stream, written);

    // Positions handed out before the restart are still valid after it.
    let resumed = reopened
        .read_all(
            written.first().map(|e| &e.position),
            FeedReadOptions::default(),
        )
        .await
        .expect("resume");
    assert_eq!(resumed, written[1..]);
}

/// Two handles race from two threads, many rounds, each round on a fresh stream.
#[test]
fn two_handles_on_one_file_have_one_winner_for_exact_zero() {
    const ROUNDS: usize = 50;
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("events.db");
    let first = SqliteEventStore::open(&path).expect("open first");
    let second = SqliteEventStore::open(&path).expect("open second");
    let exact_zero = ExpectedVersion::Exact(Version::ZERO);

    for round in 0..ROUNDS {
        let stream = order_stream();
        let barrier = Barrier::new(2);
        let race = |store: &SqliteEventStore| {
            barrier.wait();
            block_on(store.append(&stream, vec![record("A")], exact_zero))
        };
        let (a, b) = std::thread::scope(|scope| {
            let a = scope.spawn(|| race(&first));
            let b = scope.spawn(|| race(&second));
            (a.join().expect("thread a"), b.join().expect("thread b"))
        });
        let results = [a, b];
        let wins = results.iter().filter(|r| r.is_ok()).count();
        assert_eq!(
            wins, 1,
            "round {round}: exactly one handle may win: {results:?}"
        );
        assert!(
            results
                .iter()
                .any(|r| matches!(r, Err(StoreError::ConcurrencyConflict { .. }))),
            "round {round}: the loser must see a concurrency conflict: {results:?}"
        );
        for store in [&first, &second] {
            let version = block_on(store.stream_version(&stream)).expect("version");
            assert_eq!(version, Version::new(1));
        }
    }
}

#[tokio::test]
async fn handles_on_one_file_share_positions() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("events.db");
    let writer = SqliteEventStore::open(&path).expect("open writer");
    let reader = SqliteEventStore::open(&path).expect("open reader");
    let stream = order_stream();
    writer
        .append(&stream, vec![record("A")], ExpectedVersion::Any)
        .await
        .expect("append");
    let position = writer.current_position().await.expect("head");
    assert_eq!(reader.current_position().await.expect("head"), position);
    let tail = reader
        .read_all(position.as_ref(), FeedReadOptions::default())
        .await
        .expect("read");
    assert!(tail.is_empty());
}
