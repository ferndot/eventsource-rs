//! Two connections racing on one stream have exactly one winner.

mod common;

use std::sync::Barrier;

use futures::executor::block_on;

use eventsource_core::error::StoreError;
use eventsource_core::store::AggregateStore;
use eventsource_core::{AggregateId, Category, ExpectedVersion, StreamId, Version};
use eventsource_postgres::PostgresEventStore;
use eventsource_testing::record;

fn order_stream() -> StreamId {
    StreamId::new(
        AggregateId::new(),
        Category::new("Order").expect("category"),
    )
}

/// Two handles race from two real threads, many rounds, each round on a fresh stream.
#[test]
fn two_connections_have_one_winner_for_exact_zero() {
    const ROUNDS: usize = 50;
    // The runtime drives the connections; the racing threads only block on the futures.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    let schema = common::fresh_schema();
    let open = || {
        runtime
            .block_on(PostgresEventStore::open(&common::database_url(), &schema))
            .expect("open")
    };
    let (first, second) = (open(), open());
    let exact_zero = ExpectedVersion::Exact(Version::ZERO);

    for round in 0..ROUNDS {
        let stream = order_stream();
        let barrier = Barrier::new(2);
        let race = |store: &PostgresEventStore| {
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
