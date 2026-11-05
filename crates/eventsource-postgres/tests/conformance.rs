//! Port conformance for the `PostgreSQL` store, each check on a fresh schema.

mod common;

use eventsource_postgres::PostgresEventStore;

#[tokio::test]
async fn postgres_store_passes_conformance() {
    // The sync factory needs the store opened ahead of time, so build a pool of
    // fresh stores up front: one per conformance check.
    const CHECKS: usize = 11;
    let mut stores = Vec::new();
    for _ in 0..CHECKS {
        stores.push(
            PostgresEventStore::open(&common::database_url(), &common::fresh_schema())
                .await
                .expect("open store"),
        );
    }
    let stores = std::sync::Mutex::new(stores);
    eventsource_testing::run_all(|| stores.lock().expect("lock").pop().expect("a spare store"))
        .await;
}
