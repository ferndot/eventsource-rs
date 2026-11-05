//! Port conformance for the `SQLite` store, each check on a fresh database file.

use std::sync::atomic::{AtomicUsize, Ordering};

use eventsource_sqlite::SqliteEventStore;

#[tokio::test]
async fn sqlite_store_passes_conformance() {
    let dir = tempfile::tempdir().expect("tempdir");
    let counter = AtomicUsize::new(0);
    eventsource_testing::run_all(|| {
        let n = counter.fetch_add(1, Ordering::Relaxed);
        SqliteEventStore::open(dir.path().join(format!("store-{n}.db"))).expect("open store")
    })
    .await;
}
