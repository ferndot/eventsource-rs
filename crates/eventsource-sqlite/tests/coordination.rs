//! Leader election persisted in `SQLite`.

use std::sync::Arc;
use std::time::Duration;

use eventsource_core::coordination::Coordinator;
use eventsource_core::lock::{LockName, LockOwner, LockTtl};
use eventsource_sqlite::SqliteLockStore;
use eventsource_testing::ManualClock;

fn member(
    store: SqliteLockStore<Arc<ManualClock>>,
    clock: &Arc<ManualClock>,
    id: &str,
) -> Coordinator<SqliteLockStore<Arc<ManualClock>>, Arc<ManualClock>> {
    Coordinator::new(
        Arc::new(store),
        Arc::clone(clock),
        LockName::new("projections").expect("name"),
        LockOwner::new(id).expect("owner"),
        LockTtl::new(Duration::from_secs(30)).expect("ttl"),
    )
}

#[tokio::test]
async fn leadership_survives_reopen_and_a_second_handle_sees_the_leader() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("coord.db");
    let clock = Arc::new(ManualClock::default());

    let a = member(
        SqliteLockStore::open(&path, Arc::clone(&clock)).expect("open"),
        &clock,
        "a",
    );
    assert!(a.try_become_leader().await.expect("a"));
    drop(a);

    // Reopen: a new process for member a, and a second handle for member b.
    let b = member(
        SqliteLockStore::open(&path, Arc::clone(&clock)).expect("open b"),
        &clock,
        "b",
    );
    assert!(
        !b.try_become_leader().await.expect("b"),
        "a's lease survived the reopen"
    );
    let a = member(
        SqliteLockStore::open(&path, Arc::clone(&clock)).expect("reopen a"),
        &clock,
        "a",
    );
    assert!(
        a.try_become_leader().await.expect("a"),
        "a renews its old lease"
    );

    clock.advance(Duration::from_secs(31));
    assert!(
        b.try_become_leader().await.expect("b"),
        "takeover after ttl"
    );
    assert!(
        !a.try_become_leader().await.expect("a"),
        "a sees b as the leader"
    );
}
