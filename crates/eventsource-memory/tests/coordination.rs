//! Leader election over the in-memory lock store, driven by a manual clock.

use std::sync::Arc;
use std::time::Duration;

use eventsource_core::coordination::Coordinator;
use eventsource_core::lock::{LockName, LockOwner, LockTtl};
use eventsource_memory::InMemoryLockStore;
use eventsource_testing::ManualClock;

fn member(
    store: &Arc<InMemoryLockStore<Arc<ManualClock>>>,
    clock: &Arc<ManualClock>,
    id: &str,
) -> Coordinator<InMemoryLockStore<Arc<ManualClock>>, Arc<ManualClock>> {
    Coordinator::new(
        Arc::clone(store),
        Arc::clone(clock),
        LockName::new("projections").expect("name"),
        LockOwner::new(id).expect("owner"),
        LockTtl::new(Duration::from_secs(30)).expect("ttl"),
    )
}

#[tokio::test]
async fn only_one_leader_and_takeover_after_ttl_without_renewal() {
    let clock = Arc::new(ManualClock::default());
    let store = Arc::new(InMemoryLockStore::new(Arc::clone(&clock)));
    let a = member(&store, &clock, "a");
    let b = member(&store, &clock, "b");

    assert!(a.try_become_leader().await.expect("a"), "a becomes leader");
    assert!(!b.try_become_leader().await.expect("b"), "b is refused");
    assert!(a.is_leader());
    assert!(!b.is_leader());

    clock.advance(Duration::from_secs(20));
    assert!(a.try_become_leader().await.expect("renew"), "a renews");
    clock.advance(Duration::from_secs(20));
    assert!(a.is_leader(), "renewal moved the expiry to t+50");
    assert!(!b.try_become_leader().await.expect("b"), "still a's lease");

    clock.advance(Duration::from_secs(10));
    assert!(!a.is_leader(), "a's lease ran out at t+50");
    assert!(b.try_become_leader().await.expect("b"), "b takes over");
    assert!(b.is_leader());
    assert!(!a.is_leader(), "a is no longer leader");
    assert!(
        !a.try_become_leader().await.expect("a"),
        "a cannot take it back"
    );
}

#[tokio::test]
async fn resign_lets_another_member_lead_immediately() {
    let clock = Arc::new(ManualClock::default());
    let store = Arc::new(InMemoryLockStore::new(Arc::clone(&clock)));
    let a = member(&store, &clock, "a");
    let b = member(&store, &clock, "b");

    assert!(a.try_become_leader().await.expect("a"));
    assert!(!b.try_become_leader().await.expect("b"));

    assert!(a.resign().await.expect("resign"), "a held a live lease");
    assert!(!a.is_leader(), "a stepped down");
    assert!(
        b.try_become_leader().await.expect("b"),
        "no TTL wait needed"
    );
    assert!(b.is_leader());

    assert!(
        !a.resign().await.expect("resign again"),
        "nothing to release"
    );
    assert!(b.is_leader(), "a non-leader's resign does not disturb b");
}
