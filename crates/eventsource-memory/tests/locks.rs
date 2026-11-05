//! Named locks with expiry on the in-memory store, driven by a manual clock.

use std::sync::Arc;
use std::time::Duration;

use eventsource_core::Clock;
use eventsource_core::lock::{LockName, LockOwner, LockStore, LockTtl};
use eventsource_memory::InMemoryLockStore;
use eventsource_testing::ManualClock;

fn name(n: &str) -> LockName {
    LockName::new(n).expect("name")
}
fn owner(o: &str) -> LockOwner {
    LockOwner::new(o).expect("owner")
}
fn ttl(secs: u64) -> LockTtl {
    LockTtl::new(Duration::from_secs(secs)).expect("ttl")
}

fn store() -> (InMemoryLockStore<Arc<ManualClock>>, Arc<ManualClock>) {
    let clock = Arc::new(ManualClock::default());
    (InMemoryLockStore::new(Arc::clone(&clock)), clock)
}

#[tokio::test]
async fn first_owner_acquires_and_second_is_refused_while_live() {
    let (locks, clock) = store();
    let lease = locks
        .try_acquire(&name("relay"), &owner("a"), ttl(30))
        .await
        .expect("acquire")
        .expect("free lock is granted");
    assert_eq!(lease.name(), &name("relay"));
    assert_eq!(lease.owner(), &owner("a"));
    assert_eq!(lease.expires_at(), clock.now() + Duration::from_secs(30));

    let refused = locks
        .try_acquire(&name("relay"), &owner("b"), ttl(30))
        .await
        .expect("second acquire");
    assert_eq!(refused, None);

    let other_name = locks
        .try_acquire(&name("projector"), &owner("b"), ttl(30))
        .await
        .expect("other name");
    assert!(other_name.is_some(), "locks are independent per name");
}
