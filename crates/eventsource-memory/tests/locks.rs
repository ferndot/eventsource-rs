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

#[tokio::test]
async fn release_by_owner_frees_the_lock_and_by_non_owner_does_nothing() {
    let (locks, _clock) = store();
    locks
        .try_acquire(&name("relay"), &owner("a"), ttl(30))
        .await
        .expect("acquire");

    let by_stranger = locks.release(&name("relay"), &owner("b")).await.expect("release");
    assert!(!by_stranger, "a non-owner releases nothing");
    assert_eq!(
        locks
            .try_acquire(&name("relay"), &owner("b"), ttl(30))
            .await
            .expect("acquire"),
        None,
        "lock must still be held by a"
    );

    let by_owner = locks.release(&name("relay"), &owner("a")).await.expect("release");
    assert!(by_owner);
    assert!(
        locks
            .try_acquire(&name("relay"), &owner("b"), ttl(30))
            .await
            .expect("acquire")
            .is_some(),
        "released lock is free"
    );

    let again = locks.release(&name("never-held"), &owner("a")).await.expect("release");
    assert!(!again, "releasing an unheld lock is a no-op");
}
