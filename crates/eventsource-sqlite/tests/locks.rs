//! Named locks with expiry on `SQLite`, driven by a manual clock.

use std::sync::Arc;
use std::time::Duration;

use eventsource_core::Clock;
use eventsource_core::lock::{LockName, LockOwner, LockStore, LockTtl};
use eventsource_sqlite::SqliteLockStore;
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

#[tokio::test]
async fn second_owner_is_refused_until_the_ttl_passes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let clock = Arc::new(ManualClock::default());
    let locks = SqliteLockStore::open(dir.path().join("locks.db"), Arc::clone(&clock)).expect("open");

    let lease = locks
        .try_acquire(&name("relay"), &owner("a"), ttl(30))
        .await
        .expect("acquire")
        .expect("free lock is granted");
    assert_eq!(lease.expires_at(), clock.now() + Duration::from_secs(30));
    assert_eq!(
        locks
            .try_acquire(&name("relay"), &owner("b"), ttl(30))
            .await
            .expect("acquire"),
        None
    );

    clock.advance(Duration::from_secs(29));
    assert_eq!(
        locks
            .try_acquire(&name("relay"), &owner("b"), ttl(30))
            .await
            .expect("acquire"),
        None,
        "still live one second before expiry"
    );
    clock.advance(Duration::from_secs(1));
    let lease = locks
        .try_acquire(&name("relay"), &owner("b"), ttl(30))
        .await
        .expect("acquire")
        .expect("expired lease no longer excludes");
    assert_eq!(lease.owner(), &owner("b"));
}

#[tokio::test]
async fn release_and_renew_follow_ownership() {
    let dir = tempfile::tempdir().expect("tempdir");
    let clock = Arc::new(ManualClock::default());
    let locks = SqliteLockStore::open(dir.path().join("locks.db"), Arc::clone(&clock)).expect("open");
    locks
        .try_acquire(&name("relay"), &owner("a"), ttl(30))
        .await
        .expect("acquire");

    assert!(!locks.release(&name("relay"), &owner("b")).await.expect("release"));
    assert_eq!(
        locks.renew(&name("relay"), &owner("b"), ttl(60)).await.expect("renew"),
        None
    );
    clock.advance(Duration::from_secs(20));
    let renewed = locks
        .renew(&name("relay"), &owner("a"), ttl(60))
        .await
        .expect("renew")
        .expect("owner may renew");
    assert_eq!(renewed.expires_at(), clock.now() + Duration::from_secs(60));

    // Past the original expiry (t=30) but inside the renewed one (t=80).
    clock.advance(Duration::from_secs(40));
    assert_eq!(
        locks
            .try_acquire(&name("relay"), &owner("b"), ttl(30))
            .await
            .expect("acquire"),
        None,
        "renewal keeps the lock held"
    );

    assert!(locks.release(&name("relay"), &owner("a")).await.expect("release"));
    assert!(
        locks
            .try_acquire(&name("relay"), &owner("b"), ttl(30))
            .await
            .expect("acquire")
            .is_some()
    );
}

#[tokio::test]
async fn a_lock_held_before_reopen_is_still_held_after() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("locks.db");
    let clock = Arc::new(ManualClock::default());
    {
        let locks = SqliteLockStore::open(&path, Arc::clone(&clock)).expect("open");
        locks
            .try_acquire(&name("relay"), &owner("a"), ttl(30))
            .await
            .expect("acquire")
            .expect("granted");
    }
    let reopened = SqliteLockStore::open(&path, Arc::clone(&clock)).expect("reopen");
    assert_eq!(
        reopened
            .try_acquire(&name("relay"), &owner("b"), ttl(30))
            .await
            .expect("acquire"),
        None,
        "lease survives reopen"
    );
    assert!(
        reopened
            .renew(&name("relay"), &owner("a"), ttl(30))
            .await
            .expect("renew")
            .is_some(),
        "the original owner still holds it"
    );
    clock.advance(Duration::from_secs(31));
    assert!(
        reopened
            .try_acquire(&name("relay"), &owner("b"), ttl(30))
            .await
            .expect("acquire")
            .is_some(),
        "and it still expires"
    );
}

#[test]
fn two_handles_racing_for_a_free_lock_produce_exactly_one_winner() {
    use std::sync::Barrier;

    use futures::executor::block_on;

    const ROUNDS: usize = 100;
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("locks.db");
    let clock = Arc::new(ManualClock::default());
    let first = SqliteLockStore::open(&path, Arc::clone(&clock)).expect("open first");
    let second = SqliteLockStore::open(&path, Arc::clone(&clock)).expect("open second");

    for round in 0..ROUNDS {
        let lock_name = name(&format!("relay-{round}"));
        let barrier = Barrier::new(2);
        let race = |store: &SqliteLockStore<Arc<ManualClock>>, who: &str| {
            let who = owner(who);
            barrier.wait();
            block_on(store.try_acquire(&lock_name, &who, ttl(30)))
        };
        let (a, b) = std::thread::scope(|scope| {
            let a = scope.spawn(|| race(&first, "a"));
            let b = scope.spawn(|| race(&second, "b"));
            (a.join().expect("thread a"), b.join().expect("thread b"))
        });
        let results = [a, b];
        assert!(
            results.iter().all(Result::is_ok),
            "round {round}: a refusal is a value, never an error: {results:?}"
        );
        let wins = results
            .iter()
            .filter(|r| matches!(r, Ok(Some(_))))
            .count();
        assert_eq!(wins, 1, "round {round}: exactly one winner: {results:?}");
    }
}
