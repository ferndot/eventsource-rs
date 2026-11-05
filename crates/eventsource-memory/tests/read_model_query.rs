//! Querying, soft delete and versioning on the in-memory read model store.

use eventsource_core::error::ReadModelError;
use eventsource_core::readmodel::{ReadModelQuery, ReadModelStore};
use eventsource_memory::InMemoryReadModelStore;
use serde_json::json;

async fn seeded() -> InMemoryReadModelStore {
    let store = InMemoryReadModelStore::new();
    // Inserted out of id order on purpose.
    for (id, status) in [
        ("o-3", "placed"),
        ("o-1", "placed"),
        ("o-5", "shipped"),
        ("o-2", "shipped"),
        ("o-4", "placed"),
    ] {
        store
            .upsert("orders", id, json!({"status": status, "n": id}))
            .await
            .expect("upsert");
    }
    store
        .upsert("invoices", "i-1", json!({"status": "placed"}))
        .await
        .expect("other model");
    store
}

fn ids(docs: &[eventsource_core::readmodel::StoredDocument]) -> Vec<&str> {
    docs.iter().map(|d| d.id.as_str()).collect()
}

#[tokio::test]
async fn query_filters_by_equality_in_id_order_with_limit_offset_and_count() {
    let store = seeded().await;

    let placed = ReadModelQuery::new("orders").filter("status", json!("placed"));
    let found = store.query(&placed).await.expect("query");
    assert_eq!(ids(&found), ["o-1", "o-3", "o-4"]);
    assert_eq!(found[0].document, json!({"status": "placed", "n": "o-1"}));
    assert_eq!(store.count(&placed).await.expect("count"), 3);

    let page = placed.clone().offset(1).limit(1);
    assert_eq!(ids(&store.query(&page).await.expect("page")), ["o-3"]);
    assert_eq!(
        store.count(&page).await.expect("count"),
        3,
        "count ignores limit and offset"
    );

    let all = ReadModelQuery::new("orders");
    assert_eq!(store.query(&all).await.expect("all").len(), 5);

    let none = ReadModelQuery::new("orders").filter("status", json!("cancelled"));
    assert!(store.query(&none).await.expect("none").is_empty());
    assert_eq!(store.count(&none).await.expect("count"), 0);

    let two = ReadModelQuery::new("orders")
        .filter("status", json!("shipped"))
        .filter("n", json!("o-5"));
    assert_eq!(ids(&store.query(&two).await.expect("two")), ["o-5"]);
}

#[tokio::test]
async fn soft_deleted_documents_are_hidden_until_included_or_restored() {
    let store = seeded().await;

    assert!(
        store
            .soft_delete("orders", "o-3")
            .await
            .expect("soft delete")
    );
    assert!(
        !store.soft_delete("orders", "o-3").await.expect("again"),
        "already deleted"
    );
    assert!(
        !store.soft_delete("orders", "nope").await.expect("missing"),
        "missing"
    );

    assert_eq!(store.get("orders", "o-3").await.expect("get"), None);
    let placed = ReadModelQuery::new("orders").filter("status", json!("placed"));
    assert_eq!(ids(&store.query(&placed).await.expect("q")), ["o-1", "o-4"]);
    assert_eq!(store.count(&placed).await.expect("count"), 2);

    let with_deleted = placed.clone().include_deleted();
    let found = store.query(&with_deleted).await.expect("q");
    assert_eq!(ids(&found), ["o-1", "o-3", "o-4"]);
    assert!(found[1].deleted && !found[0].deleted);
    assert_eq!(store.count(&with_deleted).await.expect("count"), 3);

    let fetched = store.fetch("orders", "o-3").await.expect("fetch");
    assert!(fetched.expect("still stored").deleted);

    assert!(store.restore("orders", "o-3").await.expect("restore"));
    assert!(
        !store.restore("orders", "o-3").await.expect("again"),
        "not deleted any more"
    );
    assert_eq!(
        store.get("orders", "o-3").await.expect("get"),
        Some(json!({"status": "placed", "n": "o-3"}))
    );
    assert_eq!(store.count(&placed).await.expect("count"), 3);

    // A hard delete still removes it for good, soft-deleted or not.
    store.soft_delete("orders", "o-1").await.expect("soft");
    store.delete("orders", "o-1").await.expect("hard");
    assert_eq!(store.fetch("orders", "o-1").await.expect("fetch"), None);
}

async fn version_of(store: &InMemoryReadModelStore, id: &str) -> Option<u64> {
    store
        .fetch("orders", id)
        .await
        .expect("fetch")
        .map(|d| d.version)
}

#[tokio::test]
async fn put_if_version_checks_the_expected_version() {
    let store = InMemoryReadModelStore::new();

    // Version 0 means absent: creates at version 1.
    let v1 = store
        .put_if_version("orders", "o-1", json!({"n": 1}), 0)
        .await
        .expect("create");
    assert_eq!(v1, 1);
    assert_eq!(version_of(&store, "o-1").await, Some(1));

    // Creating again fails: it exists.
    let err = store
        .put_if_version("orders", "o-1", json!({"n": "dup"}), 0)
        .await
        .expect_err("exists");
    assert!(matches!(
        err,
        ReadModelError::VersionConflict {
            expected: 0,
            actual: 1,
            ..
        }
    ));

    // The current version succeeds and increments.
    let v2 = store
        .put_if_version("orders", "o-1", json!({"n": 2}), 1)
        .await
        .expect("update");
    assert_eq!(v2, 2);
    assert_eq!(
        store.get("orders", "o-1").await.expect("get"),
        Some(json!({"n": 2}))
    );

    // A stale version is a typed conflict and leaves the document unchanged.
    let err = store
        .put_if_version("orders", "o-1", json!({"n": "stale"}), 1)
        .await
        .expect_err("stale");
    let ReadModelError::VersionConflict {
        model,
        id,
        expected,
        actual,
    } = err
    else {
        panic!("expected a conflict, got {err:?}");
    };
    assert_eq!(
        (model.as_str(), id.as_str(), expected, actual),
        ("orders", "o-1", 1, 2)
    );
    assert_eq!(
        store.get("orders", "o-1").await.expect("get"),
        Some(json!({"n": 2}))
    );
    assert_eq!(version_of(&store, "o-1").await, Some(2));

    // A missing document has actual version 0, so a non-zero expectation conflicts.
    let err = store
        .put_if_version("orders", "o-9", json!({}), 3)
        .await
        .expect_err("missing");
    assert!(matches!(
        err,
        ReadModelError::VersionConflict {
            expected: 3,
            actual: 0,
            ..
        }
    ));
    assert_eq!(version_of(&store, "o-9").await, None);
}

#[tokio::test]
async fn every_write_increments_the_version() {
    let store = InMemoryReadModelStore::new();
    store.upsert("orders", "o-1", json!(1)).await.expect("up");
    assert_eq!(version_of(&store, "o-1").await, Some(1));
    store.upsert("orders", "o-1", json!(2)).await.expect("up");
    assert_eq!(version_of(&store, "o-1").await, Some(2));
    store.soft_delete("orders", "o-1").await.expect("soft");
    assert_eq!(version_of(&store, "o-1").await, Some(3));
    store.restore("orders", "o-1").await.expect("restore");
    assert_eq!(version_of(&store, "o-1").await, Some(4));

    // A soft-deleted document still exists for versioning, and keeps its flag when written.
    store.soft_delete("orders", "o-1").await.expect("soft");
    let v = store
        .put_if_version("orders", "o-1", json!(3), 5)
        .await
        .expect("write while deleted");
    assert_eq!(v, 6);
    assert_eq!(store.get("orders", "o-1").await.expect("get"), None);
    assert!(
        store
            .fetch("orders", "o-1")
            .await
            .expect("f")
            .expect("some")
            .deleted
    );

    // A hard delete forgets the version: the id can be created again.
    store.delete("orders", "o-1").await.expect("delete");
    store
        .put_if_version("orders", "o-1", json!(4), 0)
        .await
        .expect("recreate");
    assert_eq!(version_of(&store, "o-1").await, Some(1));
}

/// Two writers hold the same version and race to write it: exactly one wins per round.
#[test]
fn racing_put_if_version_has_exactly_one_winner_per_round() {
    use std::sync::{Arc, Barrier};

    use futures::executor::block_on;

    const ROUNDS: usize = 50;
    let store = Arc::new(InMemoryReadModelStore::new());
    for round in 0..ROUNDS {
        let id = format!("doc-{round}");
        let expected =
            block_on(store.put_if_version("race", &id, json!({"seed": true}), 0)).expect("seed");
        let barrier = Arc::new(Barrier::new(2));
        let handles: Vec<_> = (0..2)
            .map(|writer| {
                let (store, barrier, id) = (Arc::clone(&store), Arc::clone(&barrier), id.clone());
                std::thread::spawn(move || {
                    barrier.wait();
                    block_on(store.put_if_version("race", &id, json!({"writer": writer}), expected))
                })
            })
            .collect();
        let results: Vec<_> = handles
            .into_iter()
            .map(|h| h.join().expect("join"))
            .collect();
        let winners: Vec<_> = results.iter().filter(|r| r.is_ok()).collect();
        assert_eq!(winners.len(), 1, "round {round}: {results:?}");
        let loser = results.iter().find(|r| r.is_err()).expect("a loser");
        assert!(
            matches!(loser, Err(ReadModelError::VersionConflict { .. })),
            "round {round}: {loser:?}"
        );
        let stored = block_on(store.fetch("race", &id))
            .expect("fetch")
            .expect("some");
        assert_eq!(stored.version, expected + 1, "round {round}");
    }
}
