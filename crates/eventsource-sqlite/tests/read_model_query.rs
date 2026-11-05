//! Query, soft delete and versioning on `SQLite` behave like memory and survive reopen.

use eventsource_core::error::ReadModelError;
use eventsource_core::readmodel::{ReadModelQuery, ReadModelStore};
use eventsource_memory::InMemoryReadModelStore;
use eventsource_sqlite::SqliteReadModelStore;
use serde_json::{Value, json};

fn outcome<T: std::fmt::Debug>(result: Result<T, ReadModelError>) -> String {
    match result {
        Ok(value) => format!("ok {value:?}"),
        Err(ReadModelError::VersionConflict {
            model,
            id,
            expected,
            actual,
        }) => format!("conflict {model}/{id} expected {expected} actual {actual}"),
        Err(other) => panic!("unexpected error: {other:?}"),
    }
}

/// The write sequence; returns what each fallible step reported.
async fn write_sequence<S: ReadModelStore>(store: &S) -> Vec<String> {
    let mut log = Vec::new();
    // Mixed-case and numeric-looking ids pin down the byte-wise id order.
    for (id, doc) in [
        (
            "b-2",
            json!({"status": "placed", "qty": 2, "rush": true, "note": null}),
        ),
        (
            "B-1",
            json!({"status": "placed", "qty": 1.5, "rush": false}),
        ),
        ("a-3", json!({"status": "shipped", "qty": 2, "rush": true})),
        ("a-10", json!({"status": "placed", "qty": 2, "tags": ["x"]})),
        ("c-4", json!("not an object")),
    ] {
        store.upsert("orders", id, doc).await.expect("upsert");
    }
    store
        .upsert("invoices", "i-1", json!({"status": "placed"}))
        .await
        .expect("other model");
    store
        .upsert(
            "orders",
            "b-2",
            json!({"status": "placed", "qty": 2, "rush": true, "note": null, "x": 1}),
        )
        .await
        .expect("replace");
    log.push(outcome(store.soft_delete("orders", "a-10").await));
    log.push(outcome(store.soft_delete("orders", "a-10").await));
    log.push(outcome(store.soft_delete("orders", "zzz").await));
    log.push(outcome(store.restore("orders", "a-3").await));
    log.push(outcome(store.soft_delete("orders", "a-3").await));
    log.push(outcome(store.restore("orders", "a-3").await));
    log.push(outcome(store.soft_delete("orders", "B-1").await));
    log.push(outcome(
        store
            .put_if_version("orders", "n-1", json!({"status": "new"}), 0)
            .await,
    ));
    log.push(outcome(
        store
            .put_if_version("orders", "n-1", json!({"status": "dup"}), 0)
            .await,
    ));
    log.push(outcome(
        store
            .put_if_version("orders", "n-1", json!({"status": "v2"}), 1)
            .await,
    ));
    log.push(outcome(
        store
            .put_if_version("orders", "n-1", json!({"status": "stale"}), 1)
            .await,
    ));
    log.push(outcome(
        store.put_if_version("orders", "ghost", json!({}), 4).await,
    ));
    log.push(outcome(
        store
            .put_if_version("orders", "B-1", json!({"status": "placed", "qty": 9}), 3)
            .await,
    ));
    store.delete("orders", "c-4").await.expect("hard delete");
    store
        .delete("orders", "c-4")
        .await
        .expect("hard delete again");
    log
}

/// Everything readable about the store after the sequence.
async fn observe<S: ReadModelStore>(store: &S) -> Vec<String> {
    let mut seen = Vec::new();
    let queries = [
        ReadModelQuery::new("orders"),
        ReadModelQuery::new("orders").include_deleted(),
        ReadModelQuery::new("orders").filter("status", json!("placed")),
        ReadModelQuery::new("orders")
            .filter("status", json!("placed"))
            .include_deleted(),
        ReadModelQuery::new("orders").filter("qty", json!(2)),
        ReadModelQuery::new("orders")
            .filter("qty", json!(1.5))
            .include_deleted(),
        ReadModelQuery::new("orders").filter("rush", json!(true)),
        ReadModelQuery::new("orders").filter("note", json!(null)),
        ReadModelQuery::new("orders")
            .filter("tags", json!(["x"]))
            .include_deleted(),
        ReadModelQuery::new("orders")
            .filter("status", json!("placed"))
            .filter("rush", json!(true)),
        ReadModelQuery::new("orders")
            .include_deleted()
            .offset(1)
            .limit(2),
        ReadModelQuery::new("orders").include_deleted().offset(50),
        ReadModelQuery::new("orders").limit(0),
        ReadModelQuery::new("nothing"),
    ];
    for query in &queries {
        seen.push(format!(
            "query {query:?} => {:?}",
            store.query(query).await.expect("query")
        ));
        seen.push(format!(
            "count {query:?} => {}",
            store.count(query).await.expect("count")
        ));
    }
    for id in ["a-3", "a-10", "B-1", "b-2", "n-1", "c-4", "zzz"] {
        seen.push(format!(
            "get {id} => {:?}",
            store.get("orders", id).await.expect("get")
        ));
        seen.push(format!(
            "fetch {id} => {:?}",
            store.fetch("orders", id).await.expect("fetch")
        ));
    }
    seen.push(format!(
        "list => {:?}",
        store.list("orders").await.expect("list")
    ));
    seen
}

#[tokio::test]
async fn sqlite_query_soft_delete_and_versions_match_memory_and_survive_reopen() {
    let memory = InMemoryReadModelStore::new();
    let expected_log = write_sequence(&memory).await;
    let expected = observe(&memory).await;

    // Sanity: the sequence really exercised conflicts and soft deletes.
    assert!(expected_log.iter().any(|l| l.starts_with("conflict")));
    assert!(expected.iter().any(|l| l.contains("deleted: true")));

    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("rm.db");
    {
        let sqlite = SqliteReadModelStore::open(&path).expect("open");
        assert_eq!(write_sequence(&sqlite).await, expected_log);
        assert_eq!(observe(&sqlite).await, expected);
    }
    let reopened = SqliteReadModelStore::open(&path).expect("reopen");
    assert_eq!(observe(&reopened).await, expected, "after reopen");

    // Versions keep counting after reopen.
    let n1: Value = reopened
        .fetch("orders", "n-1")
        .await
        .expect("fetch")
        .map(|d| json!(d.version))
        .expect("n-1");
    assert_eq!(n1, json!(2));
    assert_eq!(
        reopened
            .put_if_version("orders", "n-1", json!({"status": "v3"}), 2)
            .await
            .expect("write after reopen"),
        3
    );
}

#[tokio::test]
async fn a_readmodels_table_from_before_versions_is_upgraded_in_place() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("old.db");
    rusqlite::Connection::open(&path)
        .expect("raw")
        .execute_batch(
            "CREATE TABLE readmodels (model TEXT NOT NULL, id TEXT NOT NULL, \
             document TEXT NOT NULL, PRIMARY KEY (model, id)); \
             INSERT INTO readmodels VALUES ('orders', 'old', '{\"a\":1}');",
        )
        .expect("old schema");
    let store = SqliteReadModelStore::open(&path).expect("open upgrades");
    let doc = store
        .fetch("orders", "old")
        .await
        .expect("fetch")
        .expect("kept");
    assert_eq!((doc.version, doc.deleted), (1, false));
    assert_eq!(
        store.get("orders", "old").await.expect("get"),
        Some(json!({"a": 1}))
    );
    drop(store);
    SqliteReadModelStore::open(&path).expect("second open is a no-op");
}
