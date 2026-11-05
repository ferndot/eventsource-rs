//! Behaviour of the in-memory read model store.

use eventsource_core::readmodel::ReadModelStore;
use eventsource_memory::InMemoryReadModelStore;
use serde_json::json;

#[tokio::test]
async fn upsert_then_get_returns_the_document() {
    let store = InMemoryReadModelStore::new();
    store
        .upsert("orders", "o-1", json!({"status": "placed"}))
        .await
        .expect("upsert");
    assert_eq!(
        store.get("orders", "o-1").await.expect("get"),
        Some(json!({"status": "placed"}))
    );
    store
        .upsert("orders", "o-1", json!({"status": "shipped"}))
        .await
        .expect("upsert again");
    assert_eq!(
        store.get("orders", "o-1").await.expect("get"),
        Some(json!({"status": "shipped"})),
        "a second upsert replaces the document"
    );
}

#[tokio::test]
async fn get_of_an_unknown_id_is_none() {
    let store = InMemoryReadModelStore::new();
    assert_eq!(store.get("orders", "missing").await.expect("get"), None);
}

#[tokio::test]
async fn delete_removes_the_document() {
    let store = InMemoryReadModelStore::new();
    store
        .upsert("orders", "o-1", json!(1))
        .await
        .expect("upsert");
    store.delete("orders", "o-1").await.expect("delete");
    assert_eq!(store.get("orders", "o-1").await.expect("get"), None);
    store
        .delete("orders", "o-1")
        .await
        .expect("deleting a missing document is not an error");
}

#[tokio::test]
async fn models_are_isolated() {
    let store = InMemoryReadModelStore::new();
    store
        .upsert("orders", "1", json!("order"))
        .await
        .expect("upsert");
    store
        .upsert("invoices", "1", json!("invoice"))
        .await
        .expect("upsert");
    assert_eq!(
        store.get("orders", "1").await.expect("get"),
        Some(json!("order"))
    );
    assert_eq!(
        store.get("invoices", "1").await.expect("get"),
        Some(json!("invoice"))
    );
    store.delete("orders", "1").await.expect("delete");
    assert_eq!(
        store.get("invoices", "1").await.expect("get"),
        Some(json!("invoice"))
    );
}
