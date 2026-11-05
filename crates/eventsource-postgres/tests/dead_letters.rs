//! Dead letters persist in the schema.

mod common;

use eventsource_core::dead_letter::{DeadLetterEntry, DeadLetterStore};
use eventsource_core::envelope::FeedReadOptions;
use eventsource_core::store::{AggregateStore, GlobalFeed};
use eventsource_core::{AggregateId, Category, ExpectedVersion, StreamId};
use eventsource_postgres::{PostgresDeadLetterStore, PostgresEventStore};
use eventsource_testing::record;

#[tokio::test]
async fn dead_letters_survive_reopen() {
    let url = common::database_url();
    let schema = common::fresh_schema();
    let events = PostgresEventStore::open(&url, &schema)
        .await
        .expect("open events");
    for name in ["First", "Second"] {
        let stream = StreamId::new(
            AggregateId::new(),
            Category::new("Order").expect("category"),
        );
        events
            .append(&stream, vec![record(name)], ExpectedVersion::NoStream)
            .await
            .expect("append");
    }
    let envelopes = events
        .read_all(None, FeedReadOptions::default())
        .await
        .expect("read");

    let (first_id, second_id);
    {
        let store = PostgresDeadLetterStore::open(&url, &schema)
            .await
            .expect("open");
        assert!(store.list("orders").await.expect("list").is_empty());
        first_id = store
            .park(DeadLetterEntry {
                subscription: "orders".to_owned(),
                envelope: envelopes[0].clone(),
                error: "boom".to_owned(),
                attempts: 5,
            })
            .await
            .expect("park first");
        second_id = store
            .park(DeadLetterEntry {
                subscription: "billing".to_owned(),
                envelope: envelopes[1].clone(),
                error: "bang".to_owned(),
                attempts: 2,
            })
            .await
            .expect("park second");
    }

    let reopened = PostgresDeadLetterStore::open(&url, &schema)
        .await
        .expect("reopen");
    let orders = reopened.list("orders").await.expect("list");
    assert_eq!(orders.len(), 1);
    assert_eq!(orders[0].id, first_id);
    assert_eq!(orders[0].entry.subscription, "orders");
    assert_eq!(orders[0].entry.envelope, envelopes[0]);
    assert_eq!(orders[0].entry.error, "boom");
    assert_eq!(orders[0].entry.attempts, 5);
    let billing = reopened.list("billing").await.expect("list");
    assert_eq!(billing.len(), 1);
    assert_eq!(billing[0].id, second_id);

    assert!(reopened.remove(first_id).await.expect("remove"));
    assert!(!reopened.remove(first_id).await.expect("remove again"));
    drop(reopened);
    let again = PostgresDeadLetterStore::open(&url, &schema)
        .await
        .expect("reopen again");
    assert!(again.list("orders").await.expect("list").is_empty());
    assert_eq!(again.list("billing").await.expect("list").len(), 1);
}
