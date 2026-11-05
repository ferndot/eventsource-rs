//! Data written through one handle is visible to later and concurrent handles.

mod common;

use eventsource_core::envelope::{FeedReadOptions, StreamReadOptions};
use eventsource_core::store::{AggregateStore, GlobalFeed};
use eventsource_core::{AggregateId, Category, ExpectedVersion, StreamId, Version};
use eventsource_postgres::PostgresEventStore;
use eventsource_testing::record;

fn order_stream() -> StreamId {
    StreamId::new(
        AggregateId::new(),
        Category::new("Order").expect("category"),
    )
}

async fn open(schema: &str) -> PostgresEventStore {
    PostgresEventStore::open(&common::database_url(), schema)
        .await
        .expect("open")
}

#[tokio::test]
async fn reopening_the_schema_returns_the_same_events_and_positions() {
    let schema = common::fresh_schema();
    let stream = order_stream();

    let (written, head) = {
        let store = open(&schema).await;
        store
            .append(
                &stream,
                vec![record("A"), record("B")],
                ExpectedVersion::NoStream,
            )
            .await
            .expect("append");
        let written = store
            .read_all(None, FeedReadOptions::default())
            .await
            .expect("feed");
        (written, store.current_position().await.expect("head"))
    };
    assert_eq!(written.len(), 2);

    // A new connection on the same schema.
    let reopened = open(&schema).await;
    let again = reopened
        .read_all(None, FeedReadOptions::default())
        .await
        .expect("feed after reopen");
    assert_eq!(again, written, "events, positions and timestamps survive");
    assert_eq!(reopened.current_position().await.expect("head"), head);
    assert_eq!(
        reopened.stream_version(&stream).await.expect("version"),
        Version::new(2)
    );
    let by_stream = reopened
        .read_stream(&stream, StreamReadOptions::default())
        .await
        .expect("stream");
    assert_eq!(by_stream, written);

    // Positions handed out before the restart are still valid after it.
    let resumed = reopened
        .read_all(
            written.first().map(|e| &e.position),
            FeedReadOptions::default(),
        )
        .await
        .expect("resume");
    assert_eq!(resumed, written[1..]);
}

#[tokio::test]
async fn handles_on_one_schema_share_positions() {
    let schema = common::fresh_schema();
    let writer = open(&schema).await;
    let reader = open(&schema).await;
    let stream = order_stream();
    writer
        .append(&stream, vec![record("A")], ExpectedVersion::Any)
        .await
        .expect("append");
    let position = writer.current_position().await.expect("head");
    assert_eq!(reader.current_position().await.expect("head"), position);
    let tail = reader
        .read_all(position.as_ref(), FeedReadOptions::default())
        .await
        .expect("read");
    assert!(tail.is_empty());
}

#[tokio::test]
async fn schemas_are_isolated_from_each_other() {
    let (first, second) = (open(&common::fresh_schema()).await, open(&common::fresh_schema()).await);
    first
        .append(&order_stream(), vec![record("A")], ExpectedVersion::Any)
        .await
        .expect("append");
    assert_eq!(second.current_position().await.expect("head"), None);
}
