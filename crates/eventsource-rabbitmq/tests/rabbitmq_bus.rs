//! Tests against a real `RabbitMQ`. Set `AMQP_URL`; defaults to `amqp://guest:guest@127.0.0.1:5672`.
#![allow(missing_docs)]

use chrono::Utc;
use eventsource_core::bus::EventPublisher;
use eventsource_core::bus::handler;
use eventsource_core::checkpoint::CheckpointStore;
use eventsource_core::envelope::{EventEnvelope, FeedReadOptions};
use eventsource_core::error::{BusError, StoreError};
use eventsource_core::event::{EventMeta, EventRecord};
use eventsource_core::ids::{AggregateId, CorrelationId};
use eventsource_core::position::Position;
use eventsource_core::store::GlobalFeed;
use eventsource_core::stream_id::{Category, StreamId};
use eventsource_core::subscription::{SubscriptionConfig, SubscriptionRunner};
use eventsource_core::version::Version;
use eventsource_memory::{InMemoryCheckpointStore, InMemoryEventBus};
use eventsource_rabbitmq::{RabbitMqConfig, RabbitMqConsumer, RabbitMqPublisher};
use serde_json::json;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

fn amqp_url() -> String {
    std::env::var("AMQP_URL").unwrap_or_else(|_| "amqp://guest:guest@127.0.0.1:5672".to_owned())
}

fn unique_config() -> RabbitMqConfig {
    let id = uuid::Uuid::new_v4();
    RabbitMqConfig::new(format!("test-ex-{id}"), format!("test-q-{id}"))
}

fn envelope(event_type: &str, n: u64) -> EventEnvelope {
    let meta = EventMeta::new(Utc::now(), CorrelationId::new());
    EventEnvelope {
        record: EventRecord::new(meta, event_type, json!({ "n": n })),
        stream: StreamId::new(AggregateId::new(), Category::new("Order").unwrap()),
        stream_version: Version::new(n),
        position: Position::new("source", n),
        stored_at: Utc::now(),
    }
}

fn page(limit: usize) -> FeedReadOptions {
    FeedReadOptions {
        limit: NonZeroUsize::new(limit),
        ..FeedReadOptions::default()
    }
}

/// Deletes the test queue and exchange over a raw connection.
async fn cleanup(config: &RabbitMqConfig) {
    let conn = lapin::Connection::connect(&amqp_url(), lapin::ConnectionProperties::default())
        .await
        .unwrap();
    let channel = conn.create_channel().await.unwrap();
    channel
        .queue_delete(
            config.queue.as_str().into(),
            lapin::options::QueueDeleteOptions::default(),
        )
        .await
        .unwrap();
    channel
        .exchange_delete(
            config.exchange.as_str().into(),
            lapin::options::ExchangeDeleteOptions::default(),
        )
        .await
        .unwrap();
    conn.close(200, "done".into()).await.unwrap();
}

#[tokio::test]
async fn published_envelope_reaches_the_queue_consumer() {
    let config = unique_config();
    let consumer = RabbitMqConsumer::connect(&amqp_url(), config.clone())
        .await
        .unwrap();
    let publisher = RabbitMqPublisher::new(&amqp_url(), config.clone());
    let sent = envelope("OrderPlaced", 1);

    publisher
        .publish(std::slice::from_ref(&sent))
        .await
        .unwrap();

    let got = consumer.read_all(None, page(10)).await.unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].record.meta.event_id, sent.record.meta.event_id);
    assert_eq!(got[0].record.event_type, "OrderPlaced");
    assert_eq!(got[0].record.payload, sent.record.payload);

    consumer.close().await.unwrap();
    cleanup(&config).await;
}

#[tokio::test]
async fn envelopes_arrive_in_publish_order() {
    let config = unique_config();
    let consumer = RabbitMqConsumer::connect(&amqp_url(), config.clone())
        .await
        .unwrap();
    let publisher = RabbitMqPublisher::new(&amqp_url(), config.clone());
    let first: Vec<_> = (1..=5).map(|n| envelope("Step", n)).collect();
    let second: Vec<_> = (6..=8).map(|n| envelope("Step", n)).collect();

    publisher.publish(&first).await.unwrap();
    publisher.publish(&second).await.unwrap();

    let got = consumer.read_all(None, page(100)).await.unwrap();
    let ids: Vec<_> = got.iter().map(|e| e.record.meta.event_id).collect();
    let expected: Vec<_> = first
        .iter()
        .chain(&second)
        .map(|e| e.record.meta.event_id)
        .collect();
    assert_eq!(ids, expected);

    consumer.close().await.unwrap();
    cleanup(&config).await;
}

#[tokio::test]
async fn binding_receives_only_the_bound_event_type() {
    let mut config = unique_config();
    config.bindings = vec!["OrderPlaced".to_owned()];
    let consumer = RabbitMqConsumer::connect(&amqp_url(), config.clone())
        .await
        .unwrap();
    let publisher = RabbitMqPublisher::new(&amqp_url(), config.clone());
    let sent = [
        envelope("OrderPlaced", 1),
        envelope("OrderShipped", 2),
        envelope("OrderPlaced", 3),
    ];

    publisher.publish(&sent).await.unwrap();

    let got = consumer.read_all(None, page(100)).await.unwrap();
    let ids: Vec<_> = got.iter().map(|e| e.record.meta.event_id).collect();
    assert_eq!(
        ids,
        vec![sent[0].record.meta.event_id, sent[2].record.meta.event_id]
    );
    assert!(got.iter().all(|e| e.record.event_type == "OrderPlaced"));

    consumer.close().await.unwrap();
    cleanup(&config).await;
}

#[tokio::test]
async fn unacknowledged_message_is_redelivered_after_reconnect() {
    let config = unique_config();
    let first = RabbitMqConsumer::connect(&amqp_url(), config.clone())
        .await
        .unwrap();
    let publisher = RabbitMqPublisher::new(&amqp_url(), config.clone());
    let sent = envelope("OrderPlaced", 1);
    publisher
        .publish(std::slice::from_ref(&sent))
        .await
        .unwrap();

    // Read it but never acknowledge: the cursor never moves past it.
    let got = first.read_all(None, page(10)).await.unwrap();
    assert_eq!(got.len(), 1);
    first.close().await.unwrap();

    let second = RabbitMqConsumer::connect(&amqp_url(), config.clone())
        .await
        .unwrap();
    let again = second.read_all(None, page(10)).await.unwrap();
    assert_eq!(again.len(), 1);
    assert_eq!(again[0].record.meta.event_id, sent.record.meta.event_id);

    second.close().await.unwrap();
    cleanup(&config).await;
}

#[tokio::test]
async fn message_is_acknowledged_once_the_cursor_moves_past_it() {
    let config = unique_config();
    let first = RabbitMqConsumer::connect(&amqp_url(), config.clone())
        .await
        .unwrap();
    let publisher = RabbitMqPublisher::new(&amqp_url(), config.clone());
    let sent: Vec<_> = (1..=3).map(|n| envelope("Step", n)).collect();
    publisher.publish(&sent).await.unwrap();

    let got = first.read_all(None, page(2)).await.unwrap();
    assert_eq!(got.len(), 2);
    // Reading on from the second event's position acknowledges the first two.
    let rest = first
        .read_all(Some(&got[1].position), page(2))
        .await
        .unwrap();
    assert_eq!(rest.len(), 1);
    first.close().await.unwrap();

    // Only the third, never acknowledged, comes back.
    let second = RabbitMqConsumer::connect(&amqp_url(), config.clone())
        .await
        .unwrap();
    let again = second.read_all(None, page(10)).await.unwrap();
    let ids: Vec<_> = again.iter().map(|e| e.record.meta.event_id).collect();
    assert_eq!(ids, vec![sent[2].record.meta.event_id]);

    second.close().await.unwrap();
    cleanup(&config).await;
}

#[tokio::test]
async fn publishing_to_an_unreachable_broker_is_a_backend_error() {
    let publisher = RabbitMqPublisher::new("amqp://guest:guest@127.0.0.1:1", unique_config());

    let result = publisher.publish(&[envelope("OrderPlaced", 1)]).await;

    assert!(
        matches!(result, Err(BusError::Backend(_))),
        "got {result:?}"
    );
}

/// Uses the in-memory checkpoint store and in-memory bus as the sink.
#[tokio::test]
async fn subscription_runner_delivers_and_checkpoints_from_the_queue() {
    let config = unique_config();
    let consumer = Arc::new(
        RabbitMqConsumer::connect(&amqp_url(), config.clone())
            .await
            .unwrap(),
    );
    let publisher = RabbitMqPublisher::new(&amqp_url(), config.clone());
    let sent: Vec<_> = (1..=5).map(|n| envelope("Step", n)).collect();
    publisher.publish(&sent).await.unwrap();

    let seen = Arc::new(Mutex::new(Vec::new()));
    let bus = InMemoryEventBus::new();
    let sink = Arc::clone(&seen);
    bus.subscribe_all(handler(move |e: EventEnvelope| {
        let sink = Arc::clone(&sink);
        async move {
            sink.lock().unwrap().push(e.record.meta.event_id);
            Ok(())
        }
    }));
    let checkpoints = Arc::new(InMemoryCheckpointStore::new());
    let mut sub = SubscriptionConfig::new("rabbit-sub");
    sub.batch_size = NonZeroUsize::new(2).unwrap();
    let mut runner =
        SubscriptionRunner::new(Arc::clone(&consumer), Arc::clone(&checkpoints), bus, sub);

    let stats = runner.run_until_caught_up().await.unwrap();

    let expected: Vec<_> = sent.iter().map(|e| e.record.meta.event_id).collect();
    assert_eq!(*seen.lock().unwrap(), expected);
    assert_eq!((stats.events_delivered, stats.batches), (5, 3));
    let saved = checkpoints.load("rabbit-sub").await.unwrap().unwrap();
    assert_eq!(saved.events_processed, 5);
    assert_eq!(saved.last_event_id, expected.last().copied());
    assert_eq!(saved.position, consumer.current_position().await.unwrap());

    // Everything was acknowledged: a new consumer finds the queue empty.
    consumer.close().await.unwrap();
    let again = RabbitMqConsumer::connect(&amqp_url(), config.clone())
        .await
        .unwrap();
    assert!(again.read_all(None, page(10)).await.unwrap().is_empty());
    again.close().await.unwrap();
    cleanup(&config).await;
}

#[tokio::test]
async fn feed_filters_are_rejected_and_leave_the_queue_untouched() {
    let config = unique_config();
    let consumer = RabbitMqConsumer::connect(&amqp_url(), config.clone())
        .await
        .unwrap();
    let publisher = RabbitMqPublisher::new(&amqp_url(), config.clone());
    publisher.publish(&[envelope("Step", 1)]).await.unwrap();

    let filtered = FeedReadOptions {
        category: Some(Category::new("Order").unwrap()),
        ..page(10)
    };
    let result = consumer.read_all(None, filtered).await;

    assert!(
        matches!(result, Err(StoreError::Backend(_))),
        "got {result:?}"
    );
    assert_eq!(consumer.read_all(None, page(10)).await.unwrap().len(), 1);

    consumer.close().await.unwrap();
    cleanup(&config).await;
}

#[tokio::test]
async fn a_position_from_another_store_is_rejected() {
    let config = unique_config();
    let consumer = RabbitMqConsumer::connect(&amqp_url(), config.clone())
        .await
        .unwrap();

    let result = consumer
        .read_all(Some(&Position::new("redis:other", 1)), page(10))
        .await;

    assert!(
        matches!(result, Err(StoreError::Position(_))),
        "got {result:?}"
    );

    consumer.close().await.unwrap();
    cleanup(&config).await;
}
