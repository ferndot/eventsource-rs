//! Tests against a real RabbitMQ. Set `AMQP_URL`; defaults to `amqp://guest:guest@127.0.0.1:5672`.
#![allow(missing_docs)]

use chrono::Utc;
use eventsource_core::bus::EventPublisher;
use eventsource_core::envelope::{EventEnvelope, FeedReadOptions};
use eventsource_core::event::{EventMeta, EventRecord};
use eventsource_core::ids::{AggregateId, CorrelationId};
use eventsource_core::position::Position;
use eventsource_core::store::GlobalFeed;
use eventsource_core::stream_id::{Category, StreamId};
use eventsource_core::version::Version;
use eventsource_rabbitmq::{RabbitMqConfig, RabbitMqConsumer, RabbitMqPublisher};
use serde_json::json;
use std::num::NonZeroUsize;

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
        .queue_delete(config.queue.as_str().into(), lapin::options::QueueDeleteOptions::default())
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
    let consumer = RabbitMqConsumer::connect(&amqp_url(), config.clone()).await.unwrap();
    let publisher = RabbitMqPublisher::new(&amqp_url(), config.clone());
    let sent = envelope("OrderPlaced", 1);

    publisher.publish(std::slice::from_ref(&sent)).await.unwrap();

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
    let consumer = RabbitMqConsumer::connect(&amqp_url(), config.clone()).await.unwrap();
    let publisher = RabbitMqPublisher::new(&amqp_url(), config.clone());
    let first: Vec<_> = (1..=5).map(|n| envelope("Step", n)).collect();
    let second: Vec<_> = (6..=8).map(|n| envelope("Step", n)).collect();

    publisher.publish(&first).await.unwrap();
    publisher.publish(&second).await.unwrap();

    let got = consumer.read_all(None, page(100)).await.unwrap();
    let ids: Vec<_> = got.iter().map(|e| e.record.meta.event_id).collect();
    let expected: Vec<_> = first.iter().chain(&second).map(|e| e.record.meta.event_id).collect();
    assert_eq!(ids, expected);

    consumer.close().await.unwrap();
    cleanup(&config).await;
}
