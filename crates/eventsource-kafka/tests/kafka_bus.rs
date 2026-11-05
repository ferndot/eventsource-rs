//! Tests against a real Kafka broker. Set `KAFKA_BOOTSTRAP`; defaults to `localhost:9092`.
#![allow(missing_docs)]

use std::num::NonZeroUsize;
use std::time::Duration;

use chrono::Utc;
use eventsource_core::bus::EventPublisher;
use eventsource_core::envelope::EventEnvelope;
use eventsource_core::event::{EventMeta, EventRecord};
use eventsource_core::ids::{AggregateId, CorrelationId};
use eventsource_core::position::Position;
use eventsource_core::stream_id::{Category, StreamId};
use eventsource_core::version::Version;
use eventsource_kafka::{KafkaBusConfig, KafkaConsumer, KafkaConsumerConfig, KafkaPublisher};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use serde_json::json;

fn bootstrap() -> String {
    std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "localhost:9092".to_owned())
}

/// Creates a fresh topic with a random name and waits for the create to be acknowledged.
async fn create_topic(partitions: i32) -> String {
    let topic = format!("test-events-{}", uuid::Uuid::new_v4());
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap())
        .create()
        .expect("admin client");
    let results = admin
        .create_topics(
            &[NewTopic::new(&topic, partitions, TopicReplication::Fixed(1))],
            &AdminOptions::new().request_timeout(Some(Duration::from_secs(20))),
        )
        .await
        .expect("create topics request");
    results[0].as_ref().expect("topic created");
    topic
}

fn unique_group() -> String {
    format!("test-group-{}", uuid::Uuid::new_v4())
}

fn envelope_in(stream: &StreamId, event_type: &str, n: u64) -> EventEnvelope {
    let meta = EventMeta::new(Utc::now(), CorrelationId::new());
    EventEnvelope {
        record: EventRecord::new(meta, event_type, json!({ "n": n })),
        stream: stream.clone(),
        stream_version: Version::new(n),
        position: Position::new("source", n),
        stored_at: Utc::now(),
    }
}

fn new_stream() -> StreamId {
    StreamId::new(AggregateId::new(), Category::new("Order").unwrap())
}

fn publisher(topic: &str) -> KafkaPublisher {
    KafkaPublisher::new(KafkaBusConfig::new(bootstrap(), topic)).unwrap()
}

fn consumer(topic: &str, group: &str) -> KafkaConsumer {
    KafkaConsumer::new(KafkaConsumerConfig::new(bootstrap(), topic, group)).unwrap()
}

fn batch(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).unwrap()
}

#[tokio::test]
async fn publish_one_envelope_is_received_with_same_id_type_and_payload() {
    let topic = create_topic(1).await;
    let sent = envelope_in(&new_stream(), "OrderPlaced", 1);
    publisher(&topic)
        .publish(std::slice::from_ref(&sent))
        .await
        .unwrap();

    let consumer = consumer(&topic, &unique_group());
    let got = consumer
        .poll_batch(batch(10), Duration::from_secs(20))
        .await
        .unwrap();

    assert_eq!(got.len(), 1);
    let received = &got[0].envelope;
    assert_eq!(received.record.meta.event_id, sent.record.meta.event_id);
    assert_eq!(received.record.event_type, "OrderPlaced");
    assert_eq!(received.record.payload, json!({ "n": 1 }));
    assert_eq!(received.stream, sent.stream);
}
