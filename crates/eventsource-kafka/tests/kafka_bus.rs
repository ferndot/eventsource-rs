//! Tests against a real Kafka broker. Set `KAFKA_BOOTSTRAP`; defaults to `localhost:9092`.
#![allow(missing_docs)]

use std::num::NonZeroUsize;
use std::time::Duration;

use chrono::Utc;
use eventsource_core::bus::EventPublisher;
use eventsource_core::envelope::EventEnvelope;
use eventsource_core::error::BusError;
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
            &[NewTopic::new(
                &topic,
                partitions,
                TopicReplication::Fixed(1),
            )],
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
    KafkaConsumer::new(&KafkaConsumerConfig::new(bootstrap(), topic, group)).unwrap()
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

/// Polls until `count` deliveries arrived, failing the test if they do not within 30 seconds.
async fn collect(consumer: &KafkaConsumer, count: usize) -> Vec<eventsource_kafka::Delivery> {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut all = Vec::new();
    while all.len() < count {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out with {} of {count} records",
            all.len()
        );
        all.extend(
            consumer
                .poll_batch(batch(100), Duration::from_secs(2))
                .await
                .unwrap(),
        );
    }
    all
}

#[tokio::test]
async fn same_stream_key_is_read_back_in_publish_order() {
    let topic = create_topic(3).await;
    let stream = new_stream();
    let sent: Vec<_> = (1..=20)
        .map(|n| envelope_in(&stream, "OrderPlaced", n))
        .collect();
    publisher(&topic).publish(&sent).await.unwrap();

    let got = collect(&consumer(&topic, &unique_group()), sent.len()).await;

    let ids: Vec<_> = got
        .iter()
        .map(|d| d.envelope.record.meta.event_id)
        .collect();
    let expected: Vec<_> = sent.iter().map(|e| e.record.meta.event_id).collect();
    assert_eq!(ids, expected);
}

#[tokio::test]
async fn different_stream_keys_keep_order_within_each_key() {
    let topic = create_topic(3).await;
    let streams: Vec<_> = (0..4).map(|_| new_stream()).collect();
    // Interleave the streams: s0#1, s1#1, s2#1, s3#1, s0#2, ...
    let sent: Vec<_> = (1..=10)
        .flat_map(|n| {
            streams
                .iter()
                .map(move |s| envelope_in(s, "OrderPlaced", n))
        })
        .collect();
    publisher(&topic).publish(&sent).await.unwrap();

    let got = collect(&consumer(&topic, &unique_group()), sent.len()).await;

    for stream in &streams {
        let mine: Vec<_> = got
            .iter()
            .filter(|d| &d.envelope.stream == stream)
            .collect();
        let versions: Vec<u64> = mine
            .iter()
            .map(|d| d.envelope.stream_version.get())
            .collect();
        assert_eq!(
            versions,
            (1..=10).collect::<Vec<_>>(),
            "order within {stream}"
        );
        // One key maps to one partition, which is what makes the order meaningful.
        assert!(mine.iter().all(|d| d.partition == mine[0].partition));
    }
}

#[tokio::test]
async fn uncommitted_records_are_redelivered_to_a_restarted_consumer() {
    let topic = create_topic(1).await;
    let group = unique_group();
    let sent: Vec<_> = (1..=3)
        .map(|n| envelope_in(&new_stream(), "OrderPlaced", n))
        .collect();
    publisher(&topic).publish(&sent).await.unwrap();
    let expected: Vec<_> = sent.iter().map(|e| e.record.meta.event_id).collect();

    let first = consumer(&topic, &group);
    let seen = collect(&first, sent.len()).await;
    assert_eq!(seen.len(), 3);
    drop(first); // "crashes" without committing

    let second = consumer(&topic, &group);
    let again = collect(&second, sent.len()).await;
    let ids: Vec<_> = again
        .iter()
        .map(|d| d.envelope.record.meta.event_id)
        .collect();
    assert_eq!(ids, expected);
}

#[tokio::test]
async fn committed_records_are_not_redelivered_but_later_ones_are() {
    let topic = create_topic(1).await;
    let group = unique_group();
    let sent: Vec<_> = (1..=3)
        .map(|n| envelope_in(&new_stream(), "OrderPlaced", n))
        .collect();
    publisher(&topic).publish(&sent).await.unwrap();

    let first = consumer(&topic, &group);
    let seen = collect(&first, sent.len()).await;
    first.commit(&seen[..2]).await.unwrap(); // handled the first two only
    drop(first);

    let second = consumer(&topic, &group);
    let again = collect(&second, 1).await;
    assert_eq!(again.len(), 1);
    assert_eq!(
        again[0].envelope.record.meta.event_id,
        sent[2].record.meta.event_id
    );
    let more = second
        .poll_batch(batch(10), Duration::from_secs(2))
        .await
        .unwrap();
    assert!(
        more.is_empty(),
        "committed records came back: {}",
        more.len()
    );
}

#[tokio::test]
async fn publish_to_unreachable_broker_fails_with_backend_error_in_bounded_time() {
    // Nothing listens on port 1, so the producer can never reach a broker.
    let mut config = KafkaBusConfig::new("127.0.0.1:1", "never-created");
    config.message_timeout = Duration::from_secs(2);
    let publisher = KafkaPublisher::new(config).unwrap();
    let sent = envelope_in(&new_stream(), "OrderPlaced", 1);

    let started = std::time::Instant::now();
    let outcome = tokio::time::timeout(
        Duration::from_secs(15),
        publisher.publish(std::slice::from_ref(&sent)),
    )
    .await
    .expect("publish hung past the bound instead of failing");

    assert!(
        matches!(outcome, Err(BusError::Backend(_))),
        "expected BusError::Backend, got {outcome:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "took {:?}",
        started.elapsed()
    );
}
