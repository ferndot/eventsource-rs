//! Tests against a real Redis. Set `REDIS_URL`; defaults to `redis://127.0.0.1:6390`.
#![allow(missing_docs)]

use chrono::Utc;
use eventsource_core::bus::EventPublisher;
use eventsource_core::envelope::EventEnvelope;
use eventsource_core::event::{EventMeta, EventRecord};
use eventsource_core::ids::{AggregateId, CorrelationId};
use eventsource_core::position::Position;
use eventsource_core::stream_id::{Category, StreamId};
use eventsource_core::version::Version;
use eventsource_redis::{RedisBusConfig, RedisPublisher};
use redis::AsyncCommands;
use redis::streams::StreamRangeReply;
use serde_json::json;

fn redis_url() -> String {
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6390".to_owned())
}

fn unique_stream() -> String {
    format!("test-events-{}", uuid::Uuid::new_v4())
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

async fn raw_entries(stream: &str) -> StreamRangeReply {
    let client = redis::Client::open(redis_url()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    conn.xrange_all(stream).await.unwrap()
}

#[tokio::test]
async fn publish_appends_envelope_to_configured_stream() {
    let stream = unique_stream();
    let publisher =
        RedisPublisher::new(&redis_url(), RedisBusConfig::new(stream.clone())).unwrap();
    let env = envelope("OrderPlaced", 1);

    publisher.publish(std::slice::from_ref(&env)).await.unwrap();

    let reply = raw_entries(&stream).await;
    assert_eq!(reply.ids.len(), 1);
    let entry = &reply.ids[0];
    let get = |k: &str| -> String { entry.get(k).unwrap() };
    assert_eq!(get("event_id"), env.record.meta.event_id.to_string());
    assert_eq!(get("event_type"), "OrderPlaced");
    let payload: serde_json::Value = serde_json::from_str(&get("payload")).unwrap();
    assert_eq!(payload, env.record.payload);
}
