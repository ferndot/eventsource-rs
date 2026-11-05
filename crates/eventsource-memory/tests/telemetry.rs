//! Telemetry wiring on the in-memory adapters.

use eventsource_core::telemetry::NoopTracer;
use eventsource_memory::InMemoryEventStore;

#[tokio::test]
async fn noop_tracer_wired_into_the_store_changes_no_behaviour() {
    eventsource_testing::run_all(|| InMemoryEventStore::new().with_tracer(NoopTracer)).await;
}

mod recording {
    use eventsource_core::store::AggregateStore;
    use eventsource_core::telemetry::{attr, span};
    use eventsource_core::{AggregateId, Category, ExpectedVersion, StreamId, StreamReadOptions};
    use eventsource_memory::InMemoryEventStore;
    use eventsource_testing::{RecordedValue, RecordingTracer, record};

    fn order_stream() -> StreamId {
        StreamId::new(
            AggregateId::new(),
            Category::new("Order").expect("category"),
        )
    }

    #[tokio::test]
    async fn append_and_load_spans_carry_stream_id_event_count_and_version() {
        let tracer = RecordingTracer::new();
        let store = InMemoryEventStore::new().with_tracer(tracer.clone());
        let stream = order_stream();

        store
            .append(
                &stream,
                vec![record("OrderPlaced"), record("OrderPaid")],
                ExpectedVersion::NoStream,
            )
            .await
            .expect("append");
        store
            .read_stream(&stream, StreamReadOptions::default())
            .await
            .expect("load");

        let append = tracer.only_span(span::APPEND);
        assert_eq!(
            append.get(attr::STREAM_ID),
            Some(&RecordedValue::Str(stream.to_string()))
        );
        assert_eq!(
            append.get(attr::AGGREGATE_TYPE),
            Some(&RecordedValue::Str("Order".into()))
        );
        assert_eq!(append.get(attr::EVENT_COUNT), Some(&RecordedValue::U64(2)));
        assert_eq!(append.get(attr::VERSION), Some(&RecordedValue::U64(2)));

        let load = tracer.only_span(span::LOAD);
        assert_eq!(
            load.get(attr::STREAM_ID),
            Some(&RecordedValue::Str(stream.to_string()))
        );
        assert_eq!(load.get(attr::VERSION), Some(&RecordedValue::U64(2)));
    }
}

mod subscription {
    use std::num::NonZeroU32;
    use std::sync::Arc;
    use std::time::Duration;

    use eventsource_core::bus::handler;
    use eventsource_core::error::BackendError;
    use eventsource_core::store::AggregateStore;
    use eventsource_core::telemetry::{attr, span};
    use eventsource_core::{
        AggregateId, Category, EventEnvelope, ExpectedVersion, RetryPolicy, StreamId,
        SubscriptionConfig, SubscriptionRunner,
    };
    use eventsource_memory::{InMemoryCheckpointStore, InMemoryEventBus, InMemoryEventStore};
    use eventsource_testing::{RecordedValue, RecordingTracer, record};

    async fn store_with_events(count: usize) -> Arc<InMemoryEventStore> {
        let store = Arc::new(InMemoryEventStore::new());
        for _ in 0..count {
            let stream = StreamId::new(
                AggregateId::new(),
                Category::new("Order").expect("category"),
            );
            store
                .append(
                    &stream,
                    vec![record("OrderPlaced")],
                    ExpectedVersion::NoStream,
                )
                .await
                .expect("append");
        }
        store
    }

    fn config(attempts: u32) -> SubscriptionConfig {
        let mut config = SubscriptionConfig::new("order-summary");
        config.retry = RetryPolicy {
            max_attempts: NonZeroU32::new(attempts).expect("nonzero"),
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(1),
        };
        config
    }

    #[tokio::test]
    async fn page_span_has_subscription_name_page_size_and_zero_failures_on_success() {
        let tracer = RecordingTracer::new();
        let store = store_with_events(3).await;
        let bus = Arc::new(InMemoryEventBus::new());
        bus.subscribe_all(handler(|_: EventEnvelope| async {
            Ok::<(), BackendError>(())
        }));
        let mut runner = SubscriptionRunner::new(
            Arc::clone(&store),
            InMemoryCheckpointStore::new(),
            bus,
            config(3),
        )
        .with_tracer(tracer.clone());

        runner.run_until_caught_up().await.expect("caught up");

        let page = tracer.only_span(span::SUBSCRIPTION_PAGE);
        assert_eq!(
            page.get(attr::SUBSCRIPTION_NAME),
            Some(&RecordedValue::Str("order-summary".into()))
        );
        assert_eq!(page.get(attr::BATCH_SIZE), Some(&RecordedValue::U64(3)));
        assert_eq!(page.get(attr::FAILURE_COUNT), Some(&RecordedValue::U64(0)));
        assert_eq!(page.failure, None);
    }

    #[tokio::test]
    async fn page_span_records_failure_count_when_a_handler_fails() {
        let tracer = RecordingTracer::new();
        let store = store_with_events(2).await;
        let bus = Arc::new(InMemoryEventBus::new());
        bus.subscribe_all(handler(|_: EventEnvelope| async {
            Err::<(), BackendError>("boom".into())
        }));
        let mut runner = SubscriptionRunner::new(
            Arc::clone(&store),
            InMemoryCheckpointStore::new(),
            bus,
            config(3),
        )
        .with_tracer(tracer.clone());

        runner
            .run_until_caught_up()
            .await
            .expect_err("handler fails");

        let page = tracer.only_span(span::SUBSCRIPTION_PAGE);
        assert_eq!(page.get(attr::BATCH_SIZE), Some(&RecordedValue::U64(2)));
        assert_eq!(page.get(attr::FAILURE_COUNT), Some(&RecordedValue::U64(3)));
        assert!(
            page.failure.is_some(),
            "a page that exhausts retries is failed"
        );
    }
}

mod publish {
    use eventsource_core::EventPublisher;
    use eventsource_core::bus::handler;
    use eventsource_core::error::BackendError;
    use eventsource_core::store::AggregateStore;
    use eventsource_core::telemetry::{attr, span};
    use eventsource_core::{AggregateId, Category, EventEnvelope, ExpectedVersion, StreamId};
    use eventsource_memory::{InMemoryEventBus, InMemoryEventStore};
    use eventsource_testing::{RecordedValue, RecordingTracer, record};

    #[tokio::test]
    async fn publish_span_has_event_count_and_handler_failure_count() {
        let tracer = RecordingTracer::new();
        let bus = InMemoryEventBus::new().with_tracer(tracer.clone());
        bus.subscribe_all(handler(|_: EventEnvelope| async {
            Err::<(), BackendError>("boom".into())
        }));
        let stream = StreamId::new(
            AggregateId::new(),
            Category::new("Order").expect("category"),
        );
        let appended = InMemoryEventStore::new()
            .append(
                &stream,
                vec![record("OrderPlaced"), record("OrderPaid")],
                ExpectedVersion::NoStream,
            )
            .await
            .expect("append");

        bus.publish(&appended.envelopes)
            .await
            .expect_err("handler fails");

        let publish = tracer.only_span(span::PUBLISH);
        assert_eq!(publish.get(attr::EVENT_COUNT), Some(&RecordedValue::U64(2)));
        assert_eq!(
            publish.get(attr::FAILURE_COUNT),
            Some(&RecordedValue::U64(2))
        );
        assert!(publish.failure.is_some());
    }
}

mod privacy {
    use std::sync::Arc;

    use eventsource_core::bus::handler;
    use eventsource_core::error::BackendError;
    use eventsource_core::store::AggregateStore;
    use eventsource_core::{
        AggregateId, Category, EventEnvelope, ExpectedVersion, StreamId, StreamReadOptions,
        SubscriptionConfig, SubscriptionRunner,
    };
    use eventsource_memory::{InMemoryCheckpointStore, InMemoryEventBus, InMemoryEventStore};
    use eventsource_testing::{RecordingTracer, record};

    const SENTINEL: &str = "SENTINEL-4f9c1e-do-not-trace";

    #[tokio::test]
    async fn recorded_attributes_never_include_event_payloads() {
        let tracer = RecordingTracer::new();
        let store = Arc::new(InMemoryEventStore::new().with_tracer(tracer.clone()));
        let bus = Arc::new(InMemoryEventBus::new().with_tracer(tracer.clone()));
        // The handler's own error text is payload-derived, so it must not leak either.
        bus.subscribe_all(handler(|envelope: EventEnvelope| async move {
            Err::<(), BackendError>(format!("bad payload {}", envelope.record.payload).into())
        }));

        let mut event = record("OrderPlaced");
        event.payload = serde_json::json!({ "card": SENTINEL });
        event.meta.actor_id = Some(SENTINEL.to_owned());
        let stream = StreamId::new(
            AggregateId::new(),
            Category::new("Order").expect("category"),
        );
        store
            .append(&stream, vec![event], ExpectedVersion::NoStream)
            .await
            .expect("append");
        store
            .read_stream(&stream, StreamReadOptions::default())
            .await
            .expect("load");
        let mut config = SubscriptionConfig::new("payments");
        config.retry.max_attempts = std::num::NonZeroU32::MIN;
        let mut runner = SubscriptionRunner::new(
            Arc::clone(&store),
            InMemoryCheckpointStore::new(),
            bus,
            config,
        )
        .with_tracer(tracer.clone());
        runner
            .run_until_caught_up()
            .await
            .expect_err("handler fails");

        let spans = tracer.spans();
        assert!(
            spans.len() >= 4,
            "append, load, publish and page spans: {spans:?}"
        );
        let everything = format!("{spans:?}");
        assert!(
            !everything.contains(SENTINEL),
            "payload leaked into telemetry: {everything}"
        );
    }
}
