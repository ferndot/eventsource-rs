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
