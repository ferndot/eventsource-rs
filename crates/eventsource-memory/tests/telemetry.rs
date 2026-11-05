//! Telemetry wiring on the in-memory adapters.

use eventsource_core::telemetry::NoopTracer;
use eventsource_memory::InMemoryEventStore;

#[tokio::test]
async fn noop_tracer_wired_into_the_store_changes_no_behaviour() {
    eventsource_testing::run_all(|| InMemoryEventStore::new().with_tracer(NoopTracer)).await;
}
