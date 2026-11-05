//! Tenant-scoped repositories over the in-memory store.

use std::sync::Arc;

use eventsource_core::{
    AggregateId, AggregateRepository, CommandContext, Decider, DomainEvent, TenantId,
};
use eventsource_memory::InMemoryEventStore;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
enum CounterEvent {
    Added(u64),
}

impl DomainEvent for CounterEvent {
    fn event_types() -> &'static [&'static str] {
        &["Added"]
    }

    fn event_type(&self) -> &'static str {
        "Added"
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct CounterState {
    sum: u64,
}

struct Add(u64);

#[derive(Debug, thiserror::Error)]
#[error("never rejected")]
struct Never;

struct Counter;

impl Decider for Counter {
    type Command = Add;
    type Event = CounterEvent;
    type State = CounterState;
    type Error = Never;

    const AGGREGATE_TYPE: &'static str = "Counter";

    fn initial_state() -> CounterState {
        CounterState { sum: 0 }
    }

    fn decide(_: &CounterState, Add(n): Add) -> Result<Vec<CounterEvent>, Never> {
        Ok(vec![CounterEvent::Added(n)])
    }

    fn evolve(state: &CounterState, event: &CounterEvent) -> CounterState {
        let CounterEvent::Added(n) = event;
        CounterState { sum: state.sum + n }
    }
}

type Store = Arc<InMemoryEventStore>;

fn repo(store: &Store, tenant: TenantId) -> AggregateRepository<Counter, Store> {
    AggregateRepository::new(Arc::clone(store))
        .expect("repository")
        .with_tenant(tenant)
        .expect("tenant category")
}

#[tokio::test]
async fn two_tenants_with_the_same_aggregate_id_get_independent_streams() {
    let store: Store = Arc::new(InMemoryEventStore::new());
    let (a, b) = (TenantId::new(), TenantId::new());
    let id = AggregateId::new();
    let ctx = CommandContext::new();

    repo(&store, a).execute(id, Add(1), &ctx).await.expect("a1");
    repo(&store, a).execute(id, Add(2), &ctx).await.expect("a2");
    repo(&store, b).execute(id, Add(10), &ctx).await.expect("b1");

    let seen_by_a = repo(&store, a).load(id).await.expect("a loads");
    let seen_by_b = repo(&store, b).load(id).await.expect("b loads");
    assert_eq!(seen_by_a.state().sum, 3);
    assert_eq!(seen_by_a.version().get(), 2);
    assert_eq!(seen_by_b.state().sum, 10);
    assert_eq!(seen_by_b.version().get(), 1);
}
