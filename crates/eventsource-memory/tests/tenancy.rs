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

#[tokio::test]
async fn a_repository_scoped_to_one_tenant_cannot_load_another_tenants_aggregate() {
    let store: Store = Arc::new(InMemoryEventStore::new());
    let (a, b) = (TenantId::new(), TenantId::new());
    let id = AggregateId::new();
    repo(&store, b)
        .execute(id, Add(7), &CommandContext::new())
        .await
        .expect("b writes");

    let as_a = repo(&store, a);
    assert!(as_a.get(id).await.expect("not an error").is_none());
    assert!(matches!(
        as_a.load(id).await,
        Err(eventsource_core::error::RepositoryError::NotFound { .. })
    ));
    assert_eq!(as_a.stored_version(id).await.expect("version").get(), 0);
    assert_eq!(repo(&store, b).load(id).await.expect("b").state().sum, 7);
}

#[tokio::test]
async fn events_written_through_a_scope_carry_the_tenant_and_it_survives_a_read_back() {
    use eventsource_core::envelope::FeedReadOptions;
    use eventsource_core::store::GlobalFeed;

    let store: Store = Arc::new(InMemoryEventStore::new());
    let tenant = TenantId::new();
    let id = AggregateId::new();
    // The command context names no tenant: the scope supplies it.
    let ctx = CommandContext::new();
    assert!(ctx.tenant_id.is_none());

    let result = repo(&store, tenant)
        .execute(id, Add(1), &ctx)
        .await
        .expect("write")
        .expect("saved");
    assert_eq!(result.envelopes[0].record.meta.tenant_id, Some(tenant));

    let stored = store
        .read_all(None, FeedReadOptions::default())
        .await
        .expect("feed");
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].record.meta.tenant_id, Some(tenant));
}
