//! Tenant isolation in the `PostgreSQL` store survives reopening the schema.

mod common;

use std::sync::Arc;

use eventsource_core::{
    AggregateId, AggregateRepository, CommandContext, Decider, DomainEvent, TenantId,
};
use eventsource_postgres::PostgresEventStore;
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

type Store = Arc<PostgresEventStore>;

fn repo(store: &Store, tenant: TenantId) -> AggregateRepository<Counter, Store> {
    AggregateRepository::new(Arc::clone(store))
        .expect("repository")
        .with_tenant(tenant)
        .expect("tenant category")
}

#[tokio::test]
async fn tenant_isolation_holds_across_reopen() {
    use eventsource_core::envelope::FeedReadOptions;
    use eventsource_core::store::GlobalFeed;

    let schema = common::fresh_schema();
    let (a, b) = (TenantId::new(), TenantId::new());
    let id = AggregateId::new();
    let ctx = CommandContext::new();

    {
        let store: Store = Arc::new(PostgresEventStore::open(&common::database_url(), &schema)
                .await
                .expect("open"));
        repo(&store, a).execute(id, Add(1), &ctx).await.expect("a1");
        repo(&store, b)
            .execute(id, Add(10), &ctx)
            .await
            .expect("b1");
        repo(&store, a).execute(id, Add(2), &ctx).await.expect("a2");
    }

    let store: Store = Arc::new(PostgresEventStore::open(&common::database_url(), &schema)
            .await
            .expect("reopen"));
    let seen_by_a = repo(&store, a).load(id).await.expect("a loads");
    let seen_by_b = repo(&store, b).load(id).await.expect("b loads");
    assert_eq!((seen_by_a.state().sum, seen_by_a.version().get()), (3, 2));
    assert_eq!((seen_by_b.state().sum, seen_by_b.version().get()), (10, 1));
    assert!(
        repo(&store, TenantId::new())
            .get(id)
            .await
            .expect("get")
            .is_none()
    );

    let options = FeedReadOptions {
        tenant_id: Some(a),
        ..FeedReadOptions::default()
    };
    let feed = store.read_all(None, options).await.expect("feed");
    let tokens: Vec<u64> = feed.iter().map(|e| e.position.token()).collect();
    assert_eq!(tokens, vec![1, 3]);
    assert!(feed.iter().all(|e| e.record.meta.tenant_id == Some(a)));
}
