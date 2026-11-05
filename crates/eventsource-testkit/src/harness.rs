//! A pre-wired in-memory store, bus and repository for one aggregate type.

use std::sync::Arc;

use eventsource_core::{
    AggregateId, AggregateRepository, AggregateStore, AppendResult, Category, CommandContext,
    Decider, EventMeta, EventRecord, EventRegistry, ExpectedVersion, RepositoryError, RetryPolicy,
    StreamId, SubscriptionConfig, SubscriptionRunner,
};
use eventsource_memory::{InMemoryCheckpointStore, InMemoryEventBus, InMemoryEventStore};

use crate::RecordingEventBus;

/// The bus a harness publishes to: a recorder in front of a real in-memory bus.
pub type HarnessBus = RecordingEventBus<Arc<InMemoryEventBus>>;

/// In-memory infrastructure for testing one aggregate type `D` end to end.
///
/// It owns an [`InMemoryEventStore`] that knows `D::Event`, an
/// [`AggregateRepository`] over it, and a [`RecordingEventBus`] wrapping an
/// [`InMemoryEventBus`]. The repository never publishes by itself, so after
/// every [`given_events`](Self::given_events) and
/// [`when_command`](Self::when_command) the harness delivers whatever is new in
/// the store's feed to the bus, the way a subscription runner would in production.
///
/// ```ignore
/// let harness = InMemoryTestHarness::<Counter>::new();
/// harness.given_events(id, [CounterEvent::Added(4)]).await;
/// harness.when_command(id, CounterCommand::Add(2)).await.unwrap();
/// harness.then_events_published([CounterEvent::Added(2)]);
/// ```
pub struct InMemoryTestHarness<D: Decider> {
    store: Arc<InMemoryEventStore>,
    bus: Arc<HarnessBus>,
    checkpoints: Arc<InMemoryCheckpointStore>,
    repository: AggregateRepository<D, Arc<InMemoryEventStore>>,
}

impl<D: Decider> Default for InMemoryTestHarness<D> {
    fn default() -> Self {
        Self::new()
    }
}

impl<D: Decider> InMemoryTestHarness<D> {
    /// A harness with an empty store and bus.
    ///
    /// # Panics
    /// If `D::AGGREGATE_TYPE` is not a valid stream category, or `D::Event`
    /// cannot be registered.
    #[must_use]
    pub fn new() -> Self {
        let mut registry = EventRegistry::new();
        registry
            .register::<D::Event>()
            .expect("event types are registrable");
        let store = Arc::new(InMemoryEventStore::new().with_registry(Arc::new(registry)));
        let repository = AggregateRepository::new(Arc::clone(&store))
            .expect("AGGREGATE_TYPE is a valid category");
        Self {
            store,
            bus: Arc::new(RecordingEventBus::wrapping(Arc::new(
                InMemoryEventBus::new(),
            ))),
            checkpoints: Arc::new(InMemoryCheckpointStore::new()),
            repository,
        }
    }

    /// The event store.
    #[must_use]
    pub fn store(&self) -> &InMemoryEventStore {
        &self.store
    }

    /// The repository, for loading aggregates or running commands directly.
    #[must_use]
    pub const fn repository(&self) -> &AggregateRepository<D, Arc<InMemoryEventStore>> {
        &self.repository
    }

    /// The recording bus. Subscribe handlers through its [`inner`](RecordingEventBus::inner) bus.
    #[must_use]
    pub fn bus(&self) -> &HarnessBus {
        &self.bus
    }

    /// Stores `events` as history of aggregate `id` without recording them as published.
    ///
    /// # Panics
    /// If the events cannot be serialized or stored, or `events` is empty.
    pub async fn given_events(&self, id: AggregateId, events: impl IntoIterator<Item = D::Event>) {
        let category = Category::new(D::AGGREGATE_TYPE).expect("valid category");
        let records: Vec<EventRecord> = events
            .into_iter()
            .map(|event| {
                let meta =
                    EventMeta::new(chrono::Utc::now(), eventsource_core::CorrelationId::new());
                EventRecord::from_event(&event, meta).expect("event serializes")
            })
            .collect();
        self.store
            .append(&StreamId::new(id, category), records, ExpectedVersion::Any)
            .await
            .expect("given events are stored");
        self.deliver().await;
        // History is not something the command under test published.
        self.bus.clear();
    }

    /// Runs `command` against aggregate `id` through the repository, then publishes its events.
    ///
    /// The result is returned rather than unwrapped, so rejection can be asserted on.
    ///
    /// # Errors
    /// Whatever [`AggregateRepository::execute`] returns.
    ///
    /// # Panics
    /// If delivering the stored events to the bus fails, for example because a handler subscribed on the bus failed.
    pub async fn when_command(
        &self,
        id: AggregateId,
        command: D::Command,
    ) -> Result<Option<AppendResult>, RepositoryError<D::Error>> {
        let result = self
            .repository
            .execute(id, command, &CommandContext::new())
            .await;
        self.deliver().await;
        result
    }

    /// Asserts the events published since the last [`given_events`](Self::given_events) are exactly `expected`, in order.
    ///
    /// # Panics
    /// If they differ, or a published record does not decode as `D::Event`.
    pub fn then_events_published(&self, expected: impl IntoIterator<Item = D::Event>)
    where
        D::Event: PartialEq,
    {
        let expected: Vec<D::Event> = expected.into_iter().collect();
        let actual: Vec<D::Event> = self
            .bus
            .published()
            .iter()
            .map(|envelope| {
                envelope
                    .record
                    .decode::<D::Event>()
                    .unwrap_or_else(|error| {
                        panic!(
                            "published record {} does not decode: {error}",
                            envelope.record.event_type
                        )
                    })
            })
            .collect();
        assert!(
            actual == expected,
            "expected published events {expected:?}, got {actual:?}"
        );
    }

    /// Pumps new events from the store's feed to the bus.
    async fn deliver(&self) {
        let mut config = SubscriptionConfig::new("testkit-harness");
        // Fail fast: a failing handler should surface now, not after backoff.
        config.retry = RetryPolicy {
            max_attempts: std::num::NonZeroU32::MIN,
            ..RetryPolicy::default()
        };
        SubscriptionRunner::new(
            Arc::clone(&self.store),
            Arc::clone(&self.checkpoints),
            Arc::clone(&self.bus),
            config,
        )
        .run_until_caught_up()
        .await
        .expect("events are delivered to the bus");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures::{Counter, CounterCommand, CounterError, CounterEvent};
    use eventsource_core::{AggregateId, RepositoryError};

    #[tokio::test]
    async fn given_events_then_command_stores_events_and_publishes_only_the_new_ones() {
        let harness = InMemoryTestHarness::<Counter>::new();
        let id = AggregateId::new();

        harness.given_events(id, [CounterEvent::Added(4)]).await;
        harness
            .when_command(id, CounterCommand::Add(2))
            .await
            .expect("accepted");

        // Stored: history plus the new event, folded by the real repository.
        let counter = harness
            .repository()
            .get(id)
            .await
            .expect("load")
            .expect("exists");
        assert_eq!(*counter.state(), 6);
        assert_eq!(
            harness
                .repository()
                .stored_version(id)
                .await
                .expect("version")
                .get(),
            2
        );
        // Published: only what the command produced, not the given history.
        harness.then_events_published([CounterEvent::Added(2)]);
    }

    #[tokio::test]
    async fn rejected_command_returns_the_domain_error_and_publishes_nothing() {
        let harness = InMemoryTestHarness::<Counter>::new();
        let id = AggregateId::new();
        harness.given_events(id, [CounterEvent::Added(9)]).await;

        let result = harness.when_command(id, CounterCommand::Add(5)).await;

        assert!(matches!(
            result,
            Err(RepositoryError::Domain(CounterError::Overflow(10)))
        ));
        harness.then_events_published([]);
    }

    #[tokio::test]
    #[should_panic(expected = "expected published events [Added(9)], got [Added(2)]")]
    async fn then_events_published_mismatch_names_expected_and_actual() {
        let harness = InMemoryTestHarness::<Counter>::new();
        let id = AggregateId::new();
        harness
            .when_command(id, CounterCommand::Add(2))
            .await
            .expect("accepted");
        harness.then_events_published([CounterEvent::Added(9)]);
    }
}
