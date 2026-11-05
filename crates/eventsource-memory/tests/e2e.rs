//! End-to-end: decider, repository, bus, and checkpoints over in-memory adapters.

use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use eventsource_core::bus::{EventPublisher, handler};
use eventsource_core::checkpoint::{Checkpoint, CheckpointStore};
use eventsource_core::envelope::{AppendResult, FeedReadOptions, StreamReadOptions};
use eventsource_core::error::{
    BackendError, BusError, DecodeError, RepositoryError, StoreError, SubscriptionError,
};
use eventsource_core::event::{EventMeta, EventRecord};
use eventsource_core::live::LiveFeed;
use eventsource_core::store::{AggregateStore, GlobalFeed};
use eventsource_core::{
    AggregateId, AggregateRepository, Category, CommandContext, CorrelationId, Decider,
    DomainEvent, EventEnvelope, EventRegistry, ExpectedVersion, Position, RetryPolicy, RunStats,
    StartFrom, StreamId, SubscriptionConfig, SubscriptionRunner, Version,
};
use eventsource_memory::{InMemoryCheckpointStore, InMemoryEventBus, InMemoryEventStore};

// ---- Domain: an order that can be placed once and then shipped. ----

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
enum OrderEvent {
    OrderPlaced { total: u64 },
    OrderShipped { tracking: String },
}

impl DomainEvent for OrderEvent {
    fn event_types() -> &'static [&'static str] {
        &["OrderPlaced", "OrderShipped"]
    }

    fn event_type(&self) -> &'static str {
        match self {
            Self::OrderPlaced { .. } => "OrderPlaced",
            Self::OrderShipped { .. } => "OrderShipped",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum OrderState {
    Draft,
    Placed { total: u64 },
    Shipped { total: u64, tracking: String },
}

enum OrderCommand {
    Place { total: u64 },
    Ship { tracking: String },
}

#[derive(Debug, PartialEq, thiserror::Error)]
enum OrderError {
    #[error("order already placed")]
    AlreadyPlaced,
    #[error("order must be placed before shipping")]
    NotPlaced,
}

struct Order;

impl Decider for Order {
    type Command = OrderCommand;
    type Event = OrderEvent;
    type State = OrderState;
    type Error = OrderError;

    const AGGREGATE_TYPE: &'static str = "Order";

    fn initial_state() -> OrderState {
        OrderState::Draft
    }

    fn decide(state: &OrderState, command: OrderCommand) -> Result<Vec<OrderEvent>, OrderError> {
        match (state, command) {
            (OrderState::Draft, OrderCommand::Place { total }) => {
                Ok(vec![OrderEvent::OrderPlaced { total }])
            }
            (_, OrderCommand::Place { .. }) => Err(OrderError::AlreadyPlaced),
            (OrderState::Placed { .. }, OrderCommand::Ship { tracking }) => {
                Ok(vec![OrderEvent::OrderShipped { tracking }])
            }
            (_, OrderCommand::Ship { .. }) => Err(OrderError::NotPlaced),
        }
    }

    fn evolve(state: &OrderState, event: &OrderEvent) -> OrderState {
        match (state, event) {
            (OrderState::Draft, OrderEvent::OrderPlaced { total }) => {
                OrderState::Placed { total: *total }
            }
            (OrderState::Placed { total }, OrderEvent::OrderShipped { tracking }) => {
                OrderState::Shipped {
                    total: *total,
                    tracking: tracking.clone(),
                }
            }
            (other, _) => other.clone(),
        }
    }
}

fn registry() -> Arc<EventRegistry> {
    let mut registry = EventRegistry::new();
    registry.register::<OrderEvent>().expect("register");
    Arc::new(registry)
}

fn repo() -> AggregateRepository<Order, InMemoryEventStore> {
    let store = InMemoryEventStore::new().with_registry(registry());
    AggregateRepository::new(store).expect("valid category")
}

// ---- Tests ----

#[tokio::test]
async fn execute_then_load_rebuilds_state_from_events() {
    let repo = repo();
    let id = AggregateId::new();
    let ctx = CommandContext::new();

    repo.execute(id, OrderCommand::Place { total: 42 }, &ctx)
        .await
        .expect("place");
    repo.execute(
        id,
        OrderCommand::Ship {
            tracking: "T-1".into(),
        },
        &ctx,
    )
    .await
    .expect("ship");

    let order = repo.load(id).await.expect("load");
    assert_eq!(
        order.state(),
        &OrderState::Shipped {
            total: 42,
            tracking: "T-1".into()
        }
    );
    assert_eq!(order.version().get(), 2);
}

#[tokio::test]
async fn rejected_command_stores_nothing() {
    let repo = repo();
    let id = AggregateId::new();

    let result = repo
        .execute(
            id,
            OrderCommand::Ship {
                tracking: "T".into(),
            },
            &CommandContext::new(),
        )
        .await;
    assert!(matches!(
        result,
        Err(RepositoryError::Domain(OrderError::NotPlaced))
    ));
    assert_eq!(repo.stored_version(id).await.expect("version").get(), 0);
}

#[tokio::test]
async fn stale_copy_is_rejected_instead_of_overwriting() {
    let repo = repo();
    let id = AggregateId::new();
    let ctx = CommandContext::new();
    let mut stamp = test_stamp();
    repo.execute(id, OrderCommand::Place { total: 1 }, &ctx)
        .await
        .expect("place");

    let mut first = repo.load(id).await.expect("first copy");
    let mut second = repo.load(id).await.expect("second copy");

    first
        .execute(
            OrderCommand::Ship {
                tracking: "A".into(),
            },
            &mut stamp,
        )
        .expect("ship first");
    repo.save(&mut first).await.expect("first save wins");

    second
        .execute(
            OrderCommand::Ship {
                tracking: "B".into(),
            },
            &mut stamp,
        )
        .expect("ship second");
    let result = repo.save(&mut second).await;
    assert!(matches!(
        result,
        Err(RepositoryError::Store(
            StoreError::ConcurrencyConflict { .. }
        ))
    ));
}

#[tokio::test]
async fn retry_recovers_from_a_lost_race() {
    let repo = repo();
    let id = AggregateId::new();
    let ctx = CommandContext::new();
    let mut stamp = test_stamp();
    repo.execute(id, OrderCommand::Place { total: 1 }, &ctx)
        .await
        .expect("place");

    // Another writer ships the order between our load and save; the retry reloads and
    // then sees the order already shipped, so the second Ship is rejected by the domain.
    let mut stale = repo.load(id).await.expect("load");
    stale
        .execute(
            OrderCommand::Ship {
                tracking: "X".into(),
            },
            &mut stamp,
        )
        .expect("ship");
    repo.execute(
        id,
        OrderCommand::Ship {
            tracking: "Y".into(),
        },
        &ctx,
    )
    .await
    .expect("winner ships");
    assert!(matches!(
        repo.save(&mut stale).await,
        Err(RepositoryError::Store(
            StoreError::ConcurrencyConflict { .. }
        ))
    ));

    // The retry reloads, sees the order already shipped, and the domain rejects the
    // command. Domain rejections are not conflicts, so the command runs exactly once.
    let attempts = std::num::NonZeroUsize::new(3).expect("nonzero");
    let mut calls = 0;
    let result = repo
        .execute_with_retry(
            id,
            || {
                calls += 1;
                OrderCommand::Ship {
                    tracking: "Z".into(),
                }
            },
            &ctx,
            attempts,
        )
        .await;
    assert!(matches!(
        result,
        Err(RepositoryError::Domain(OrderError::NotPlaced))
    ));
    assert_eq!(calls, 1, "domain rejections are not retried");
}

#[tokio::test]
async fn retry_succeeds_after_losing_a_race() {
    let inner = Arc::new(InMemoryEventStore::new().with_registry(registry()));
    let id = AggregateId::new();
    let stream = StreamId::new(id, Category::new("Order").expect("category"));
    let rival = EventRecord::from_event(
        &OrderEvent::OrderPlaced { total: 7 },
        EventMeta::new(chrono::Utc::now(), CorrelationId::new()),
    )
    .expect("json");
    let store = RacingStore {
        inner: Arc::clone(&inner),
        rival: Mutex::new(Some((stream, rival))),
    };
    let repo = AggregateRepository::<Order, _>::new(store).expect("category");

    // The first attempt loses to a rival placement. The retry reloads, so shipping is valid.
    let mut calls = 0;
    let result = repo
        .execute_with_retry(
            id,
            || {
                calls += 1;
                if calls == 1 {
                    OrderCommand::Place { total: 1 }
                } else {
                    OrderCommand::Ship {
                        tracking: "T".into(),
                    }
                }
            },
            &CommandContext::new(),
            NonZeroUsize::new(3).expect("nonzero"),
        )
        .await;
    result.expect("second attempt succeeds");
    assert_eq!(calls, 2);
    assert_eq!(repo.stored_version(id).await.expect("version").get(), 2);
}

#[tokio::test]
async fn save_returns_the_envelopes_the_feed_later_serves() {
    let (store, repo) = store_and_repo();
    let result = repo
        .execute(
            AggregateId::new(),
            OrderCommand::Place { total: 5 },
            &CommandContext::new(),
        )
        .await
        .expect("place")
        .expect("events were saved");
    let feed = store
        .read_all(None, FeedReadOptions::default())
        .await
        .expect("feed");
    assert_eq!(result.envelopes, feed);
}

#[tokio::test]
async fn runner_delivers_in_feed_order_and_checkpoints() {
    let (store, repo) = store_and_repo();
    place_orders(&repo, 3).await;
    let checkpoints = Arc::new(InMemoryCheckpointStore::new());
    let (bus, seen) = recording_bus();
    let mut runner = runner_for(
        &store,
        &checkpoints,
        &bus,
        SubscriptionConfig::new("orders"),
    );

    let stats = runner.run_until_caught_up().await.expect("run");

    assert_eq!(*seen.lock().expect("lock"), [1, 2, 3]);
    assert_eq!(
        stats,
        RunStats {
            events_delivered: 3,
            batches: 1
        }
    );
    let saved = checkpoints
        .load("orders")
        .await
        .expect("load")
        .expect("saved");
    assert_eq!(saved.position.map(|p| p.token()), Some(3));
    assert_eq!(saved.events_processed, 3);
}

#[tokio::test]
async fn runner_resumes_from_its_checkpoint() {
    let (store, repo) = store_and_repo();
    place_orders(&repo, 2).await;
    let checkpoints = Arc::new(InMemoryCheckpointStore::new());
    let (bus, _) = recording_bus();
    let mut first = runner_for(
        &store,
        &checkpoints,
        &bus,
        SubscriptionConfig::new("orders"),
    );
    first.run_until_caught_up().await.expect("first run");

    place_orders(&repo, 1).await;
    let (bus, seen) = recording_bus();
    let mut second = runner_for(
        &store,
        &checkpoints,
        &bus,
        SubscriptionConfig::new("orders"),
    );
    second.run_until_caught_up().await.expect("second run");

    assert_eq!(*seen.lock().expect("lock"), [3], "only the new event");
}

#[tokio::test]
async fn runner_pages_by_batch_size() {
    let (store, repo) = store_and_repo();
    place_orders(&repo, 5).await;
    let checkpoints = Arc::new(InMemoryCheckpointStore::new());
    let (bus, seen) = recording_bus();
    let mut config = SubscriptionConfig::new("orders");
    config.batch_size = NonZeroUsize::new(2).expect("nonzero");
    let mut runner = runner_for(&store, &checkpoints, &bus, config);

    let stats = runner.run_until_caught_up().await.expect("run");

    assert_eq!(*seen.lock().expect("lock"), [1, 2, 3, 4, 5]);
    assert_eq!(stats.batches, 3);
}

#[tokio::test]
async fn failed_page_is_not_checkpointed_and_is_redelivered_after_restart() {
    let (store, repo) = store_and_repo();
    place_orders(&repo, 1).await;
    let checkpoints = Arc::new(InMemoryCheckpointStore::new());
    let failing = Arc::new(InMemoryEventBus::new());
    failing.subscribe_all(handler(|_e: EventEnvelope| async {
        Err::<(), BackendError>("downstream is down".into())
    }));
    let mut config = SubscriptionConfig::new("orders");
    config.retry = fast_retry(2);
    let mut runner = runner_for(&store, &checkpoints, &failing, config.clone());

    let result = runner.run_until_caught_up().await;
    assert!(matches!(
        result,
        Err(SubscriptionError::Handler { attempts: 2, .. })
    ));
    assert_eq!(checkpoints.load("orders").await.expect("load"), None);

    let (bus, seen) = recording_bus();
    let mut restarted = runner_for(&store, &checkpoints, &bus, config);
    restarted.run_until_caught_up().await.expect("restart");
    assert_eq!(*seen.lock().expect("lock"), [1]);
}

#[tokio::test]
async fn transient_failure_is_retried_and_the_page_is_delivered() {
    let (store, repo) = store_and_repo();
    place_orders(&repo, 1).await;
    let checkpoints = Arc::new(InMemoryCheckpointStore::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let bus = Arc::new(InMemoryEventBus::new());
    let counter = Arc::clone(&calls);
    bus.subscribe_all(handler(move |_e: EventEnvelope| {
        let counter = Arc::clone(&counter);
        async move {
            if counter.fetch_add(1, Ordering::SeqCst) == 0 {
                Err::<(), BackendError>("flaky".into())
            } else {
                Ok(())
            }
        }
    }));
    let mut config = SubscriptionConfig::new("orders");
    config.retry = fast_retry(3);
    let mut runner = runner_for(&store, &checkpoints, &bus, config);

    let stats = runner.run_until_caught_up().await.expect("retried");

    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(stats.events_delivered, 1);
}

#[tokio::test]
async fn start_from_latest_skips_existing_history() {
    let (store, repo) = store_and_repo();
    place_orders(&repo, 2).await;
    let checkpoints = Arc::new(InMemoryCheckpointStore::new());
    let (bus, seen) = recording_bus();
    let mut config = SubscriptionConfig::new("orders");
    config.start_from = StartFrom::Latest;
    let mut runner = runner_for(&store, &checkpoints, &bus, config.clone());
    runner.run_until_caught_up().await.expect("skip history");
    assert!(seen.lock().expect("lock").is_empty());

    place_orders(&repo, 1).await;
    let mut resumed = runner_for(&store, &checkpoints, &bus, config);
    resumed.run_until_caught_up().await.expect("new event");
    assert_eq!(*seen.lock().expect("lock"), [3]);
}

#[tokio::test]
async fn shutdown_stops_an_idle_runner() {
    let (store, _repo) = store_and_repo();
    let checkpoints = Arc::new(InMemoryCheckpointStore::new());
    let (bus, _) = recording_bus();
    let mut config = SubscriptionConfig::new("orders");
    config.poll_interval = Duration::from_secs(3600);
    let runner = runner_for(&store, &checkpoints, &bus, config);

    let shutdown = tokio::time::sleep(Duration::from_millis(20));
    let stats = tokio::time::timeout(Duration::from_secs(5), runner.run(shutdown))
        .await
        .expect("stopped in time")
        .expect("run");
    assert_eq!(stats, RunStats::default());
}

#[tokio::test]
async fn wake_signal_delivers_before_the_poll_interval() {
    let (store, repo) = store_and_repo();
    let checkpoints = Arc::new(InMemoryCheckpointStore::new());
    let (bus, seen) = recording_bus();
    let mut config = SubscriptionConfig::new("orders");
    config.poll_interval = Duration::from_secs(3600);
    let runner = runner_for(&store, &checkpoints, &bus, config).with_wake(store.wake());
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let task = tokio::spawn(runner.run(async {
        let _ = stopped.await;
    }));

    place_orders(&repo, 1).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        while seen.lock().expect("lock").is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("delivered without waiting for the poll interval");

    let _ = stop.send(());
    task.await.expect("join").expect("run");
    assert_eq!(*seen.lock().expect("lock"), [1]);
}

#[tokio::test]
async fn slow_subscriber_does_not_delay_a_save() {
    let (store, repo) = store_and_repo();
    let gate = Arc::new(tokio::sync::Notify::new());
    let bus = Arc::new(InMemoryEventBus::new());
    let waiter = Arc::clone(&gate);
    bus.subscribe_all(handler(move |_e: EventEnvelope| {
        let waiter = Arc::clone(&waiter);
        async move {
            waiter.notified().await;
            Ok::<(), BackendError>(())
        }
    }));
    let checkpoints = Arc::new(InMemoryCheckpointStore::new());
    let mut runner = runner_for(&store, &checkpoints, &bus, SubscriptionConfig::new("slow"));
    let subscriber = tokio::spawn(async move { runner.run_until_caught_up().await });

    let saved = tokio::time::timeout(
        Duration::from_secs(1),
        repo.execute(
            AggregateId::new(),
            OrderCommand::Place { total: 1 },
            &CommandContext::new(),
        ),
    )
    .await
    .expect("save is not blocked by the subscriber")
    .expect("place");
    assert!(saved.is_some());

    gate.notify_one();
    subscriber.await.expect("join").expect("run");
}

#[tokio::test]
async fn bus_runs_every_handler_and_specific_before_catch_all() {
    let order: Arc<Mutex<Vec<&'static str>>> = Arc::default();
    let bus = InMemoryEventBus::new();

    let log = Arc::clone(&order);
    bus.subscribe_all(handler(move |_e: EventEnvelope| {
        let log = Arc::clone(&log);
        async move {
            log.lock().expect("lock").push("catch-all");
            Ok::<(), BackendError>(())
        }
    }));
    let log = Arc::clone(&order);
    bus.subscribe(
        "OrderPlaced",
        handler(move |_e: EventEnvelope| {
            let log = Arc::clone(&log);
            async move {
                log.lock().expect("lock").push("specific-failing");
                Err::<(), BackendError>("boom".into())
            }
        }),
    );
    let log = Arc::clone(&order);
    bus.subscribe(
        "OrderPlaced",
        handler(move |_e: EventEnvelope| {
            let log = Arc::clone(&log);
            async move {
                log.lock().expect("lock").push("specific-after");
                Ok::<(), BackendError>(())
            }
        }),
    );

    let envelope = envelope_for(&OrderEvent::OrderPlaced { total: 1 });
    let result = bus.publish(std::slice::from_ref(&envelope)).await;
    assert!(matches!(result, Err(BusError::HandlerFailures(ref f)) if f.len() == 1));
    assert_eq!(
        *order.lock().expect("lock"),
        ["specific-failing", "specific-after", "catch-all"]
    );
}

#[tokio::test]
async fn checkpoints_round_trip() {
    let store = InMemoryCheckpointStore::new();
    assert_eq!(store.load("projection").await.expect("load"), None);

    let checkpoint = Checkpoint {
        position: Some(Position::new("memory", 3)),
        last_event_id: None,
        events_processed: 3,
    };
    store
        .save("projection", checkpoint.clone())
        .await
        .expect("save");
    assert_eq!(
        store.load("projection").await.expect("load"),
        Some(checkpoint)
    );
}

#[tokio::test]
async fn panicking_handler_does_not_stop_later_handlers() {
    let reached = Arc::new(Mutex::new(false));
    let bus = InMemoryEventBus::new();
    bus.subscribe_all(handler(|_e: EventEnvelope| async { explode() }));
    let flag = Arc::clone(&reached);
    bus.subscribe_all(handler(move |_e: EventEnvelope| {
        let flag = Arc::clone(&flag);
        async move {
            *flag.lock().expect("lock") = true;
            Ok::<(), BackendError>(())
        }
    }));

    let envelope = envelope_for(&OrderEvent::OrderPlaced { total: 1 });
    let result = bus.publish(std::slice::from_ref(&envelope)).await;
    assert!(matches!(result, Err(BusError::HandlerFailures(ref f)) if f.len() == 1));
    assert!(
        *reached.lock().expect("lock"),
        "later handler must still run"
    );
}

#[tokio::test]
async fn stored_type_mismatch_is_reported_on_load() {
    let store = Arc::new(InMemoryEventStore::new().with_registry(registry()));
    let repo = AggregateRepository::<Order, _>::new(Arc::clone(&store)).expect("category");
    let id = AggregateId::new();
    // Stored under the wrong name: the payload is OrderPlaced but the type says OrderShipped.
    let payload = serde_json::to_value(OrderEvent::OrderPlaced { total: 5 }).expect("json");
    let meta = EventMeta::new(chrono::Utc::now(), CorrelationId::new());
    let record = EventRecord::new(meta, "OrderShipped", payload);
    let stream = StreamId::new(id, Category::new("Order").expect("category"));
    store
        .append(&stream, vec![record], ExpectedVersion::Any)
        .await
        .expect("raw append");

    let result = repo.load(id).await;
    assert!(matches!(
        result,
        Err(RepositoryError::Decode(
            DecodeError::EventTypeMismatch { .. }
        ))
    ));
}

#[tokio::test]
async fn pending_event_ids_are_stable_across_a_failed_save() {
    let store = Arc::new(InMemoryEventStore::new().with_registry(registry()));
    let repo = AggregateRepository::<Order, _>::new(Arc::clone(&store)).expect("category");
    let id = AggregateId::new();
    let mut stamp = test_stamp();

    let mut aggregate = repo.load_or_new(id).await.expect("new");
    aggregate
        .execute(OrderCommand::Place { total: 3 }, &mut stamp)
        .expect("place");
    let before = aggregate.uncommitted_events()[0].meta.event_id;

    // Another writer lands first, so the save conflicts.
    let mut racer = repo.load_or_new(id).await.expect("racer");
    racer
        .execute(OrderCommand::Place { total: 4 }, &mut stamp)
        .expect("racer place");
    repo.save(&mut racer).await.expect("racer wins");

    assert!(matches!(
        repo.save(&mut aggregate).await,
        Err(RepositoryError::Store(
            StoreError::ConcurrencyConflict { .. }
        ))
    ));
    assert_eq!(aggregate.uncommitted_events()[0].meta.event_id, before);
}

#[tokio::test]
async fn live_stream_yields_only_new_events_in_order_and_survives_drop() {
    use futures::StreamExt;

    let (store, repo) = store_and_repo();
    place_orders(&repo, 2).await;

    let mut live = Arc::clone(&store).subscribe_live();
    let abandoned = Arc::clone(&store).subscribe_live();

    let writer = tokio::spawn(async move {
        place_orders(&repo, 3).await;
        repo
    });
    let tokens = tokio::time::timeout(Duration::from_secs(5), async {
        let mut tokens = Vec::new();
        while tokens.len() < 3 {
            tokens.push(live.next().await.expect("open").position.token());
        }
        tokens
    })
    .await
    .expect("live events arrived");
    assert_eq!(tokens, [3, 4, 5], "history is skipped, new events in order");
    let repo = writer.await.expect("join");

    // Dropping a stream must not break later appends or other streams.
    drop(abandoned);
    place_orders(&repo, 1).await;
    let next = tokio::time::timeout(Duration::from_secs(5), live.next())
        .await
        .expect("woken by append")
        .expect("open");
    assert_eq!(next.position.token(), 6);

    // An idle stream stays pending instead of ending or spinning.
    let mut idle = Arc::clone(&store).subscribe_live();
    assert!(
        tokio::time::timeout(Duration::from_millis(50), idle.next())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn background_publish_does_not_wait_isolates_panics_and_records_failures() {
    let gate = Arc::new(tokio::sync::Notify::new());
    let reached = Arc::new(Mutex::new(false));
    let bus = Arc::new(InMemoryEventBus::new());
    bus.subscribe_all(handler(|_e: EventEnvelope| async { explode() }));
    let waiter = Arc::clone(&gate);
    bus.subscribe_all(handler(move |_e: EventEnvelope| {
        let waiter = Arc::clone(&waiter);
        async move {
            waiter.notified().await;
            Ok::<(), BackendError>(())
        }
    }));
    let flag = Arc::clone(&reached);
    bus.subscribe_all(handler(move |_e: EventEnvelope| {
        let flag = Arc::clone(&flag);
        async move {
            *flag.lock().expect("lock") = true;
            Ok::<(), BackendError>(())
        }
    }));

    // Returns immediately although a handler is blocked on the gate.
    bus.publish_background(vec![envelope_for(&OrderEvent::OrderPlaced { total: 1 })]);
    assert!(bus.take_background_failures().is_empty());

    gate.notify_one();
    tokio::time::timeout(Duration::from_secs(5), bus.flush())
        .await
        .expect("flushed");
    assert!(*reached.lock().expect("lock"), "later handler still ran");
    assert_eq!(bus.take_background_failures().len(), 1, "panic recorded");
}

// ---- Helpers ----
type TestRunner = SubscriptionRunner<
    Arc<InMemoryEventStore>,
    Arc<InMemoryCheckpointStore>,
    Arc<InMemoryEventBus>,
>;

fn store_and_repo() -> (
    Arc<InMemoryEventStore>,
    AggregateRepository<Order, Arc<InMemoryEventStore>>,
) {
    let store = Arc::new(InMemoryEventStore::new().with_registry(registry()));
    let repo = AggregateRepository::<Order, _>::new(Arc::clone(&store)).expect("category");
    (store, repo)
}

async fn place_orders(repo: &AggregateRepository<Order, Arc<InMemoryEventStore>>, count: u64) {
    for total in 0..count {
        repo.execute(
            AggregateId::new(),
            OrderCommand::Place { total },
            &CommandContext::new(),
        )
        .await
        .expect("place");
    }
}

fn runner_for(
    store: &Arc<InMemoryEventStore>,
    checkpoints: &Arc<InMemoryCheckpointStore>,
    bus: &Arc<InMemoryEventBus>,
    config: SubscriptionConfig,
) -> TestRunner {
    SubscriptionRunner::new(
        Arc::clone(store),
        Arc::clone(checkpoints),
        Arc::clone(bus),
        config,
    )
}

/// Retries with no real waiting, so failure tests stay fast.
fn fast_retry(attempts: u32) -> RetryPolicy {
    RetryPolicy {
        max_attempts: NonZeroU32::new(attempts).expect("nonzero"),
        initial_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(1),
    }
}

/// A bus whose catch-all handler records the feed token of every event it sees.
fn recording_bus() -> (Arc<InMemoryEventBus>, Arc<Mutex<Vec<u64>>>) {
    let seen: Arc<Mutex<Vec<u64>>> = Arc::default();
    let bus = Arc::new(InMemoryEventBus::new());
    let sink = Arc::clone(&seen);
    bus.subscribe_all(handler(move |envelope: EventEnvelope| {
        let sink = Arc::clone(&sink);
        async move {
            sink.lock().expect("lock").push(envelope.position.token());
            Ok::<(), BackendError>(())
        }
    }));
    (bus, seen)
}

/// A stamper that gives each event a fresh id at one fixed time.
fn test_stamp() -> impl FnMut() -> EventMeta {
    let at = chrono::Utc::now();
    move || EventMeta::new(at, CorrelationId::new())
}

/// Wraps a store and injects one rival append just before the first write, to force a conflict.
struct RacingStore {
    inner: Arc<InMemoryEventStore>,
    rival: Mutex<Option<(StreamId, EventRecord)>>,
}

impl AggregateStore for RacingStore {
    async fn append(
        &self,
        stream: &StreamId,
        events: Vec<EventRecord>,
        expected: ExpectedVersion,
    ) -> Result<AppendResult, StoreError> {
        let rival = self.rival.lock().expect("lock").take();
        if let Some((rival_stream, record)) = rival {
            self.inner
                .append(&rival_stream, vec![record], ExpectedVersion::Any)
                .await?;
        }
        self.inner.append(stream, events, expected).await
    }

    async fn read_stream(
        &self,
        stream: &StreamId,
        options: StreamReadOptions,
    ) -> Result<Vec<EventEnvelope>, StoreError> {
        self.inner.read_stream(stream, options).await
    }

    async fn stream_version(&self, stream: &StreamId) -> Result<Version, StoreError> {
        self.inner.stream_version(stream).await
    }
}

/// A handler body that panics instead of returning.
fn explode() -> Result<(), BackendError> {
    panic!("handler exploded")
}

fn envelope_for(event: &OrderEvent) -> EventEnvelope {
    use eventsource_core::event::{EventMeta, EventRecord};
    use eventsource_core::{Category, CorrelationId, StreamId, Version};

    let record = EventRecord::from_event(
        event,
        EventMeta::new(chrono::Utc::now(), CorrelationId::new()),
    )
    .expect("serialize");
    EventEnvelope {
        record,
        stream: StreamId::new(
            AggregateId::new(),
            Category::new("Order").expect("category"),
        ),
        stream_version: Version::new(1),
        position: Position::new("memory", 1),
        stored_at: chrono::Utc::now(),
    }
}
