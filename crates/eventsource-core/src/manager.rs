//! Subscription manager: named runners with start, stop, pause, resume, shutdown and health.
//!
//! The manager spawns nothing. [`SubscriptionManager::new`] returns a cheap,
//! cloneable handle and a [`ManagerDriver`]; the application runs the driver
//! on whatever executor it uses (for example `tokio::spawn(driver.run())`).
//! Every registered runner is polled inside that one future.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};

use futures::future::{self, BoxFuture, Either};
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::{mpsc, watch};

use crate::bus::EventPublisher;
use crate::checkpoint::CheckpointStore;
use crate::dead_letter::DeadLetterStore;
use crate::error::SubscriptionError;
use crate::store::GlobalFeed;
use crate::subscription::{RunStats, SubscriptionRunner};
use crate::telemetry::Tracer;

/// What a managed subscription is doing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubscriptionStatus {
    /// The runner is delivering events.
    Running,
    /// Delivery is suspended. The checkpoint is kept.
    Paused,
    /// The runner was stopped on request.
    Stopped,
    /// The runner gave up. The text is the error and its causes.
    Failed(String),
}

/// Why a manager call was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ManagerError {
    /// No subscription is registered under this name.
    #[error("no subscription named {0:?}")]
    UnknownSubscription(String),
    /// A subscription with this name is already registered.
    #[error("subscription {0:?} is already registered")]
    AlreadyRegistered(String),
}

type RunFuture = BoxFuture<'static, Result<RunStats, SubscriptionError>>;
type Factory = Box<dyn Fn(BoxFuture<'static, ()>) -> RunFuture + Send + Sync>;

struct Entry {
    status: watch::Receiver<SubscriptionStatus>,
}

struct Inner {
    entries: Mutex<HashMap<String, Entry>>,
    tasks: mpsc::UnboundedSender<BoxFuture<'static, ()>>,
}

/// Handle to a set of named subscription runners. Clones share the same set.
#[derive(Clone)]
pub struct SubscriptionManager {
    inner: Arc<Inner>,
}

/// The future that polls every registered runner. Run it exactly once.
pub struct ManagerDriver {
    tasks: mpsc::UnboundedReceiver<BoxFuture<'static, ()>>,
}

impl SubscriptionManager {
    /// An empty manager and the driver that runs its subscriptions.
    #[must_use]
    pub fn new() -> (Self, ManagerDriver) {
        let (tx, rx) = mpsc::unbounded_channel();
        let inner = Inner {
            entries: Mutex::new(HashMap::new()),
            tasks: tx,
        };
        (
            Self {
                inner: Arc::new(inner),
            },
            ManagerDriver { tasks: rx },
        )
    }

    /// Registers a runner under `name` and starts it.
    ///
    /// `build` makes a fresh runner each time the subscription (re)starts, so
    /// it should clone the adapters it needs. `name` should equal the runner's
    /// checkpoint name.
    ///
    /// # Errors
    /// [`ManagerError::AlreadyRegistered`] if `name` is taken.
    pub fn register<F, C, P, D, T, B>(
        &self,
        name: impl Into<String>,
        build: B,
    ) -> Result<(), ManagerError>
    where
        B: Fn() -> SubscriptionRunner<F, C, P, D, T> + Send + Sync + 'static,
        F: GlobalFeed + 'static,
        C: CheckpointStore + 'static,
        P: EventPublisher + 'static,
        D: DeadLetterStore + 'static,
        T: Tracer + 'static,
    {
        let name = name.into();
        let mut entries = self
            .inner
            .entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if entries.contains_key(&name) {
            return Err(ManagerError::AlreadyRegistered(name));
        }
        let factory: Factory = Box::new(move |stop| Box::pin(build().run(stop)));
        let (status_tx, status_rx) = watch::channel(SubscriptionStatus::Running);
        entries.insert(name, Entry { status: status_rx });
        // A dropped driver means nothing will ever run; the status stays as registered.
        let _ = self.inner.tasks.send(Box::pin(drive(factory, status_tx)));
        Ok(())
    }

    /// The current status of `name`.
    ///
    /// # Errors
    /// [`ManagerError::UnknownSubscription`] if `name` is not registered.
    pub fn status(&self, name: &str) -> Result<SubscriptionStatus, ManagerError> {
        let entries = self
            .inner
            .entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        entries
            .get(name)
            .map(|entry| entry.status.borrow().clone())
            .ok_or_else(|| ManagerError::UnknownSubscription(name.to_owned()))
    }
}

impl ManagerDriver {
    /// Polls every registered runner until the manager is shut down.
    pub async fn run(mut self) {
        let mut running: FuturesUnordered<BoxFuture<'static, ()>> = FuturesUnordered::new();
        loop {
            if running.is_empty() {
                match self.tasks.recv().await {
                    Some(task) => running.push(task),
                    None => return,
                }
                continue;
            }
            let incoming = Box::pin(self.tasks.recv());
            match future::select(incoming, running.next()).await {
                Either::Left((Some(task), _)) => running.push(task),
                Either::Left((None, _)) => {
                    while running.next().await.is_some() {}
                    return;
                }
                Either::Right(_) => {}
            }
        }
    }
}

/// Runs one subscription's runner.
async fn drive(factory: Factory, status: watch::Sender<SubscriptionStatus>) {
    let never: Pin<Box<dyn Future<Output = ()> + Send>> = Box::pin(future::pending());
    let _ = factory(never).await;
    let _ = status;
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex, PoisonError};
    use std::time::Duration;

    use chrono::Utc;
    use tokio::sync::Notify;

    use super::*;
    use crate::bus::EventPublisher;
    use crate::checkpoint::{Checkpoint, CheckpointStore};
    use crate::envelope::{EventEnvelope, FeedReadOptions};
    use crate::error::{BusError, CheckpointError, StoreError};
    use crate::event::{EventMeta, EventRecord};
    use crate::ids::{AggregateId, CorrelationId};
    use crate::position::Position;
    use crate::store::GlobalFeed;
    use crate::stream_id::{Category, StreamId};
    use crate::subscription::{RetryPolicy, SubscriptionConfig, SubscriptionRunner};
    use crate::version::Version;

    /// In-test feed: events with positions 1..=n.
    #[derive(Clone, Default)]
    struct Feed {
        events: Arc<Mutex<Vec<EventEnvelope>>>,
    }

    impl Feed {
        fn append(&self, count: u64) {
            let mut events = self.events.lock().unwrap_or_else(PoisonError::into_inner);
            for _ in 0..count {
                let token = events.len() as u64 + 1;
                let meta = EventMeta::new(Utc::now(), CorrelationId::new());
                events.push(EventEnvelope {
                    record: EventRecord::new(meta, "Thing", serde_json::json!({ "n": token })),
                    stream: StreamId::new(AggregateId::new(), Category::new("thing").unwrap()),
                    stream_version: Version::new(1),
                    position: Position::new("fake", token),
                    stored_at: Utc::now(),
                });
            }
        }
    }

    impl GlobalFeed for Feed {
        async fn read_all(
            &self,
            from: Option<&Position>,
            options: FeedReadOptions,
        ) -> Result<Vec<EventEnvelope>, StoreError> {
            let after = from.map_or(0, Position::token);
            let events = self.events.lock().unwrap_or_else(PoisonError::into_inner);
            let limit = options
                .limit
                .map_or(usize::MAX, std::num::NonZeroUsize::get);
            Ok(events
                .iter()
                .filter(|e| e.position.token() > after)
                .take(limit)
                .cloned()
                .collect())
        }

        async fn current_position(&self) -> Result<Option<Position>, StoreError> {
            let events = self.events.lock().unwrap_or_else(PoisonError::into_inner);
            Ok(events.last().map(|e| e.position.clone()))
        }
    }

    #[derive(Clone, Default)]
    struct Checkpoints {
        saved: Arc<Mutex<HashMap<String, Checkpoint>>>,
    }

    impl Checkpoints {
        fn get(&self, name: &str) -> Option<Checkpoint> {
            let saved = self.saved.lock().unwrap_or_else(PoisonError::into_inner);
            saved.get(name).cloned()
        }
    }

    impl CheckpointStore for Checkpoints {
        async fn load(&self, name: &str) -> Result<Option<Checkpoint>, CheckpointError> {
            Ok(self.get(name))
        }

        async fn save(&self, name: &str, checkpoint: Checkpoint) -> Result<(), CheckpointError> {
            let mut saved = self.saved.lock().unwrap_or_else(PoisonError::into_inner);
            saved.insert(name.to_owned(), checkpoint);
            Ok(())
        }
    }

    /// Records delivered position tokens. Can fail on demand or dawdle inside a page.
    #[derive(Clone, Default)]
    struct Sink {
        delivered: Arc<Mutex<Vec<u64>>>,
        fail_with: Option<&'static str>,
        page_delay: Duration,
        entered: Arc<Notify>,
    }

    impl Sink {
        fn tokens(&self) -> Vec<u64> {
            self.delivered
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone()
        }
    }

    impl EventPublisher for Sink {
        async fn publish(&self, envelopes: &[EventEnvelope]) -> Result<(), BusError> {
            self.entered.notify_one();
            tokio::time::sleep(self.page_delay).await;
            if let Some(reason) = self.fail_with {
                return Err(BusError::Backend(reason.into()));
            }
            let mut delivered = self
                .delivered
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            delivered.extend(envelopes.iter().map(|e| e.position.token()));
            Ok(())
        }
    }

    fn config(name: &str) -> SubscriptionConfig {
        let mut config = SubscriptionConfig::new(name);
        config.poll_interval = Duration::from_millis(10);
        config.batch_size = std::num::NonZeroUsize::new(2).unwrap();
        config.retry = RetryPolicy {
            max_attempts: std::num::NonZeroU32::MIN,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(1),
        };
        config
    }

    fn register(
        manager: &SubscriptionManager,
        name: &'static str,
        feed: &Feed,
        checkpoints: &Checkpoints,
        sink: &Sink,
    ) {
        let (feed, checkpoints, sink) = (feed.clone(), checkpoints.clone(), sink.clone());
        manager
            .register(name, move || {
                SubscriptionRunner::new(
                    feed.clone(),
                    checkpoints.clone(),
                    sink.clone(),
                    config(name),
                )
            })
            .unwrap();
    }

    async fn eventually<T>(mut probe: impl FnMut() -> Option<T>) -> T {
        for _ in 0..500 {
            if let Some(value) = probe() {
                return value;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("condition not reached within 5s");
    }

    #[tokio::test]
    async fn registers_two_subscriptions_and_reports_both_running() {
        let (manager, driver) = SubscriptionManager::new();
        let driver = tokio::spawn(driver.run());
        let feed = Feed::default();
        feed.append(3);
        let (checkpoints, sink_a, sink_b) =
            (Checkpoints::default(), Sink::default(), Sink::default());
        register(&manager, "alpha", &feed, &checkpoints, &sink_a);
        register(&manager, "beta", &feed, &checkpoints, &sink_b);

        assert_eq!(manager.status("alpha"), Ok(SubscriptionStatus::Running));
        assert_eq!(manager.status("beta"), Ok(SubscriptionStatus::Running));
        eventually(|| (sink_a.tokens() == [1, 2, 3] && sink_b.tokens() == [1, 2, 3]).then_some(()))
            .await;
        assert_eq!(manager.status("alpha"), Ok(SubscriptionStatus::Running));
        assert_eq!(
            manager.status("nope"),
            Err(ManagerError::UnknownSubscription("nope".to_owned()))
        );
        driver.abort();
    }
}
