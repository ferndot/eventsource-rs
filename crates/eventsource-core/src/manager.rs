//! Subscription manager: named runners with start, stop, pause, resume, shutdown and health.
//!
//! The manager spawns nothing. [`SubscriptionManager::new`] returns a cheap,
//! cloneable handle and a [`ManagerDriver`]; the application runs the driver
//! on whatever executor it uses (for example `tokio::spawn(driver.run())`).
//! Every registered runner is polled inside that one future.

use std::collections::HashMap;
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
    /// The call does not apply to the subscription's current status.
    #[error("subscription {name:?} is {status:?}")]
    InvalidState {
        /// The subscription.
        name: String,
        /// Its status when the call was refused.
        status: SubscriptionStatus,
    },
    /// [`SubscriptionManager::shutdown`] was called; no more subscriptions are accepted.
    #[error("the manager is shutting down")]
    ShuttingDown,
    /// The [`ManagerDriver`] was dropped, so no subscription can make progress.
    #[error("the manager driver is gone")]
    DriverGone,
}

/// What a subscription has been asked to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Control {
    Run,
    Pause,
    Shutdown,
}

type RunFuture = BoxFuture<'static, Result<RunStats, SubscriptionError>>;
type Factory = Box<dyn Fn(BoxFuture<'static, ()>) -> RunFuture + Send + Sync>;

struct Entry {
    status: Arc<watch::Sender<SubscriptionStatus>>,
    control: watch::Sender<Control>,
}

struct Inner {
    entries: Mutex<HashMap<String, Entry>>,
    tasks: Mutex<Option<mpsc::UnboundedSender<BoxFuture<'static, ()>>>>,
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
            tasks: Mutex::new(Some(tx)),
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
        let tasks = self
            .inner
            .tasks
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let Some(tasks) = tasks.as_ref() else {
            return Err(ManagerError::ShuttingDown);
        };
        let status = Arc::new(watch::channel(SubscriptionStatus::Running).0);
        let (control, control_rx) = watch::channel(Control::Run);
        let task = drive(factory, control_rx, Arc::clone(&status));
        entries.insert(name, Entry { status, control });
        // A dropped driver means nothing will ever run; the status stays as registered.
        let _ = tasks.send(Box::pin(task));
        Ok(())
    }

    /// Stops every subscription after its current page and waits for all of them.
    ///
    /// Each runner finishes the page it is delivering, saves that page's
    /// checkpoint and stops; nothing is cancelled mid-event. When this returns
    /// every status is [`SubscriptionStatus::Stopped`] (or `Failed`), the
    /// driver is about to return, and no new subscriptions are accepted.
    /// The driver must be running, or this waits for it.
    pub async fn shutdown(&self) {
        let controls: Vec<_> = {
            let entries = self
                .inner
                .entries
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            // Closing the intake ends the driver once the running futures finish.
            self.inner
                .tasks
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
            entries
                .values()
                .map(|entry| {
                    entry.control.send_replace(Control::Shutdown);
                    entry.control.clone()
                })
                .collect()
        };
        // A control sender reports `closed` once its subscription's future has finished.
        future::join_all(controls.iter().map(watch::Sender::closed)).await;
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

    /// Stops delivery for `name` after its current page. The checkpoint is kept.
    ///
    /// Returns once the runner has stopped. A paused subscription holds its
    /// place: [`Self::resume`] continues from the saved checkpoint.
    ///
    /// # Errors
    /// [`ManagerError::UnknownSubscription`], or [`ManagerError::InvalidState`]
    /// if the subscription is not running (or failed while pausing).
    pub async fn pause(&self, name: &str) -> Result<(), ManagerError> {
        let (mut status, gone) = {
            let entries = self
                .inner
                .entries
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let entry = lookup(&entries, name)?;
            require(name, entry, |s| *s == SubscriptionStatus::Running)?;
            entry.control.send_replace(Control::Pause);
            (entry.status.subscribe(), entry.control.clone())
        };
        let settled = status.wait_for(|s| *s != SubscriptionStatus::Running);
        let closed = Box::pin(gone.closed());
        match future::select(Box::pin(settled), closed).await {
            Either::Left((Ok(_), _)) => {}
            _ => return Err(ManagerError::DriverGone),
        }
        let status = status.borrow().clone();
        if status == SubscriptionStatus::Paused {
            Ok(())
        } else {
            Err(ManagerError::InvalidState {
                name: name.to_owned(),
                status,
            })
        }
    }

    /// Restarts a paused subscription from its saved checkpoint.
    ///
    /// # Errors
    /// [`ManagerError::UnknownSubscription`], or [`ManagerError::InvalidState`]
    /// if the subscription is not paused.
    pub fn resume(&self, name: &str) -> Result<(), ManagerError> {
        let entries = self
            .inner
            .entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let entry = lookup(&entries, name)?;
        require(name, entry, |s| *s == SubscriptionStatus::Paused)?;
        entry.status.send_replace(SubscriptionStatus::Running);
        entry.control.send_replace(Control::Run);
        Ok(())
    }
}

fn lookup<'a>(entries: &'a HashMap<String, Entry>, name: &str) -> Result<&'a Entry, ManagerError> {
    entries
        .get(name)
        .ok_or_else(|| ManagerError::UnknownSubscription(name.to_owned()))
}

fn require(
    name: &str,
    entry: &Entry,
    allowed: impl Fn(&SubscriptionStatus) -> bool,
) -> Result<(), ManagerError> {
    let status = entry.status.borrow().clone();
    if allowed(&status) {
        Ok(())
    } else {
        Err(ManagerError::InvalidState {
            name: name.to_owned(),
            status,
        })
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

/// The error and its causes on one line, e.g. `outer: inner`.
fn describe(error: &(dyn std::error::Error + 'static)) -> String {
    let mut text = error.to_string();
    let mut cause = error.source();
    while let Some(inner) = cause {
        text.push_str(": ");
        text.push_str(&inner.to_string());
        cause = inner.source();
    }
    text
}

/// Resolves once `control` is no longer [`Control::Run`], or its sender is gone.
fn stop_signal(mut control: watch::Receiver<Control>) -> BoxFuture<'static, ()> {
    Box::pin(async move {
        while *control.borrow_and_update() == Control::Run {
            if control.changed().await.is_err() {
                return;
            }
        }
    })
}

/// Runs one subscription's runner, following its control signal.
async fn drive(
    factory: Factory,
    mut control: watch::Receiver<Control>,
    status: Arc<watch::Sender<SubscriptionStatus>>,
) {
    loop {
        let wanted = *control.borrow_and_update();
        match wanted {
            Control::Run => {
                status.send_replace(SubscriptionStatus::Running);
                match factory(stop_signal(control.clone())).await {
                    Ok(_) if *control.borrow() == Control::Run => return,
                    Ok(_) => {}
                    Err(error) => {
                        status.send_replace(SubscriptionStatus::Failed(describe(&error)));
                        // Stay failed until someone changes what the subscription should do.
                        if control.changed().await.is_err() {
                            return;
                        }
                    }
                }
            }
            Control::Shutdown => {
                status.send_replace(SubscriptionStatus::Stopped);
                return;
            }
            Control::Pause => {
                status.send_replace(SubscriptionStatus::Paused);
                if control.changed().await.is_err() {
                    return;
                }
            }
        }
    }
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

    #[tokio::test]
    async fn pause_stops_delivery_and_resume_continues_from_the_checkpoint() {
        let (manager, driver) = SubscriptionManager::new();
        let driver = tokio::spawn(driver.run());
        let feed = Feed::default();
        feed.append(3);
        let (checkpoints, sink) = (Checkpoints::default(), Sink::default());
        register(&manager, "alpha", &feed, &checkpoints, &sink);
        eventually(|| (sink.tokens() == [1, 2, 3]).then_some(())).await;

        manager.pause("alpha").await.unwrap();
        assert_eq!(manager.status("alpha"), Ok(SubscriptionStatus::Paused));
        feed.append(3);
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(
            sink.tokens(),
            [1, 2, 3],
            "nothing is delivered while paused"
        );
        let paused = checkpoints.get("alpha").unwrap();
        assert_eq!(paused.position, Some(Position::new("fake", 3)));
        assert_eq!(paused.events_processed, 3);

        manager.resume("alpha").unwrap();
        assert_eq!(manager.status("alpha"), Ok(SubscriptionStatus::Running));
        eventually(|| (sink.tokens().len() >= 6).then_some(())).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            sink.tokens(),
            [1, 2, 3, 4, 5, 6],
            "in order, no duplicates, no gaps"
        );
        let resumed = checkpoints.get("alpha").unwrap();
        assert_eq!(resumed.position, Some(Position::new("fake", 6)));
        assert_eq!(resumed.events_processed, 6);
        driver.abort();
    }

    #[tokio::test]
    async fn shutdown_waits_for_the_current_page_and_checkpoints_the_last_delivered_event() {
        let (manager, driver) = SubscriptionManager::new();
        let driver = tokio::spawn(driver.run());
        let feed = Feed::default();
        feed.append(4);
        let checkpoints = Checkpoints::default();
        let slow = || Sink {
            page_delay: Duration::from_millis(150),
            ..Sink::default()
        };
        let (sink_a, sink_b) = (slow(), slow());
        register(&manager, "alpha", &feed, &checkpoints, &sink_a);
        register(&manager, "beta", &feed, &checkpoints, &sink_b);
        sink_a.entered.notified().await;
        sink_b.entered.notified().await;
        // Both runners are now inside their first page (events 1 and 2).

        manager.shutdown().await;

        for (name, sink) in [("alpha", &sink_a), ("beta", &sink_b)] {
            assert_eq!(
                sink.tokens(),
                [1, 2],
                "{name}: current page finished, next not started"
            );
            let saved = checkpoints.get(name).unwrap();
            assert_eq!(saved.position, Some(Position::new("fake", 2)), "{name}");
            assert_eq!(saved.events_processed, 2, "{name}");
            assert_eq!(manager.status(name), Ok(SubscriptionStatus::Stopped));
        }
        tokio::time::timeout(Duration::from_secs(1), driver)
            .await
            .expect("driver ends after shutdown")
            .unwrap();
        let late = manager.register("gamma", || {
            SubscriptionRunner::new(
                Feed::default(),
                Checkpoints::default(),
                Sink::default(),
                config("gamma"),
            )
        });
        assert_eq!(late, Err(ManagerError::ShuttingDown));
    }

    #[tokio::test]
    async fn failing_handler_marks_only_that_subscription_failed() {
        let (manager, driver) = SubscriptionManager::new();
        let driver = tokio::spawn(driver.run());
        let feed = Feed::default();
        feed.append(2);
        let checkpoints = Checkpoints::default();
        let broken = Sink {
            fail_with: Some("boom: disk on fire"),
            ..Sink::default()
        };
        let healthy = Sink::default();
        register(&manager, "broken", &feed, &checkpoints, &broken);
        register(&manager, "healthy", &feed, &checkpoints, &healthy);

        let status = eventually(|| match manager.status("broken") {
            Ok(status @ SubscriptionStatus::Failed(_)) => Some(status),
            _ => None,
        })
        .await;
        let SubscriptionStatus::Failed(text) = status else {
            unreachable!()
        };
        assert!(
            text.contains("boom: disk on fire"),
            "error text was {text:?}"
        );
        assert!(broken.tokens().is_empty());
        assert_eq!(
            checkpoints.get("broken"),
            None,
            "failed page is not checkpointed"
        );

        feed.append(2);
        eventually(|| (healthy.tokens() == [1, 2, 3, 4]).then_some(())).await;
        assert_eq!(manager.status("healthy"), Ok(SubscriptionStatus::Running));
        driver.abort();
    }
}
