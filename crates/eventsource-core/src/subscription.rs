//! Subscription runner: reads the global feed in pages and delivers each page to a sink.
//!
//! Delivery is at-least-once. A page is checkpointed only after the sink
//! succeeds for all of it, so a failure or restart delivers that page again.
//! Handlers must therefore be idempotent. Pages are delivered one at a time, in
//! feed order, and the runner spawns no tasks of its own.

use std::future::Future;
use std::num::{NonZeroU32, NonZeroUsize};
use std::pin::Pin;
use std::time::Duration;

use futures::FutureExt;
use futures::future::{self, Either};
use tokio::sync::watch;

use crate::bus::EventPublisher;
use crate::checkpoint::{Checkpoint, CheckpointStore};
use crate::dead_letter::{DeadLetterEntry, DeadLetterStore, NoDeadLetters, describe_failure};
use crate::envelope::{EventEnvelope, FeedReadOptions};
use crate::error::{BusError, SubscriptionError};
use crate::ids::TenantId;
use crate::store::GlobalFeed;
use crate::stream_id::Category;
use crate::telemetry::{NoopTracer, Span, Tracer, attr, span};

/// Where a subscription starts when it has no saved checkpoint.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum StartFrom {
    /// From the first event in the feed.
    #[default]
    Beginning,
    /// After the newest event. Existing history is skipped.
    Latest,
}

/// Retry policy for a page that the sink fails to deliver.
///
/// This is the only retry policy in the library. Handlers do not retry on their own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Total attempts per page, including the first.
    pub max_attempts: NonZeroU32,
    /// Wait before the second attempt. Doubles after each failure.
    pub initial_backoff: Duration,
    /// Upper bound on the wait between attempts.
    pub max_backoff: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: NonZeroU32::new(5).expect("5 is nonzero"),
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(10),
        }
    }
}

const DEFAULT_BATCH_SIZE: NonZeroUsize = match NonZeroUsize::new(100) {
    Some(size) => size,
    None => NonZeroUsize::MIN,
};

/// Settings for one named subscription. Set the public fields directly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionConfig {
    /// Checkpoint name. Two runners with the same name share progress, so use one per consumer.
    pub name: String,
    /// Maximum events per page.
    pub batch_size: NonZeroUsize,
    /// How long to wait for new events when no wake signal arrives.
    pub poll_interval: Duration,
    /// Where to start when no checkpoint exists.
    pub start_from: StartFrom,
    /// Retry policy for a failing page.
    pub retry: RetryPolicy,
    /// Only deliver events for this tenant.
    pub tenant_id: Option<TenantId>,
    /// Only deliver events for this category.
    pub category: Option<Category>,
}

impl SubscriptionConfig {
    /// A config named `name` with the defaults: batches of 100, a one-second poll, and no filters.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            batch_size: DEFAULT_BATCH_SIZE,
            poll_interval: Duration::from_secs(1),
            start_from: StartFrom::default(),
            retry: RetryPolicy::default(),
            tenant_id: None,
            category: None,
        }
    }
}

/// What a runner has delivered so far.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RunStats {
    /// Events delivered and checkpointed.
    pub events_delivered: u64,
    /// Pages delivered and checkpointed.
    pub batches: u64,
}

/// Outcome of waiting for new work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Wait {
    /// Something may have changed, or the poll interval passed. Read the feed again.
    Work,
    /// Shutdown was requested.
    Stop,
}

/// Called with the runner's checkpoint when it starts and after each saved page.
type ProgressObserver = Box<dyn Fn(&Checkpoint) + Send + Sync>;

/// Delivers the feed to a sink, checkpointing after each page.
///
/// `F` is the feed, `C` the checkpoint store, and `P` the sink, usually an
/// [`crate::bus::EventPublisher`] such as the in-memory bus. `D` is the
/// dead-letter store; see [`Self::with_dead_letters`].
pub struct SubscriptionRunner<F, C, P, D = NoDeadLetters, T = NoopTracer> {
    feed: F,
    checkpoints: C,
    sink: P,
    dead_letters: Option<D>,
    config: SubscriptionConfig,
    wake: Option<watch::Receiver<u64>>,
    cursor: Option<Checkpoint>,
    stats: RunStats,
    tracer: T,
    progress: Option<ProgressObserver>,
}

impl<F, C, P> SubscriptionRunner<F, C, P> {
    /// A runner that has not read anything yet.
    pub fn new(feed: F, checkpoints: C, sink: P, config: SubscriptionConfig) -> Self {
        Self {
            feed,
            checkpoints,
            sink,
            dead_letters: None,
            config,
            wake: None,
            cursor: None,
            stats: RunStats::default(),
            tracer: NoopTracer,
            progress: None,
        }
    }
}

impl<F, C, P, T> SubscriptionRunner<F, C, P, NoDeadLetters, T> {
    /// Parks an event that fails every attempt in `store` and moves on, instead of stopping.
    ///
    /// With a store configured the runner hands events to the sink one at a
    /// time, so a failure is pinned to one event. Each event gets the full
    /// [`RetryPolicy`]; the checkpoint then advances past it as usual. Without
    /// a store, a page that exhausts its attempts stops the runner.
    ///
    /// Delivery stays at-least-once: a restart mid-page redelivers the page's
    /// events that already succeeded.
    #[must_use]
    pub fn with_dead_letters<D2>(self, store: D2) -> SubscriptionRunner<F, C, P, D2, T> {
        SubscriptionRunner {
            feed: self.feed,
            checkpoints: self.checkpoints,
            sink: self.sink,
            dead_letters: Some(store),
            config: self.config,
            wake: self.wake,
            cursor: self.cursor,
            stats: self.stats,
            tracer: self.tracer,
            progress: self.progress,
        }
    }
}

impl<F, C, P, D, T> SubscriptionRunner<F, C, P, D, T> {
    /// Reports one span per delivered page to `tracer`.
    ///
    /// Without this call the runner uses [`NoopTracer`], which compiles away.
    #[must_use]
    pub fn with_tracer<T2: Tracer>(self, tracer: T2) -> SubscriptionRunner<F, C, P, D, T2> {
        SubscriptionRunner {
            feed: self.feed,
            checkpoints: self.checkpoints,
            sink: self.sink,
            dead_letters: self.dead_letters,
            config: self.config,
            wake: self.wake,
            cursor: self.cursor,
            stats: self.stats,
            tracer,
            progress: self.progress,
        }
    }

    /// Reports the checkpoint to `observer` when the runner starts and after each page is saved.
    ///
    /// The observer runs inline between pages, so it must not block.
    /// [`crate::manager::SubscriptionManager`] uses this for health reporting.
    #[must_use]
    pub fn with_progress(mut self, observer: impl Fn(&Checkpoint) + Send + Sync + 'static) -> Self {
        self.progress = Some(Box::new(observer));
        self
    }
}

impl<F, C, P, D, T> SubscriptionRunner<F, C, P, D, T>
where
    F: GlobalFeed,
    C: CheckpointStore,
    P: EventPublisher,
    D: DeadLetterStore,
    T: Tracer,
{
    /// Wakes the runner when the store signals new events, so it does not wait out the poll interval.
    ///
    /// The value is ignored; only the change matters. The poll interval still
    /// applies as a backstop for writers the signal does not cover.
    #[must_use]
    pub fn with_wake(mut self, wake: watch::Receiver<u64>) -> Self {
        self.wake = Some(wake);
        self
    }

    /// Delivers pages as they arrive until `shutdown` completes.
    ///
    /// A page already in flight is finished before the runner stops, so no
    /// handler is cancelled partway through an event.
    ///
    /// # Errors
    /// [`SubscriptionError::Handler`] if a page fails after every attempt. The
    /// checkpoint then stays before that page, so a restart redelivers it.
    /// Feed and checkpoint failures are returned at once and are not retried.
    pub async fn run(
        mut self,
        shutdown: impl Future<Output = ()> + Send,
    ) -> Result<RunStats, SubscriptionError> {
        let mut shutdown = Box::pin(shutdown);
        self.ensure_started().await?;
        loop {
            if (&mut shutdown).now_or_never().is_some() {
                return Ok(self.stats);
            }
            if self.process_page().await? {
                continue;
            }
            if self.wait_for_work(&mut shutdown).await == Wait::Stop {
                return Ok(self.stats);
            }
        }
    }

    /// Delivers pages until the feed has nothing new, then returns. For tests and batch jobs.
    ///
    /// # Errors
    /// As [`Self::run`].
    pub async fn run_until_caught_up(&mut self) -> Result<RunStats, SubscriptionError> {
        self.ensure_started().await?;
        while self.process_page().await? {}
        Ok(self.stats)
    }

    /// Loads the saved checkpoint, or creates one according to `start_from`.
    async fn ensure_started(&mut self) -> Result<(), SubscriptionError> {
        if self.cursor.is_some() {
            return Ok(());
        }
        let cursor = match self.checkpoints.load(&self.config.name).await? {
            Some(saved) => saved,
            None => match self.config.start_from {
                StartFrom::Beginning => Checkpoint::default(),
                StartFrom::Latest => {
                    // Save now, so a restart does not jump to a newer position.
                    let fresh = Checkpoint {
                        position: self.feed.current_position().await?,
                        ..Checkpoint::default()
                    };
                    self.checkpoints
                        .save(&self.config.name, fresh.clone())
                        .await?;
                    fresh
                }
            },
        };
        self.report_progress(&cursor);
        self.cursor = Some(cursor);
        Ok(())
    }

    /// Reads and delivers one page. Returns `false` when the feed had nothing new.
    async fn process_page(&mut self) -> Result<bool, SubscriptionError> {
        let cursor = self.cursor.clone().unwrap_or_default();
        let options = FeedReadOptions {
            tenant_id: self.config.tenant_id,
            category: self.config.category.clone(),
            limit: Some(self.config.batch_size),
        };
        let batch = self
            .feed
            .read_all(cursor.position.as_ref(), options)
            .await?;
        let Some(last) = batch.last() else {
            return Ok(false);
        };
        let delivered = u64::try_from(batch.len()).unwrap_or(u64::MAX);
        let next = Checkpoint {
            position: Some(last.position.clone()),
            last_event_id: Some(last.record.meta.event_id),
            events_processed: cursor.events_processed.saturating_add(delivered),
        };

        let mut trace = self.start_page_span(batch.len());
        let mut failures = 0;
        let outcome = self.deliver_page(&batch, &mut failures).await;
        trace.set(attr::FAILURE_COUNT, u64::from(failures).into());
        if let Err(error) = outcome {
            trace.fail(error_kind(&error));
            return Err(error);
        }
        if let Err(error) = self.checkpoints.save(&self.config.name, next.clone()).await {
            trace.fail("CheckpointError");
            return Err(error.into());
        }
        drop(trace);

        self.report_progress(&next);
        self.cursor = Some(next);
        self.stats.events_delivered = self.stats.events_delivered.saturating_add(delivered);
        self.stats.batches += 1;
        Ok(true)
    }

    fn report_progress(&self, checkpoint: &Checkpoint) {
        if let Some(observer) = &self.progress {
            observer(checkpoint);
        }
    }

    fn start_page_span(&self, page_size: usize) -> T::Span {
        if !T::ENABLED {
            return self.tracer.start(span::SUBSCRIPTION_PAGE, &[]);
        }
        self.tracer.start(
            span::SUBSCRIPTION_PAGE,
            &[
                (attr::SUBSCRIPTION_NAME, self.config.name.as_str().into()),
                (attr::BATCH_SIZE, page_size.into()),
            ],
        )
    }

    /// Delivers a page: whole under the retry policy, or event by event with a dead-letter store.
    ///
    /// `failures` counts failed sink attempts, including ones a retry later recovered.
    async fn deliver_page(
        &self,
        batch: &[EventEnvelope],
        failures: &mut u32,
    ) -> Result<(), SubscriptionError> {
        let Some(store) = &self.dead_letters else {
            return self
                .deliver(batch, failures)
                .await
                .map_err(|(attempts, last)| SubscriptionError::Handler {
                    name: self.config.name.clone(),
                    attempts,
                    last,
                });
        };
        for envelope in batch {
            let Err((attempts, last)) =
                self.deliver(std::slice::from_ref(envelope), failures).await
            else {
                continue;
            };
            let entry = DeadLetterEntry {
                subscription: self.config.name.clone(),
                envelope: envelope.clone(),
                error: describe_failure(&last),
                attempts,
            };
            store
                .park(entry)
                .await
                .map_err(|source| SubscriptionError::DeadLetter {
                    name: self.config.name.clone(),
                    attempts,
                    source,
                })?;
        }
        Ok(())
    }

    /// Hands `batch` to the sink, retrying under the retry policy.
    ///
    /// On exhaustion returns the attempts made and the last failure.
    async fn deliver(
        &self,
        batch: &[EventEnvelope],
        failures: &mut u32,
    ) -> Result<(), (u32, BusError)> {
        let policy = self.config.retry;
        let mut backoff = policy.initial_backoff;
        let mut attempt: u32 = 1;
        loop {
            let error = match self.sink.publish(batch).await {
                Ok(()) => return Ok(()),
                Err(error) => error,
            };
            *failures += 1;
            if attempt >= policy.max_attempts.get() {
                return Err((attempt, error));
            }
            tokio::time::sleep(backoff).await;
            backoff = backoff.saturating_mul(2).min(policy.max_backoff);
            attempt += 1;
        }
    }

    /// Waits for a wake signal, the poll interval, or shutdown, whichever comes first.
    async fn wait_for_work<S: Future<Output = ()>>(&mut self, shutdown: &mut Pin<Box<S>>) -> Wait {
        let poll = self.config.poll_interval;
        let sleep = Box::pin(tokio::time::sleep(poll));
        let wake = Box::pin(async {
            match self.wake.as_mut() {
                Some(rx) => rx.changed().await.is_ok(),
                None => future::pending::<bool>().await,
            }
        });
        let (outcome, wake_closed) =
            match future::select(shutdown, future::select(wake, sleep)).await {
                Either::Left(_) => (Wait::Stop, false),
                // A closed sender means no more wake-ups; fall back to polling only.
                Either::Right((Either::Left((changed, _)), _)) => (Wait::Work, !changed),
                Either::Right((Either::Right(_), _)) => (Wait::Work, false),
            };
        if wake_closed {
            self.wake = None;
        }
        outcome
    }
}

/// A short, payload-free name for a subscription error, safe to put on a span.
fn error_kind(error: &SubscriptionError) -> &'static str {
    match error {
        SubscriptionError::Handler { .. } => "Handler",
        SubscriptionError::DeadLetter { .. } => "DeadLetter",
        _ => "SubscriptionError",
    }
}
