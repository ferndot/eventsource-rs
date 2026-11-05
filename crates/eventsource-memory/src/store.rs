//! In-process event store. Non-persistent; suitable for tests and single-process use.

use std::collections::{HashMap, HashSet, VecDeque};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, PoisonError};

use futures::stream::{self, BoxStream};
use tokio::sync::watch;

use eventsource_core::Category;
use eventsource_core::envelope::{
    AppendResult, EventEnvelope, FeedReadOptions, ReadDirection, StreamReadOptions,
};
use eventsource_core::error::{PositionError, StoreError};
use eventsource_core::event::EventRecord;
use eventsource_core::ids::EventId;
use eventsource_core::live::LiveFeed;
use eventsource_core::store::{AggregateStore, GlobalFeed};
use eventsource_core::version::ExpectedVersion;
use eventsource_core::{Clock, EventRegistry, Position, StreamId, SystemClock, Version};

#[derive(Default)]
struct Inner {
    events: Vec<EventEnvelope>,
    streams: HashMap<StreamId, Vec<usize>>,
    event_ids: HashSet<EventId>,
}

/// An in-memory [`AggregateStore`] and [`GlobalFeed`].
///
/// All state sits behind one [`Mutex`]. The lock is never held across an
/// `.await`, and every method completes without awaiting, so there is no
/// interleaving to reason about inside an append.
pub struct InMemoryEventStore {
    store_id: String,
    registry: Option<Arc<EventRegistry>>,
    clock: Arc<dyn Clock>,
    inner: Mutex<Inner>,
    wake: watch::Sender<u64>,
}

impl InMemoryEventStore {
    /// A store with a unique id, no registry check, and the system clock.
    #[must_use]
    pub fn new() -> Self {
        Self {
            store_id: uuid::Uuid::new_v4().to_string(),
            registry: None,
            clock: Arc::new(SystemClock),
            inner: Mutex::new(Inner::default()),
            wake: watch::channel(0).0,
        }
    }

    /// A receiver that changes after every successful append.
    ///
    /// The value is the newest global token. Subscribers should only use the
    /// change as a hint to read the feed again; the feed stays the source of truth.
    #[must_use]
    pub fn wake(&self) -> watch::Receiver<u64> {
        self.wake.subscribe()
    }

    /// Sets the store id embedded in every [`Position`].
    ///
    /// Positions from stores with the same id compare as if they came from one
    /// feed. Pass an explicit id only when you need a stable one, such as in a
    /// test that checks serialized positions.
    #[must_use]
    pub fn with_store_id(mut self, store_id: impl Into<String>) -> Self {
        self.store_id = store_id.into();
        self
    }

    /// Rejects appends whose event types are not in `registry`.
    #[must_use]
    pub fn with_registry(mut self, registry: Arc<EventRegistry>) -> Self {
        self.registry = Some(registry);
        self
    }

    /// Sets the clock used for `stored_at`.
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        // A panic in another thread cannot leave `Inner` half-written: every
        // mutation below finishes before it can panic, so the data is still valid.
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn position(&self, index: usize) -> Position {
        Position::new(self.store_id.clone(), (index as u64) + 1)
    }
}

impl Default for InMemoryEventStore {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateStore for InMemoryEventStore {
    async fn append(
        &self,
        stream: &StreamId,
        events: Vec<EventRecord>,
        expected: ExpectedVersion,
    ) -> Result<AppendResult, StoreError> {
        if events.is_empty() {
            return Err(StoreError::EmptyAppend);
        }
        if let Some(registry) = &self.registry
            && let Some(unknown) = events.iter().find(|e| !registry.contains(&e.event_type))
        {
            return Err(StoreError::UnregisteredEventType(
                unknown.event_type.clone(),
            ));
        }

        let mut inner = self.lock();
        let current = Version::new(inner.streams.get(stream).map_or(0, Vec::len) as u64);
        if !expected.is_satisfied_by(current) {
            return Err(StoreError::ConcurrencyConflict {
                stream: stream.clone(),
                expected,
                actual: current,
            });
        }

        let mut batch_ids = HashSet::with_capacity(events.len());
        for record in &events {
            let id = record.meta.event_id;
            if inner.event_ids.contains(&id) || !batch_ids.insert(id) {
                return Err(StoreError::DuplicateEvent(id));
            }
        }

        let now = self.clock.now();
        let stored_at = inner
            .events
            .last()
            .map_or(now, |last| now.max(last.stored_at));
        let base = inner.events.len();
        let envelopes: Vec<EventEnvelope> = events
            .into_iter()
            .enumerate()
            .map(|(offset, record)| EventEnvelope {
                record,
                stream: stream.clone(),
                stream_version: current.advance(offset as u64 + 1),
                position: self.position(base + offset),
                stored_at,
            })
            .collect();

        for envelope in &envelopes {
            inner.event_ids.insert(envelope.record.meta.event_id);
        }
        let indices = inner.streams.entry(stream.clone()).or_default();
        indices.extend(base..base + envelopes.len());
        inner.events.extend(envelopes.iter().cloned());

        let new_version = current.advance(envelopes.len() as u64);
        let head = envelopes.last().map_or(0, |e| e.position.token());
        drop(inner);
        self.wake.send_replace(head);

        Ok(AppendResult {
            stream: stream.clone(),
            new_version,
            envelopes,
        })
    }

    async fn read_stream(
        &self,
        stream: &StreamId,
        options: StreamReadOptions,
    ) -> Result<Vec<EventEnvelope>, StoreError> {
        let inner = self.lock();
        let indices: &[usize] = inner.streams.get(stream).map_or(&[], Vec::as_slice);
        let total = indices.len();

        // Versions are 1-based and inclusive; index `i` holds version `i + 1`.
        let start = options
            .from
            .map_or(0, |v| {
                usize::try_from(v.get())
                    .unwrap_or(usize::MAX)
                    .saturating_sub(1)
            })
            .min(total);
        let end = options
            .to
            .map_or(total, |v| usize::try_from(v.get()).unwrap_or(usize::MAX))
            .min(total);
        if start >= end {
            return Ok(Vec::new());
        }

        let slice = indices[start..end].iter().map(|&i| inner.events[i].clone());
        let limit = options.limit.map_or(usize::MAX, NonZeroUsize::get);
        let selected = match options.direction {
            ReadDirection::Forward => slice.take(limit).collect(),
            ReadDirection::Backward => slice.rev().take(limit).collect(),
        };
        Ok(selected)
    }

    async fn stream_version(&self, stream: &StreamId) -> Result<Version, StoreError> {
        let inner = self.lock();
        Ok(Version::new(
            inner.streams.get(stream).map_or(0, Vec::len) as u64
        ))
    }
}

impl GlobalFeed for InMemoryEventStore {
    async fn read_all(
        &self,
        from: Option<&Position>,
        options: FeedReadOptions,
    ) -> Result<Vec<EventEnvelope>, StoreError> {
        let start = match from {
            Some(position) => {
                if position.store_id() != self.store_id {
                    return Err(PositionError::Foreign {
                        left: self.store_id.clone(),
                        right: position.store_id().to_owned(),
                    }
                    .into());
                }
                // Token `n` is the event at index `n - 1`, so the next event is index `n`.
                usize::try_from(position.token()).unwrap_or(usize::MAX)
            }
            None => 0,
        };

        let inner = self.lock();
        let tail = inner.events.get(start..).unwrap_or(&[]);
        let matching = tail.iter().filter(|envelope| {
            options
                .tenant_id
                .is_none_or(|t| envelope.record.meta.tenant_id == Some(t))
                && options
                    .category
                    .as_ref()
                    .is_none_or(|c: &Category| envelope.stream.category() == c)
        });
        let limit = options.limit.map_or(usize::MAX, NonZeroUsize::get);
        Ok(matching.take(limit).cloned().collect())
    }

    async fn current_position(&self) -> Result<Option<Position>, StoreError> {
        let inner = self.lock();
        Ok(inner
            .events
            .last()
            .map(|envelope| envelope.position.clone()))
    }
}

/// Max events read from the feed per wake-up while tailing.
const LIVE_PAGE: NonZeroUsize = NonZeroUsize::MIN.saturating_add(255);

/// Cursor state of one live stream.
struct LiveTail {
    store: Arc<InMemoryEventStore>,
    wake: watch::Receiver<u64>,
    after: Option<Position>,
    buffered: VecDeque<EventEnvelope>,
}

impl LiveTail {
    /// The next event after the cursor, waiting for appends. `None` ends the stream.
    async fn next(&mut self) -> Option<EventEnvelope> {
        loop {
            if let Some(next) = self.buffered.pop_front() {
                self.after = Some(next.position.clone());
                return Some(next);
            }
            let options = FeedReadOptions {
                limit: Some(LIVE_PAGE),
                ..FeedReadOptions::default()
            };
            let page = self
                .store
                .read_all(self.after.as_ref(), options)
                .await
                .ok()?;
            if page.is_empty() {
                // Errs only if the sender is gone, which cannot happen while we hold the store.
                self.wake.changed().await.ok()?;
            } else {
                self.buffered.extend(page);
            }
        }
    }
}

impl LiveFeed for InMemoryEventStore {
    /// Tails the feed from its current head, waking on the store's append signal.
    fn subscribe_live(self: Arc<Self>) -> BoxStream<'static, EventEnvelope> {
        // Subscribe to wake-ups before reading the head, so an append between
        // the two shows up as a change instead of being missed.
        let wake = self.wake();
        let after = self.lock().events.last().map(|e| e.position.clone());
        let tail = LiveTail {
            store: self,
            wake,
            after,
            buffered: VecDeque::new(),
        };
        Box::pin(stream::unfold(tail, |mut tail| async move {
            let next = tail.next().await?;
            Some((next, tail))
        }))
    }
}
