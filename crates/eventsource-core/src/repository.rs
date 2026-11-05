//! Aggregate repository: load, save, and execute commands against a store.
//!
//! The repository never publishes. Events reach subscribers only through a
//! [`crate::subscription::SubscriptionRunner`] reading the feed, so a save
//! succeeds or fails on the append alone.

use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::sync::Arc;

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::aggregate::AggregateRoot;
use crate::clock::{Clock, SystemClock};
use crate::decider::Decider;
use crate::envelope::{AppendResult, EventEnvelope, StreamReadOptions};
use crate::error::{AtomicAppendError, RepositoryError, StoreError, StreamIdError};
use crate::event::{EventMeta, EventRecord};
use crate::ids::{AggregateId, CausationId, CorrelationId, EventId, TenantId};
use crate::outbox::{NewOutboxMessage, OutboxAppend};
use crate::snapshot::{NoSnapshots, Snapshot, SnapshotPolicy, SnapshotStore};
use crate::store::AggregateStore;
use crate::stream_id::{Category, StreamId};
use crate::version::{ExpectedVersion, Version};

/// Who and what is issuing a command. Stamped onto every event it produces.
///
/// This is a plain value passed alongside the command, not a base class or a
/// trait the command must implement. The decider never sees it, so it stays pure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandContext {
    /// Correlation id for the events this command produces.
    pub correlation_id: CorrelationId,
    /// The event or command that caused this one, if any.
    pub causation_id: Option<CausationId>,
    /// Tenant the command runs for.
    pub tenant_id: Option<TenantId>,
    /// User or system issuing the command.
    pub actor_id: Option<String>,
}

impl CommandContext {
    /// A context with a fresh correlation id and no other provenance.
    #[must_use]
    #[allow(clippy::new_without_default)] // Deliberately no Default: it would mint a fresh id silently.
    pub fn new() -> Self {
        Self {
            correlation_id: CorrelationId::new(),
            causation_id: None,
            tenant_id: None,
            actor_id: None,
        }
    }
}

/// Snapshot wiring for a repository: the store, when to write, and how to
/// (de)serialize `D::State`. The codec is captured as function pointers when
/// snapshots are enabled, so only snapshotting repositories need a
/// serializable state.
struct SnapshotConfig<D: Decider, N> {
    store: N,
    policy: SnapshotPolicy,
    encode: fn(&D::State) -> Result<Value, serde_json::Error>,
    decode: fn(Value) -> Result<D::State, serde_json::Error>,
}

/// Loads, saves, and executes one aggregate type `D`.
///
/// Statically dispatched over the event store `S` and, when enabled with
/// [`Self::with_snapshots`], the snapshot store `N`. The event store is the
/// only place that decides whether a save wins. Snapshots are an optimisation:
/// a missing, unreadable, or mismatched snapshot means a full replay.
pub struct AggregateRepository<D: Decider, S, N = NoSnapshots> {
    store: S,
    clock: Arc<dyn Clock>,
    category: Category,
    tenant: Option<TenantId>,
    snapshots: Option<SnapshotConfig<D, N>>,
    _decider: PhantomData<fn() -> D>,
}

impl<D, S> AggregateRepository<D, S>
where
    D: Decider,
    S: AggregateStore,
{
    /// A repository over `store` with the system clock.
    ///
    /// # Errors
    /// Returns an error if `D::AGGREGATE_TYPE` is not a valid category.
    pub fn new(store: S) -> Result<Self, StreamIdError> {
        Ok(Self {
            store,
            clock: Arc::new(SystemClock),
            category: Category::new(D::AGGREGATE_TYPE)?,
            tenant: None,
            snapshots: None,
            _decider: PhantomData,
        })
    }

    /// Turns on snapshots: loads start from the latest usable snapshot in
    /// `snapshots`, and saves write one whenever `policy` says a boundary was crossed.
    #[must_use]
    pub fn with_snapshots<N: SnapshotStore>(
        self,
        snapshots: N,
        policy: SnapshotPolicy,
    ) -> AggregateRepository<D, S, N>
    where
        D::State: Serialize + DeserializeOwned,
    {
        AggregateRepository {
            store: self.store,
            clock: self.clock,
            category: self.category,
            tenant: self.tenant,
            snapshots: Some(SnapshotConfig {
                store: snapshots,
                policy,
                encode: |state| serde_json::to_value(state),
                decode: |value| serde_json::from_value(value),
            }),
            _decider: PhantomData,
        }
    }
}

impl<D, S, N> AggregateRepository<D, S, N>
where
    D: Decider,
    S: AggregateStore,
    N: SnapshotStore,
{
    /// Replaces the clock used to stamp `occurred_at`. Tests use this for fixed times.
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// Binds the repository to one tenant.
    ///
    /// Streams are namespaced by tenant, so the same aggregate id under two
    /// tenants is two independent streams, and a scoped repository cannot see
    /// another tenant's aggregates (or unscoped ones).
    ///
    /// # Errors
    /// Returns an error if the tenant-qualified category is not valid.
    pub fn with_tenant(mut self, tenant: TenantId) -> Result<Self, StreamIdError> {
        self.category = Category::new(format!("{tenant}.{}", D::AGGREGATE_TYPE))?;
        self.tenant = Some(tenant);
        Ok(self)
    }

    fn stream(&self, id: AggregateId) -> StreamId {
        StreamId::new(id, self.category.clone())
    }

    /// Loads an aggregate, or `None` if it has no events.
    ///
    /// The history is checked as it is read: stream versions must run 1..=n
    /// without gaps, every event must belong to this stream, and each stored
    /// event type must match the variant its payload decodes to.
    ///
    /// # Errors
    /// [`RepositoryError::CorruptStream`] if the history is inconsistent,
    /// [`RepositoryError::Decode`] if a payload is not a valid `D::Event`,
    /// or a store error if the read fails.
    pub async fn get(
        &self,
        id: AggregateId,
    ) -> Result<Option<AggregateRoot<D>>, RepositoryError<D::Error>> {
        let stream = self.stream(id);
        if let Some(aggregate) = self.read_snapshot(id, &stream).await {
            let at = aggregate.committed_version();
            let envelopes = self.read_after(&stream, at).await?;
            // A snapshot beyond the end of the stream cannot be right.
            if !envelopes.is_empty() || self.store.stream_version(&stream).await? >= at {
                return Self::replay(&stream, aggregate, &envelopes).map(Some);
            }
        }
        let envelopes = self.read_after(&stream, Version::ZERO).await?;
        if envelopes.is_empty() {
            return Ok(None);
        }
        Self::replay(&stream, AggregateRoot::new(id), &envelopes).map(Some)
    }

    /// The events of `stream` after version `after`.
    async fn read_after(
        &self,
        stream: &StreamId,
        after: Version,
    ) -> Result<Vec<EventEnvelope>, StoreError> {
        let options = StreamReadOptions {
            from: (after > Version::ZERO).then(|| after.next()),
            ..StreamReadOptions::default()
        };
        self.store.read_stream(stream, options).await
    }

    /// Checks `envelopes` continue `aggregate`'s history, then applies them.
    fn replay(
        stream: &StreamId,
        mut aggregate: AggregateRoot<D>,
        envelopes: &[EventEnvelope],
    ) -> Result<AggregateRoot<D>, RepositoryError<D::Error>> {
        let loaded = aggregate.committed_version().get();
        let mut events = Vec::with_capacity(envelopes.len());
        for (index, envelope) in envelopes.iter().enumerate() {
            let expected = loaded + u64::try_from(index).unwrap_or(u64::MAX) + 1;
            if envelope.stream != *stream {
                return Err(RepositoryError::CorruptStream {
                    stream: stream.clone(),
                    detail: format!("event belongs to stream {}", envelope.stream),
                });
            }
            if envelope.stream_version.get() != expected {
                return Err(RepositoryError::CorruptStream {
                    stream: stream.clone(),
                    detail: format!(
                        "expected stream version {expected} at index {index}, found {}",
                        envelope.stream_version.get()
                    ),
                });
            }
            events.push(envelope.record.decode::<D::Event>()?);
        }
        aggregate.load_from_history(events);
        Ok(aggregate)
    }

    /// The aggregate as of the stored snapshot, or `None` to replay from the start.
    ///
    /// Every way a snapshot can be unusable (absent, unreadable, undecodable)
    /// is the same answer: the events are the truth, so replay them.
    async fn read_snapshot(&self, id: AggregateId, stream: &StreamId) -> Option<AggregateRoot<D>> {
        let config = self.snapshots.as_ref()?;
        let snapshot = config.store.load(stream).await.ok()??;
        if snapshot.version == Version::ZERO || snapshot.schema_version != D::state_schema_version()
        {
            return None;
        }
        let state = (config.decode)(snapshot.state).ok()?;
        Some(AggregateRoot::from_snapshot(id, snapshot.version, state))
    }

    /// Loads an aggregate, or a new empty one if none exists.
    ///
    /// # Errors
    /// Same as [`Self::get`].
    pub async fn load_or_new(
        &self,
        id: AggregateId,
    ) -> Result<AggregateRoot<D>, RepositoryError<D::Error>> {
        Ok(self
            .get(id)
            .await?
            .unwrap_or_else(|| AggregateRoot::new(id)))
    }

    /// Loads an aggregate.
    ///
    /// # Errors
    /// [`RepositoryError::NotFound`] if the aggregate has no events; otherwise as [`Self::get`].
    pub async fn load(
        &self,
        id: AggregateId,
    ) -> Result<AggregateRoot<D>, RepositoryError<D::Error>> {
        self.get(id).await?.ok_or(RepositoryError::NotFound { id })
    }

    /// Persists an aggregate's uncommitted events.
    ///
    /// The append is the commit point. The save uses the aggregate's committed
    /// version as its expectation, so a stale copy is rejected rather than
    /// overwriting newer events. Each event is written with the metadata it was
    /// stamped with at execute time, so a failed save can be retried safely.
    ///
    /// Returns `None` when there was nothing to save.
    ///
    /// # Errors
    /// [`StoreError::ConcurrencyConflict`] (wrapped) if another writer got there first.
    pub async fn save(
        &self,
        aggregate: &mut AggregateRoot<D>,
    ) -> Result<Option<AppendResult>, RepositoryError<D::Error>> {
        let Some(records) = Self::pending_records(aggregate)? else {
            return Ok(None);
        };
        let previous = aggregate.committed_version();
        let expected = ExpectedVersion::Exact(previous);
        let result = self
            .store
            .append(&self.stream(aggregate.id()), records, expected)
            .await?;
        self.committed(aggregate, previous).await?;
        Ok(Some(result))
    }

    /// The aggregate's uncommitted events as records, or `None` if there are none.
    fn pending_records(
        aggregate: &AggregateRoot<D>,
    ) -> Result<Option<Vec<EventRecord>>, RepositoryError<D::Error>> {
        if aggregate.uncommitted_events().is_empty() {
            return Ok(None);
        }
        aggregate
            .uncommitted_events()
            .iter()
            .map(|pending| EventRecord::from_event(&pending.event, pending.meta.clone()))
            .collect::<Result<Vec<_>, _>>()
            .map(Some)
            .map_err(RepositoryError::Encode)
    }

    /// Bookkeeping after the events of a save are stored.
    async fn committed(
        &self,
        aggregate: &mut AggregateRoot<D>,
        previous: Version,
    ) -> Result<(), RepositoryError<D::Error>> {
        aggregate.mark_committed();
        self.write_snapshot(aggregate, previous).await
    }

    /// Snapshots `aggregate` if the save that took it from `previous` crossed a policy boundary.
    async fn write_snapshot(
        &self,
        aggregate: &AggregateRoot<D>,
        previous: Version,
    ) -> Result<(), RepositoryError<D::Error>> {
        let Some(config) = &self.snapshots else {
            return Ok(());
        };
        if !config.policy.crossed(previous, aggregate.version()) {
            return Ok(());
        }
        let state = (config.encode)(aggregate.state()).map_err(RepositoryError::Encode)?;
        let snapshot = Snapshot {
            stream: self.stream(aggregate.id()),
            version: aggregate.version(),
            schema_version: D::state_schema_version(),
            state,
            created_at: self.clock.now(),
        };
        // The events are already committed and the snapshot is only a cache,
        // so a failed write must not fail the save. The next boundary retries.
        let _ = config.store.save(snapshot).await;
        Ok(())
    }

    /// Loads (or creates), runs one command, and saves.
    ///
    /// # Errors
    /// [`RepositoryError::Domain`] if the decider rejects the command, otherwise as [`Self::save`].
    pub async fn execute(
        &self,
        id: AggregateId,
        command: D::Command,
        context: &CommandContext,
    ) -> Result<Option<AppendResult>, RepositoryError<D::Error>> {
        let mut aggregate = self.load_or_new(id).await?;
        self.run(&mut aggregate, command, context)?;
        self.save(&mut aggregate).await
    }

    /// Like [`Self::execute`], but retries from a fresh load when another writer wins the race.
    ///
    /// `make_command` is called again for each attempt, because a command is
    /// consumed by value. Only concurrency conflicts are retried. Other errors
    /// return immediately.
    ///
    /// # Errors
    /// The last error if every attempt conflicts, or any non-conflict error at once.
    pub async fn execute_with_retry(
        &self,
        id: AggregateId,
        mut make_command: impl FnMut() -> D::Command,
        context: &CommandContext,
        attempts: NonZeroUsize,
    ) -> Result<Option<AppendResult>, RepositoryError<D::Error>> {
        let mut remaining = attempts.get();
        loop {
            match self.execute(id, make_command(), context).await {
                Err(RepositoryError::Store(StoreError::ConcurrencyConflict { .. }))
                    if remaining > 1 =>
                {
                    remaining -= 1;
                }
                result => return result,
            }
        }
    }

    /// The version currently stored for `id`; zero if it does not exist.
    ///
    /// # Errors
    /// Returns a store error if the read fails.
    pub async fn stored_version(
        &self,
        id: AggregateId,
    ) -> Result<Version, RepositoryError<D::Error>> {
        Ok(self.store.stream_version(&self.stream(id)).await?)
    }

    /// Runs a command on an in-memory aggregate, stamping its events with `context`.
    fn run(
        &self,
        aggregate: &mut AggregateRoot<D>,
        command: D::Command,
        context: &CommandContext,
    ) -> Result<(), RepositoryError<D::Error>> {
        if let (Some(scope), Some(named)) = (self.tenant, context.tenant_id)
            && scope != named
        {
            return Err(RepositoryError::TenantMismatch {
                scope,
                context: named,
            });
        }
        let occurred_at = self.clock.now();
        let mut stamp = || EventMeta {
            event_id: EventId::new(),
            occurred_at,
            correlation_id: context.correlation_id,
            causation_id: context.causation_id,
            tenant_id: self.tenant.or(context.tenant_id),
            actor_id: context.actor_id.clone(),
        };
        aggregate
            .execute(command, &mut stamp)
            .map(|_| ())
            .map_err(RepositoryError::Domain)
    }
}

impl<D, S, N> AggregateRepository<D, S, N>
where
    D: Decider,
    S: OutboxAppend,
    N: SnapshotStore,
{
    /// Like [`Self::save`], but also enqueues one outbox message per event, in the same atomic step.
    ///
    /// `to_message` is called once for each event, in order. If any message is
    /// rejected or cannot be stored, no event is stored either and the
    /// aggregate keeps its uncommitted events, so the save can be retried.
    ///
    /// # Errors
    /// [`RepositoryError::Outbox`] if a message was rejected, otherwise as [`Self::save`].
    pub async fn save_with_outbox(
        &self,
        aggregate: &mut AggregateRoot<D>,
        to_message: impl Fn(&EventRecord) -> NewOutboxMessage + Send,
    ) -> Result<Option<AppendResult>, RepositoryError<D::Error>> {
        let Some(records) = Self::pending_records(aggregate)? else {
            return Ok(None);
        };
        let stream = self.stream(aggregate.id());
        let messages = records
            .iter()
            .map(|record| to_message(record).for_stream(stream.clone()))
            .collect();
        let previous = aggregate.committed_version();
        let expected = ExpectedVersion::Exact(previous);
        let result = self
            .store
            .append_with_outbox(&stream, records, expected, messages)
            .await
            .map_err(|error| match error {
                AtomicAppendError::Store(error) => RepositoryError::Store(error),
                AtomicAppendError::Outbox(error) => RepositoryError::Outbox(error),
            })?;
        self.committed(aggregate, previous).await?;
        Ok(Some(result))
    }

    /// Like [`Self::execute`], but saves with [`Self::save_with_outbox`].
    ///
    /// # Errors
    /// [`RepositoryError::Domain`] if the decider rejects the command, otherwise as [`Self::save_with_outbox`].
    pub async fn execute_with_outbox(
        &self,
        id: AggregateId,
        command: D::Command,
        context: &CommandContext,
        to_message: impl Fn(&EventRecord) -> NewOutboxMessage + Send,
    ) -> Result<Option<AppendResult>, RepositoryError<D::Error>> {
        let mut aggregate = self.load_or_new(id).await?;
        self.run(&mut aggregate, command, context)?;
        self.save_with_outbox(&mut aggregate, to_message).await
    }
}
