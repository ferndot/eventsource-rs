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
use crate::envelope::{AppendResult, StreamReadOptions};
use crate::error::{RepositoryError, StoreError, StreamIdError};
use crate::event::{EventMeta, EventRecord};
use crate::ids::{AggregateId, CausationId, CorrelationId, EventId, TenantId};
use crate::snapshot::{NoSnapshots, SnapshotPolicy, SnapshotStore};
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
        let (mut aggregate, loaded) = match self.read_snapshot(id, &stream).await? {
            Some(aggregate) => {
                let version = aggregate.committed_version();
                (aggregate, version)
            }
            None => (AggregateRoot::new(id), Version::ZERO),
        };
        let options = StreamReadOptions {
            from: (loaded > Version::ZERO).then(|| loaded.next()),
            ..StreamReadOptions::default()
        };
        let envelopes = self.store.read_stream(&stream, options).await?;
        if envelopes.is_empty() {
            return Ok((loaded > Version::ZERO).then_some(aggregate));
        }

        let mut events = Vec::with_capacity(envelopes.len());
        for (index, envelope) in envelopes.iter().enumerate() {
            let expected = loaded.get() + u64::try_from(index).unwrap_or(u64::MAX) + 1;
            if envelope.stream != stream {
                return Err(RepositoryError::CorruptStream {
                    stream,
                    detail: format!("event belongs to stream {}", envelope.stream),
                });
            }
            if envelope.stream_version.get() != expected {
                return Err(RepositoryError::CorruptStream {
                    stream,
                    detail: format!(
                        "expected stream version {expected} at index {index}, found {}",
                        envelope.stream_version.get()
                    ),
                });
            }
            events.push(envelope.record.decode::<D::Event>()?);
        }

        aggregate.load_from_history(events);
        Ok(Some(aggregate))
    }

    /// The aggregate as of the stored snapshot, or `None` if there is none.
    async fn read_snapshot(
        &self,
        id: AggregateId,
        stream: &StreamId,
    ) -> Result<Option<AggregateRoot<D>>, RepositoryError<D::Error>> {
        let Some(config) = &self.snapshots else {
            return Ok(None);
        };
        let loaded = config
            .store
            .load(stream)
            .await
            .map_err(|e| StoreError::Backend(Box::new(e)))?;
        let Some(snapshot) = loaded else {
            return Ok(None);
        };
        let state =
            (config.decode)(snapshot.state).map_err(|e| RepositoryError::CorruptStream {
                stream: stream.clone(),
                detail: format!("snapshot state: {e}"),
            })?;
        Ok(Some(AggregateRoot::from_snapshot(
            id,
            snapshot.version,
            state,
        )))
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
        if aggregate.uncommitted_events().is_empty() {
            return Ok(None);
        }
        let records = aggregate
            .uncommitted_events()
            .iter()
            .map(|pending| EventRecord::from_event(&pending.event, pending.meta.clone()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(RepositoryError::Encode)?;

        let expected = ExpectedVersion::Exact(aggregate.committed_version());
        let result = self
            .store
            .append(&self.stream(aggregate.id()), records, expected)
            .await?;
        aggregate.mark_committed();
        Ok(Some(result))
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
        let occurred_at = self.clock.now();
        let mut stamp = || EventMeta {
            event_id: EventId::new(),
            occurred_at,
            correlation_id: context.correlation_id,
            causation_id: context.causation_id,
            tenant_id: context.tenant_id,
            actor_id: context.actor_id.clone(),
        };
        aggregate
            .execute(command, &mut stamp)
            .map(|_| ())
            .map_err(RepositoryError::Domain)
    }
}
