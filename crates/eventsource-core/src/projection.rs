//! Projections: folding events into a read model.
//!
//! A [`Projector`] turns one event into read model changes. A
//! [`ProjectionSink`] adapts a projector to the [`EventPublisher`] sink that a
//! [`crate::subscription::SubscriptionRunner`] delivers pages to, so delivery,
//! retry and checkpointing are the runner's, not duplicated here.
//!
//! The sink writes a whole page's changes to the [`ReadModelStore`] before it
//! reports success. The runner saves the checkpoint only after that, so a
//! crash can leave the view ahead of the checkpoint but never behind it. The
//! page is then delivered again, which is why changes must be idempotent
//! upserts and deletes.

use std::collections::HashMap;
use std::future::Future;

use serde_json::Value;
use tokio::sync::Mutex;

use crate::bus::EventPublisher;
use crate::envelope::EventEnvelope;
use crate::error::{BackendError, BusError, ReadModelError};
use crate::readmodel::ReadModelStore;

/// One write to a read model.
#[derive(Debug, Clone, PartialEq)]
pub enum ReadModelChange {
    /// Insert or replace the document under `(model, id)`.
    Upsert {
        /// The read model name.
        model: String,
        /// The document id within the model.
        id: String,
        /// The new document.
        document: Value,
    },
    /// Remove the document under `(model, id)`.
    Delete {
        /// The read model name.
        model: String,
        /// The document id within the model.
        id: String,
    },
}

impl ReadModelChange {
    /// An upsert of `document` under `(model, id)`.
    #[must_use]
    pub fn upsert(model: impl Into<String>, id: impl Into<String>, document: Value) -> Self {
        Self::Upsert {
            model: model.into(),
            id: id.into(),
            document,
        }
    }

    /// A delete of `(model, id)`.
    #[must_use]
    pub fn delete(model: impl Into<String>, id: impl Into<String>) -> Self {
        Self::Delete {
            model: model.into(),
            id: id.into(),
        }
    }
}

/// What a projector sees of the read model while it handles one page.
///
/// Reads see the changes already produced for earlier events in the same page,
/// even though they are not written to the store yet.
pub struct ReadView<'a, S> {
    store: &'a S,
    pending: HashMap<(String, String), Option<Value>>,
}

impl<'a, S: ReadModelStore> ReadView<'a, S> {
    fn new(store: &'a S) -> Self {
        Self {
            store,
            pending: HashMap::new(),
        }
    }

    /// The current document under `(model, id)`, including this page's pending changes.
    ///
    /// # Errors
    /// [`ReadModelError`] if the store cannot be read.
    pub async fn get(&self, model: &str, id: &str) -> Result<Option<Value>, ReadModelError> {
        if let Some(pending) = self.pending.get(&(model.to_owned(), id.to_owned())) {
            return Ok(pending.clone());
        }
        self.store.get(model, id).await
    }

    fn record(&mut self, changes: &[ReadModelChange]) {
        for change in changes {
            match change {
                ReadModelChange::Upsert {
                    model,
                    id,
                    document,
                } => {
                    self.pending
                        .insert((model.clone(), id.clone()), Some(document.clone()));
                }
                ReadModelChange::Delete { model, id } => {
                    self.pending.insert((model.clone(), id.clone()), None);
                }
            }
        }
    }
}

/// Maps events to read model changes.
///
/// `apply` is called once per event, in feed order. It may keep state, but a
/// page that fails to write is applied again, so state that is not derived from
/// the read model must tolerate seeing an event twice.
pub trait Projector: Send + 'static {
    /// The changes `envelope` makes to the read model. Return none to ignore the event.
    ///
    /// # Errors
    /// A [`BackendError`] fails the page; the runner retries it under its retry policy.
    fn apply<S: ReadModelStore>(
        &mut self,
        envelope: &EventEnvelope,
        view: &ReadView<'_, S>,
    ) -> impl Future<Output = Result<Vec<ReadModelChange>, BackendError>> + Send;
}

/// An [`EventPublisher`] that projects each delivered page into a read model store.
pub struct ProjectionSink<S, P> {
    store: S,
    projector: Mutex<P>,
}

impl<S: ReadModelStore, P: Projector> ProjectionSink<S, P> {
    /// A sink that writes `projector`'s changes to `store`.
    pub fn new(store: S, projector: P) -> Self {
        Self {
            store,
            projector: Mutex::new(projector),
        }
    }
}

impl<S: ReadModelStore, P: Projector> EventPublisher for ProjectionSink<S, P> {
    async fn publish(&self, envelopes: &[EventEnvelope]) -> Result<(), BusError> {
        let mut projector = self.projector.lock().await;
        let mut view = ReadView::new(&self.store);
        let mut page_changes = Vec::new();
        for envelope in envelopes {
            let changes = projector
                .apply(envelope, &view)
                .await
                .map_err(BusError::Backend)?;
            view.record(&changes);
            page_changes.extend(changes);
        }
        drop(view);
        self.store
            .apply_changes(&page_changes)
            .await
            .map_err(|error| BusError::Backend(Box::new(error)))
    }
}
