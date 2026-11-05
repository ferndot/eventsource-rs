//! Read model port: a queryable view kept up to date by projecting events.
//!
//! Documents are [`serde_json::Value`]s addressed by a model name and an id, so
//! the port says nothing about how an adapter stores them.

use std::future::Future;
use std::sync::Arc;

use serde_json::Value;

use crate::error::ReadModelError;

/// Durable storage for read model documents, keyed by `(model, id)`.
///
/// Writes must be idempotent: upserting the same document twice leaves the
/// same state, because a projection delivers at least once.
pub trait ReadModelStore: Send + Sync {
    /// Inserts `document` under `(model, id)`, replacing any existing one.
    fn upsert(
        &self,
        model: &str,
        id: &str,
        document: Value,
    ) -> impl Future<Output = Result<(), ReadModelError>> + Send;

    /// The document under `(model, id)`, if any.
    fn get(
        &self,
        model: &str,
        id: &str,
    ) -> impl Future<Output = Result<Option<Value>, ReadModelError>> + Send;

    /// Removes the document under `(model, id)`. Removing a missing one is not an error.
    fn delete(
        &self,
        model: &str,
        id: &str,
    ) -> impl Future<Output = Result<(), ReadModelError>> + Send;
}

impl<T: ReadModelStore> ReadModelStore for Arc<T> {
    fn upsert(
        &self,
        model: &str,
        id: &str,
        document: Value,
    ) -> impl Future<Output = Result<(), ReadModelError>> + Send {
        (**self).upsert(model, id, document)
    }

    fn get(
        &self,
        model: &str,
        id: &str,
    ) -> impl Future<Output = Result<Option<Value>, ReadModelError>> + Send {
        (**self).get(model, id)
    }

    fn delete(
        &self,
        model: &str,
        id: &str,
    ) -> impl Future<Output = Result<(), ReadModelError>> + Send {
        (**self).delete(model, id)
    }
}
