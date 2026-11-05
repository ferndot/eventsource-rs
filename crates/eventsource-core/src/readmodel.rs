//! Read model port: a queryable view kept up to date by projecting events.
//!
//! Documents are [`serde_json::Value`]s addressed by a model name and an id, so
//! the port says nothing about how an adapter stores them.

use std::future::Future;
use std::sync::Arc;

use serde_json::Value;

use crate::error::ReadModelError;
use crate::projection::ReadModelChange;

/// A stored document together with its id and bookkeeping.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredDocument {
    /// The id the document is stored under.
    pub id: String,
    /// How many writes the document has seen; the first write makes it 1.
    pub version: u64,
    /// Whether the document is soft-deleted.
    pub deleted: bool,
    /// The document itself.
    pub document: Value,
}

/// A read of one model: equality filters on top-level fields, paging, ordered by id.
///
/// A document matches when it is an object that has every filtered field equal
/// to the given value. A missing field never matches, not even a `null` filter.
/// Soft-deleted documents are left out unless [`include_deleted`](Self::include_deleted)
/// is set.
#[derive(Debug, Clone, PartialEq)]
pub struct ReadModelQuery {
    /// The model to read.
    pub model: String,
    /// `(field, value)` pairs, all of which must match.
    pub filters: Vec<(String, Value)>,
    /// The most documents to return; `None` is no limit.
    pub limit: Option<usize>,
    /// How many matching documents to skip, in id order.
    pub offset: usize,
    /// Whether soft-deleted documents take part.
    pub include_deleted: bool,
}

impl ReadModelQuery {
    /// A query for every live document of `model`.
    #[must_use]
    pub fn new(model: impl Into<String>) -> Self {
        Self {
            model: model.into(),
            filters: Vec::new(),
            limit: None,
            offset: 0,
            include_deleted: false,
        }
    }

    /// Also requires top-level `field` to equal `value`.
    #[must_use]
    pub fn filter(mut self, field: impl Into<String>, value: Value) -> Self {
        self.filters.push((field.into(), value));
        self
    }

    /// Returns at most `limit` documents.
    #[must_use]
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Skips the first `offset` matches.
    #[must_use]
    pub fn offset(mut self, offset: usize) -> Self {
        self.offset = offset;
        self
    }

    /// Lets soft-deleted documents match too.
    #[must_use]
    pub fn include_deleted(mut self) -> Self {
        self.include_deleted = true;
        self
    }

    /// Whether `document` satisfies every filter.
    #[must_use]
    pub fn matches(&self, document: &Value) -> bool {
        self.filters
            .iter()
            .all(|(field, expected)| document.get(field) == Some(expected))
    }
}

/// Durable storage for read model documents, keyed by `(model, id)`.
///
/// Writes must be idempotent: upserting the same document twice leaves the
/// same state, because a projection delivers at least once.
pub trait ReadModelStore: Send + Sync {
    /// Inserts `document` under `(model, id)`, replacing any existing one, and
    /// increments its version. A soft-deleted document stays soft-deleted.
    fn upsert(
        &self,
        model: &str,
        id: &str,
        document: Value,
    ) -> impl Future<Output = Result<(), ReadModelError>> + Send;

    /// The live document under `(model, id)`, if any. Soft-deleted ones are hidden;
    /// use [`fetch`](Self::fetch) to see them.
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

    /// Writes `document` only if the stored version equals `expected_version`,
    /// and returns the new version. Version 0 means "absent", so it creates.
    ///
    /// Soft-deleted documents exist for this purpose and keep their deleted flag.
    ///
    /// # Errors
    /// [`ReadModelError::VersionConflict`] if the stored version differs (0 when
    /// the document is absent); the document is left unchanged.
    fn put_if_version(
        &self,
        model: &str,
        id: &str,
        document: Value,
        expected_version: u64,
    ) -> impl Future<Output = Result<u64, ReadModelError>> + Send;

    /// The stored form of `(model, id)`, soft-deleted or not.
    fn fetch(
        &self,
        model: &str,
        id: &str,
    ) -> impl Future<Output = Result<Option<StoredDocument>, ReadModelError>> + Send;

    /// Marks the document deleted, hiding it from [`get`](Self::get) and from
    /// queries that do not include deleted documents.
    ///
    /// Returns whether a live document was marked: `false` if it is missing or
    /// already soft-deleted.
    fn soft_delete(
        &self,
        model: &str,
        id: &str,
    ) -> impl Future<Output = Result<bool, ReadModelError>> + Send;

    /// Brings a soft-deleted document back.
    ///
    /// Returns whether one was restored: `false` if it is missing or not deleted.
    fn restore(
        &self,
        model: &str,
        id: &str,
    ) -> impl Future<Output = Result<bool, ReadModelError>> + Send;

    /// Every document of `model`, soft-deleted ones included, ordered by id
    /// (byte-wise on the UTF-8 id).
    fn list(
        &self,
        model: &str,
    ) -> impl Future<Output = Result<Vec<StoredDocument>, ReadModelError>> + Send;

    /// The documents matching `query`, ordered by id, after offset and limit.
    ///
    /// The default scans [`list`](Self::list). Adapters may push the work down.
    fn query(
        &self,
        query: &ReadModelQuery,
    ) -> impl Future<Output = Result<Vec<StoredDocument>, ReadModelError>> + Send {
        async move {
            Ok(self
                .list(&query.model)
                .await?
                .into_iter()
                .filter(|doc| {
                    (query.include_deleted || !doc.deleted) && query.matches(&doc.document)
                })
                .skip(query.offset)
                .take(query.limit.unwrap_or(usize::MAX))
                .collect())
        }
    }

    /// How many documents match `query`, ignoring its limit and offset.
    fn count(
        &self,
        query: &ReadModelQuery,
    ) -> impl Future<Output = Result<usize, ReadModelError>> + Send {
        async move {
            Ok(self
                .list(&query.model)
                .await?
                .iter()
                .filter(|doc| {
                    (query.include_deleted || !doc.deleted) && query.matches(&doc.document)
                })
                .count())
        }
    }

    /// Writes `changes` in order.
    ///
    /// The default applies them one by one. Adapters with transactions should
    /// override it so a page's changes land together or not at all.
    fn apply_changes(
        &self,
        changes: &[ReadModelChange],
    ) -> impl Future<Output = Result<(), ReadModelError>> + Send {
        async move {
            for change in changes {
                match change {
                    ReadModelChange::Upsert {
                        model,
                        id,
                        document,
                    } => self.upsert(model, id, document.clone()).await?,
                    ReadModelChange::Delete { model, id } => self.delete(model, id).await?,
                }
            }
            Ok(())
        }
    }
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

    fn put_if_version(
        &self,
        model: &str,
        id: &str,
        document: Value,
        expected_version: u64,
    ) -> impl Future<Output = Result<u64, ReadModelError>> + Send {
        (**self).put_if_version(model, id, document, expected_version)
    }

    fn fetch(
        &self,
        model: &str,
        id: &str,
    ) -> impl Future<Output = Result<Option<StoredDocument>, ReadModelError>> + Send {
        (**self).fetch(model, id)
    }

    fn soft_delete(
        &self,
        model: &str,
        id: &str,
    ) -> impl Future<Output = Result<bool, ReadModelError>> + Send {
        (**self).soft_delete(model, id)
    }

    fn restore(
        &self,
        model: &str,
        id: &str,
    ) -> impl Future<Output = Result<bool, ReadModelError>> + Send {
        (**self).restore(model, id)
    }

    fn list(
        &self,
        model: &str,
    ) -> impl Future<Output = Result<Vec<StoredDocument>, ReadModelError>> + Send {
        (**self).list(model)
    }

    fn query(
        &self,
        query: &ReadModelQuery,
    ) -> impl Future<Output = Result<Vec<StoredDocument>, ReadModelError>> + Send {
        (**self).query(query)
    }

    fn count(
        &self,
        query: &ReadModelQuery,
    ) -> impl Future<Output = Result<usize, ReadModelError>> + Send {
        (**self).count(query)
    }

    fn apply_changes(
        &self,
        changes: &[ReadModelChange],
    ) -> impl Future<Output = Result<(), ReadModelError>> + Send {
        (**self).apply_changes(changes)
    }
}
