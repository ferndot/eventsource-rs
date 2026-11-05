//! In-memory read model store.

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};

use eventsource_core::error::ReadModelError;
use eventsource_core::readmodel::ReadModelStore;
use serde_json::Value;

/// A [`ReadModelStore`] that keeps documents in process memory.
#[derive(Default)]
pub struct InMemoryReadModelStore {
    documents: Mutex<HashMap<(String, String), Value>>,
}

impl InMemoryReadModelStore {
    /// An empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn key(model: &str, id: &str) -> (String, String) {
        (model.to_owned(), id.to_owned())
    }
}

impl ReadModelStore for InMemoryReadModelStore {
    async fn upsert(&self, model: &str, id: &str, document: Value) -> Result<(), ReadModelError> {
        let mut documents = self
            .documents
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        documents.insert(Self::key(model, id), document);
        Ok(())
    }

    async fn get(&self, model: &str, id: &str) -> Result<Option<Value>, ReadModelError> {
        let documents = self
            .documents
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        Ok(documents.get(&Self::key(model, id)).cloned())
    }

    async fn delete(&self, model: &str, id: &str) -> Result<(), ReadModelError> {
        let mut documents = self
            .documents
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        documents.remove(&Self::key(model, id));
        Ok(())
    }
}
