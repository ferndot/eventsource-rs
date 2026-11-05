//! In-memory read model store.

use std::collections::{BTreeMap, btree_map};
use std::sync::{Mutex, PoisonError};

use eventsource_core::error::ReadModelError;
use eventsource_core::readmodel::{ReadModelStore, StoredDocument};
use serde_json::Value;

struct Entry {
    document: Value,
    version: u64,
    deleted: bool,
}

impl Entry {
    fn new(document: Value) -> Self {
        Self {
            document,
            version: 1,
            deleted: false,
        }
    }

    fn stored(&self, id: &str) -> StoredDocument {
        StoredDocument {
            id: id.to_owned(),
            version: self.version,
            deleted: self.deleted,
            document: self.document.clone(),
        }
    }
}

/// A [`ReadModelStore`] that keeps documents in process memory.
#[derive(Default)]
pub struct InMemoryReadModelStore {
    documents: Mutex<BTreeMap<(String, String), Entry>>,
}

impl InMemoryReadModelStore {
    /// An empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Flips the deleted flag; `false` if the document is missing or already in that state.
    fn set_deleted(&self, model: &str, id: &str, deleted: bool) -> bool {
        let mut documents = self
            .documents
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        match documents.get_mut(&Self::key(model, id)) {
            Some(entry) if entry.deleted != deleted => {
                entry.deleted = deleted;
                entry.version += 1;
                true
            }
            _ => false,
        }
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
        match documents.entry(Self::key(model, id)) {
            btree_map::Entry::Occupied(mut slot) => {
                let entry = slot.get_mut();
                entry.document = document;
                entry.version += 1;
            }
            btree_map::Entry::Vacant(slot) => {
                slot.insert(Entry::new(document));
            }
        }
        Ok(())
    }

    async fn get(&self, model: &str, id: &str) -> Result<Option<Value>, ReadModelError> {
        let documents = self
            .documents
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        Ok(documents
            .get(&Self::key(model, id))
            .filter(|entry| !entry.deleted)
            .map(|entry| entry.document.clone()))
    }

    async fn delete(&self, model: &str, id: &str) -> Result<(), ReadModelError> {
        let mut documents = self
            .documents
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        documents.remove(&Self::key(model, id));
        Ok(())
    }

    async fn put_if_version(
        &self,
        model: &str,
        id: &str,
        document: Value,
        expected_version: u64,
    ) -> Result<u64, ReadModelError> {
        let mut documents = self
            .documents
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let actual = documents
            .get(&Self::key(model, id))
            .map_or(0, |entry| entry.version);
        if actual != expected_version {
            return Err(ReadModelError::VersionConflict {
                model: model.to_owned(),
                id: id.to_owned(),
                expected: expected_version,
                actual,
            });
        }
        match documents.entry(Self::key(model, id)) {
            btree_map::Entry::Occupied(mut slot) => {
                let entry = slot.get_mut();
                entry.document = document;
                entry.version += 1;
                Ok(entry.version)
            }
            btree_map::Entry::Vacant(slot) => Ok(slot.insert(Entry::new(document)).version),
        }
    }

    async fn fetch(&self, model: &str, id: &str) -> Result<Option<StoredDocument>, ReadModelError> {
        let documents = self
            .documents
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        Ok(documents
            .get(&Self::key(model, id))
            .map(|entry| entry.stored(id)))
    }

    async fn soft_delete(&self, model: &str, id: &str) -> Result<bool, ReadModelError> {
        Ok(self.set_deleted(model, id, true))
    }

    async fn restore(&self, model: &str, id: &str) -> Result<bool, ReadModelError> {
        Ok(self.set_deleted(model, id, false))
    }

    async fn list(&self, model: &str) -> Result<Vec<StoredDocument>, ReadModelError> {
        let documents = self
            .documents
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        Ok(documents
            .iter()
            .filter(|((m, _), _)| m == model)
            .map(|((_, id), entry)| entry.stored(id))
            .collect())
    }
}
