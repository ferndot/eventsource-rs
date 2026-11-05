//! Registry of known event wire names.
//!
//! Built once at startup and then shared immutably, so there is no global
//! state and no locking. Tests create their own registries.

use std::any::TypeId;
use std::collections::BTreeMap;

use crate::error::RegistryError;
use crate::event::DomainEvent;

/// Maps event wire names to the Rust event type that owns them.
#[derive(Debug, Clone, Default)]
pub struct EventRegistry {
    names: BTreeMap<&'static str, TypeId>,
}

impl EventRegistry {
    /// An empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers every wire name of `E`.
    ///
    /// Registering the same type twice is a no-op.
    ///
    /// # Errors
    /// Returns [`RegistryError::DuplicateEventType`] if a name is already owned by a different type.
    pub fn register<E: DomainEvent>(&mut self) -> Result<(), RegistryError> {
        let owner = TypeId::of::<E>();
        // Check every name before inserting any, so a conflict leaves the registry unchanged.
        if let Some(&name) = E::event_types()
            .iter()
            .find(|&&name| self.names.get(name).is_some_and(|&o| o != owner))
        {
            return Err(RegistryError::DuplicateEventType {
                name: name.to_owned(),
            });
        }
        for &name in E::event_types() {
            self.names.insert(name, owner);
        }
        Ok(())
    }

    /// Whether `name` is a registered wire name.
    #[must_use]
    pub fn contains(&self, name: &str) -> bool {
        self.names.contains_key(name)
    }

    /// All registered wire names, sorted.
    pub fn event_types(&self) -> impl Iterator<Item = &str> {
        self.names.keys().copied()
    }
}
