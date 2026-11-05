//! Tracking of fire-and-forget delivery tasks, independent of what they deliver.

use std::future::Future;
use std::sync::{Mutex, PoisonError};

use tokio::task::JoinHandle;

/// Runs deliveries as tracked tasks so callers can wait for them.
#[derive(Default)]
pub(crate) struct BackgroundDelivery {
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl BackgroundDelivery {
    /// Spawns `delivery` as a tracked task. Needs a Tokio runtime.
    pub(crate) fn spawn<F>(&self, delivery: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let handle = tokio::spawn(delivery);
        let mut tasks = self.tasks.lock().unwrap_or_else(PoisonError::into_inner);
        tasks.retain(|task| !task.is_finished());
        tasks.push(handle);
    }

    /// Waits for every task spawned so far.
    pub(crate) async fn flush(&self) {
        let tasks = std::mem::take(&mut *self.tasks.lock().unwrap_or_else(PoisonError::into_inner));
        for task in tasks {
            // Deliveries isolate handler panics themselves, so a join error means cancellation.
            let _ = task.await;
        }
    }
}
