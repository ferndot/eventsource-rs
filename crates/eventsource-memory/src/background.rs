//! Tracking of fire-and-forget delivery tasks, independent of what they deliver.

use std::future::Future;
use std::sync::{Mutex, MutexGuard, PoisonError};

use eventsource_core::bus::HandlerFailure;
use eventsource_core::error::BusError;
use tokio::task::JoinHandle;

/// Runs deliveries as tracked tasks and keeps the failures they report.
#[derive(Default)]
pub(crate) struct BackgroundDelivery {
    tasks: Mutex<Vec<JoinHandle<()>>>,
    failures: Mutex<Vec<HandlerFailure>>,
}

impl BackgroundDelivery {
    /// Spawns `delivery`, recording handler failures it returns. Needs a Tokio runtime.
    pub(crate) fn spawn<F>(self: &std::sync::Arc<Self>, delivery: F)
    where
        F: Future<Output = Result<(), BusError>> + Send + 'static,
    {
        let this = std::sync::Arc::clone(self);
        let handle = tokio::spawn(async move {
            if let Err(BusError::HandlerFailures(mut failed)) = delivery.await {
                lock(&this.failures).append(&mut failed);
            }
        });
        let mut tasks = lock(&self.tasks);
        tasks.retain(|task| !task.is_finished());
        tasks.push(handle);
    }

    /// Waits for every task spawned so far.
    pub(crate) async fn flush(&self) {
        let tasks = std::mem::take(&mut *lock(&self.tasks));
        for task in tasks {
            // Panics are caught inside deliveries, so a join error means cancellation only.
            let _ = task.await;
        }
    }

    /// Takes the recorded failures.
    pub(crate) fn take_failures(&self) -> Vec<HandlerFailure> {
        std::mem::take(&mut *lock(&self.failures))
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}
