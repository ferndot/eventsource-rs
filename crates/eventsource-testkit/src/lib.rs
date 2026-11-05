//! Testing toolkit for eventsource domains.
//!
//! This is a separate crate from `eventsource-testing`, which holds conformance
//! suites that adapters run and which `eventsource-memory` itself dev-depends
//! on. The toolkit depends on the memory adapter, so it cannot live there.

mod builder;
mod recording;

pub use builder::EventBuilder;
pub use recording::RecordingEventBus;
mod scenario;

#[cfg(test)]
mod fixtures;
pub use scenario::{DeciderScenario, ScenarioResult};
mod harness;
pub use harness::{HarnessBus, InMemoryTestHarness};
