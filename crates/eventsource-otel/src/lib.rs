//! Tracer adapter that emits eventsource spans through the [`tracing`] crate.
//!
//! This crate does not depend on the OpenTelemetry SDK. It emits ordinary
//! `tracing` spans using the OpenTelemetry field conventions (`otel.name`,
//! `otel.status_code`), so an application that installs the
//! `tracing-opentelemetry` layer gets real OpenTelemetry spans, and one that
//! installs only `tracing-subscriber`'s `fmt` layer gets log lines. With no
//! subscriber installed the spans are disabled and cost a few atomic loads.
//!
//! ```
//! use eventsource_memory::InMemoryEventStore;
//! use eventsource_otel::TracingTracer;
//! let store = InMemoryEventStore::new().with_tracer(TracingTracer::new());
//! # let _ = store;
//! ```

use eventsource_core::telemetry::{self, AttrValue, Tracer, attr};

/// A [`Tracer`] that opens a `tracing` span per operation.
#[derive(Debug, Clone, Copy, Default)]
pub struct TracingTracer;

impl TracingTracer {
    /// A tracer that emits to the current `tracing` dispatcher.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

/// A `tracing` span that ends when dropped.
#[derive(Debug)]
pub struct TracingSpan(tracing::Span);

/// Declares a span with every eventsource field present but empty, so later
/// `record` calls have somewhere to land. `tracing` fixes a span's fields at
/// its call site, which is why the names are matched below, not passed through.
macro_rules! declared {
    ($name:literal) => {
        tracing::info_span!(
            $name,
            otel.name = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
            eventsource.stream.id = tracing::field::Empty,
            eventsource.aggregate.type = tracing::field::Empty,
            eventsource.event.count = tracing::field::Empty,
            eventsource.version = tracing::field::Empty,
            eventsource.position = tracing::field::Empty,
            eventsource.subscription.name = tracing::field::Empty,
            eventsource.batch.size = tracing::field::Empty,
            eventsource.failure.count = tracing::field::Empty,
            eventsource.error.type = tracing::field::Empty,
        )
    };
}

fn open(name: &'static str) -> tracing::Span {
    match name {
        telemetry::span::APPEND => declared!("eventsource.append"),
        telemetry::span::LOAD => declared!("eventsource.load"),
        telemetry::span::PUBLISH => declared!("eventsource.publish"),
        telemetry::span::SUBSCRIPTION_PAGE => declared!("eventsource.subscription.page"),
        other => {
            let span = declared!("eventsource.operation");
            span.record("otel.name", other);
            span
        }
    }
}

fn record(span: &tracing::Span, key: &'static str, value: AttrValue<'_>) {
    match value {
        AttrValue::Str(s) => span.record(key, s),
        AttrValue::U64(n) => span.record(key, n),
    };
}

impl telemetry::Span for TracingSpan {
    fn set(&mut self, key: &'static str, value: AttrValue<'_>) {
        record(&self.0, key, value);
    }

    fn fail(&mut self, description: &str) {
        self.0.record("otel.status_code", "ERROR");
        self.0.record(attr::ERROR_TYPE, description);
    }
}

impl Tracer for TracingTracer {
    type Span = TracingSpan;

    fn start(
        &self,
        name: &'static str,
        attributes: &[(&'static str, AttrValue<'_>)],
    ) -> TracingSpan {
        let span = open(name);
        for (key, value) in attributes {
            record(&span, key, *value);
        }
        TracingSpan(span)
    }
}
