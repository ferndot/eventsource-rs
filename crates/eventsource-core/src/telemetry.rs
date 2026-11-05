//! Tracing port: spans around appends, loads, publishes and subscription pages.
//!
//! Adapters implement [`Tracer`]. Instrumented code is generic over the tracer,
//! defaulting to [`NoopTracer`], so a build without telemetry pays nothing:
//! the no-op span is a zero-sized type, every method is empty, and call sites
//! skip building attributes when [`Tracer::ENABLED`] is `false`.
//!
//! Spans carry identifiers and counts only. Event payloads and metadata are
//! never passed to a tracer.

/// Attribute names, following the Python reference where one exists.
pub mod attr {
    /// Full stream id, `"{aggregate_id}:{category}"`.
    pub const STREAM_ID: &str = "eventsource.stream.id";
    /// Stream category (aggregate type name).
    pub const AGGREGATE_TYPE: &str = "eventsource.aggregate.type";
    /// Number of events in the operation.
    pub const EVENT_COUNT: &str = "eventsource.event.count";
    /// Stream version after (append) or at the time of (load) the operation.
    pub const VERSION: &str = "eventsource.version";
    /// Global store position token of the newest event involved.
    pub const POSITION: &str = "eventsource.position";
    /// Name of the subscription.
    pub const SUBSCRIPTION_NAME: &str = "eventsource.subscription.name";
    /// Number of events in a delivered page.
    pub const BATCH_SIZE: &str = "eventsource.batch.size";
    /// Number of failed attempts or handlers in the operation.
    pub const FAILURE_COUNT: &str = "eventsource.failure.count";
    /// Description of the error that ended the span, if any.
    pub const ERROR_TYPE: &str = "eventsource.error.type";
}

/// Span names.
pub mod span {
    /// An append to a stream.
    pub const APPEND: &str = "eventsource.append";
    /// A read of one stream.
    pub const LOAD: &str = "eventsource.load";
    /// A publish of events to handlers.
    pub const PUBLISH: &str = "eventsource.publish";
    /// One page delivered by a subscription runner.
    pub const SUBSCRIPTION_PAGE: &str = "eventsource.subscription.page";
}

/// A span attribute value. Strings are borrowed, so building one does not allocate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttrValue<'a> {
    /// Text.
    Str(&'a str),
    /// Unsigned integer: counts, versions, positions.
    U64(u64),
}

impl From<usize> for AttrValue<'_> {
    fn from(value: usize) -> Self {
        Self::U64(u64::try_from(value).unwrap_or(u64::MAX))
    }
}

impl From<u64> for AttrValue<'_> {
    fn from(value: u64) -> Self {
        Self::U64(value)
    }
}

impl<'a> From<&'a str> for AttrValue<'a> {
    fn from(value: &'a str) -> Self {
        Self::Str(value)
    }
}

/// A started span. It ends when dropped.
pub trait Span: Send {
    /// Sets or overwrites an attribute.
    fn set(&mut self, key: &'static str, value: AttrValue<'_>);

    /// Marks the span as failed with a short description. Never pass payload data.
    fn fail(&mut self, description: &str);
}

/// Creates spans.
pub trait Tracer: Send + Sync {
    /// `false` lets instrumented code skip building attributes entirely.
    const ENABLED: bool = true;

    /// The span type this tracer hands out.
    type Span: Span;

    /// Starts a span named `name` with initial `attributes`.
    fn start(&self, name: &'static str, attributes: &[(&'static str, AttrValue<'_>)])
    -> Self::Span;
}

/// A tracer that records nothing. The default everywhere.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopTracer;

/// The span of a [`NoopTracer`].
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopSpan;

impl Span for NoopSpan {
    #[inline]
    fn set(&mut self, _key: &'static str, _value: AttrValue<'_>) {}

    #[inline]
    fn fail(&mut self, _description: &str) {}
}

impl Tracer for NoopTracer {
    const ENABLED: bool = false;
    type Span = NoopSpan;

    #[inline]
    fn start(&self, _name: &'static str, _attributes: &[(&'static str, AttrValue<'_>)]) -> NoopSpan {
        NoopSpan
    }
}

impl<T: Tracer> Tracer for std::sync::Arc<T> {
    const ENABLED: bool = T::ENABLED;
    type Span = T::Span;

    fn start(&self, name: &'static str, attributes: &[(&'static str, AttrValue<'_>)]) -> T::Span {
        (**self).start(name, attributes)
    }
}
