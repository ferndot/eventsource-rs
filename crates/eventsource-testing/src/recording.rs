//! A tracer that keeps every span in memory, for asserting on telemetry in tests.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};

use eventsource_core::telemetry::{AttrValue, Span, Tracer, attr};

/// An owned attribute value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordedValue {
    /// Text.
    Str(String),
    /// Unsigned integer.
    U64(u64),
}

impl From<AttrValue<'_>> for RecordedValue {
    fn from(value: AttrValue<'_>) -> Self {
        match value {
            AttrValue::Str(s) => Self::Str(s.to_owned()),
            AttrValue::U64(n) => Self::U64(n),
        }
    }
}

/// One span as the tracer saw it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RecordedSpan {
    /// Span name.
    pub name: String,
    /// Final attribute values by key.
    pub attributes: BTreeMap<String, RecordedValue>,
    /// The failure description, if the span was marked failed.
    pub failure: Option<String>,
}

impl RecordedSpan {
    /// The value of attribute `key`.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&RecordedValue> {
        self.attributes.get(key)
    }
}

/// Records spans in memory. Clones share one log.
#[derive(Debug, Clone, Default)]
pub struct RecordingTracer {
    spans: Arc<Mutex<Vec<RecordedSpan>>>,
}

impl RecordingTracer {
    /// An empty recorder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Every span started so far, in start order.
    #[must_use]
    pub fn spans(&self) -> Vec<RecordedSpan> {
        self.spans
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// The spans named `name`.
    #[must_use]
    pub fn spans_named(&self, name: &str) -> Vec<RecordedSpan> {
        self.spans()
            .into_iter()
            .filter(|s| s.name == name)
            .collect()
    }

    /// The one span named `name`.
    ///
    /// # Panics
    /// If there is not exactly one.
    #[must_use]
    pub fn only_span(&self, name: &str) -> RecordedSpan {
        let mut found = self.spans_named(name);
        assert_eq!(found.len(), 1, "expected one `{name}` span, got {found:?}");
        found.remove(0)
    }
}

/// A span in a [`RecordingTracer`]'s log.
#[derive(Debug)]
pub struct RecordingSpan {
    spans: Arc<Mutex<Vec<RecordedSpan>>>,
    index: usize,
}

impl RecordingSpan {
    fn with(&self, f: impl FnOnce(&mut RecordedSpan)) {
        let mut spans = self.spans.lock().unwrap_or_else(PoisonError::into_inner);
        f(&mut spans[self.index]);
    }
}

impl Span for RecordingSpan {
    fn set(&mut self, key: &'static str, value: AttrValue<'_>) {
        self.with(|span| {
            span.attributes.insert(key.to_owned(), value.into());
        });
    }

    fn fail(&mut self, description: &str) {
        self.with(|span| {
            span.failure = Some(description.to_owned());
            span.attributes.insert(
                attr::ERROR_TYPE.to_owned(),
                RecordedValue::Str(description.to_owned()),
            );
        });
    }
}

impl Tracer for RecordingTracer {
    type Span = RecordingSpan;

    fn start(
        &self,
        name: &'static str,
        attributes: &[(&'static str, AttrValue<'_>)],
    ) -> RecordingSpan {
        let mut spans = self.spans.lock().unwrap_or_else(PoisonError::into_inner);
        spans.push(RecordedSpan {
            name: name.to_owned(),
            attributes: attributes
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).into()))
                .collect(),
            failure: None,
        });
        RecordingSpan {
            spans: Arc::clone(&self.spans),
            index: spans.len() - 1,
        }
    }
}
