//! The adapter's spans, as a `tracing` subscriber sees them.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use eventsource_core::telemetry::{AttrValue, Span, Tracer, attr, span};
use eventsource_otel::TracingTracer;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Subscriber, subscriber};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{Layer, Registry};

#[derive(Debug, Default, Clone)]
struct Seen {
    name: String,
    fields: BTreeMap<String, String>,
    closed: bool,
}

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<Seen>>>);

struct Fields<'a>(&'a mut BTreeMap<String, String>);

impl Visit for Fields<'_> {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_owned(), value.to_owned());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.0.insert(field.name().to_owned(), format!("{value:?}"));
    }
}

impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for Capture {
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let mut seen = Seen {
            name: attrs.metadata().name().to_owned(),
            ..Seen::default()
        };
        attrs.record(&mut Fields(&mut seen.fields));
        let mut all = self.0.lock().expect("lock");
        all.push(seen);
        let index = all.len() - 1;
        ctx.span(id)
            .expect("span exists")
            .extensions_mut()
            .insert(index);
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        let index = *ctx
            .span(id)
            .expect("span exists")
            .extensions()
            .get::<usize>()
            .expect("indexed");
        let mut all = self.0.lock().expect("lock");
        values.record(&mut Fields(&mut all[index].fields));
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let index = *ctx
            .span(&id)
            .expect("span exists")
            .extensions()
            .get::<usize>()
            .expect("indexed");
        self.0.lock().expect("lock")[index].closed = true;
    }
}

#[test]
fn tracing_tracer_emits_a_span_with_the_expected_name_and_fields() {
    let capture = Capture::default();
    let subscriber = Registry::default().with(capture.clone());

    subscriber::with_default(subscriber, || {
        let tracer = TracingTracer::new();
        let mut append = tracer.start(
            span::APPEND,
            &[
                (attr::STREAM_ID, AttrValue::Str("abc:Order")),
                (attr::AGGREGATE_TYPE, AttrValue::Str("Order")),
                (attr::EVENT_COUNT, AttrValue::U64(2)),
            ],
        );
        append.set(attr::VERSION, AttrValue::U64(7));

        let mut page = tracer.start(span::SUBSCRIPTION_PAGE, &[]);
        page.set(attr::SUBSCRIPTION_NAME, AttrValue::Str("orders"));
        page.set(attr::BATCH_SIZE, AttrValue::U64(100));
        page.set(attr::FAILURE_COUNT, AttrValue::U64(3));
        page.fail("Handler");
    });

    let seen = capture.0.lock().expect("lock").clone();
    let append = seen
        .iter()
        .find(|s| s.name == span::APPEND)
        .expect("append span");
    assert_eq!(append.fields[attr::STREAM_ID], "abc:Order");
    assert_eq!(append.fields[attr::AGGREGATE_TYPE], "Order");
    assert_eq!(append.fields[attr::EVENT_COUNT], "2");
    assert_eq!(append.fields[attr::VERSION], "7");
    assert!(append.closed, "the span ends when the guard drops");

    let page = seen
        .iter()
        .find(|s| s.name == span::SUBSCRIPTION_PAGE)
        .expect("page span");
    assert_eq!(page.fields[attr::SUBSCRIPTION_NAME], "orders");
    assert_eq!(page.fields[attr::BATCH_SIZE], "100");
    assert_eq!(page.fields[attr::FAILURE_COUNT], "3");
    assert_eq!(page.fields["otel.status_code"], "ERROR");
    assert_eq!(page.fields[attr::ERROR_TYPE], "Handler");
}
