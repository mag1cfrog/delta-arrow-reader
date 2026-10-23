//! Captures typed fields and context transitions for diagnostic contract tests.
#![allow(dead_code, clippy::unwrap_used)]

use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    thread::{self, ThreadId},
};
use tracing::{
    Event, Level, Metadata, Subscriber,
    field::{Field, Visit},
    span::{Attributes, Id, Record},
    subscriber::Interest,
};
use tracing_core::span::Current;

#[derive(Clone, Debug)]
pub struct CapturedTraceRecord {
    pub name: String,
    pub span_id: Option<u64>,
    pub parent: Option<u64>,
    pub thread: ThreadId,
    pub level: Level,
    pub target: String,
    pub is_span: bool,
    pub fields: BTreeMap<String, Value>,
}

#[derive(Clone)]
pub struct TraceCollector {
    level: Level,
    records: Arc<Mutex<Vec<CapturedTraceRecord>>>,
    spans: Arc<Mutex<SpanRegistry>>,
    lifetime: Arc<()>,
    // Keep untraced threads from caching disabled callsites for a scoped collector.
    _untraced: tracing::Dispatch,
    pub on_capture: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl TraceCollector {
    pub fn new(level: Level) -> Self {
        Self {
            level,
            records: Arc::default(),
            spans: Arc::default(),
            lifetime: Arc::new(()),
            _untraced: tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default()),
            on_capture: None,
        }
    }

    pub fn records(&self) -> Vec<CapturedTraceRecord> {
        self.records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub fn events(&self) -> Vec<CapturedTraceRecord> {
        self.records()
            .into_iter()
            .filter(|record| !record.is_span)
            .collect()
    }

    pub fn clear_records(&self) {
        self.records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
    }

    pub fn lifetime(&self) -> std::sync::Weak<()> {
        Arc::downgrade(&self.lifetime)
    }

    pub fn linked_to(&self, record: &CapturedTraceRecord, ancestor: u64) -> bool {
        let spans = self.spans.lock().unwrap();
        let mut pending: Vec<_> = record.span_id.or(record.parent).into_iter().collect();
        while let Some(id) = pending.pop() {
            if id == ancestor {
                return true;
            }
            let span = &spans.by_id[&id];
            pending.extend(span.parent);
            pending.extend(span.follows_from.iter().copied());
        }
        false
    }

    pub fn assert_no_entered_spans(&self) {
        assert!(
            self.spans
                .lock()
                .unwrap()
                .entered_by_thread
                .values()
                .all(Vec::is_empty)
        );
    }

    fn current_span_id(&self) -> Option<u64> {
        self.spans
            .lock()
            .unwrap()
            .entered_by_thread
            .get(&thread::current().id())
            .and_then(|stack| stack.last().copied())
    }

    fn capture(
        &self,
        metadata: &Metadata<'_>,
        fields: TraceFieldVisitor,
        span_id: Option<u64>,
        parent: Option<u64>,
    ) {
        self.records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(CapturedTraceRecord {
                name: metadata.name().into(),
                span_id,
                parent,
                thread: thread::current().id(),
                level: *metadata.level(),
                target: metadata.target().into(),
                is_span: metadata.is_span(),
                fields: fields.0,
            });
        if let Some(probe) = &self.on_capture {
            probe();
        }
    }
}

impl Subscriber for TraceCollector {
    fn register_callsite(&self, _: &'static Metadata<'static>) -> Interest {
        Interest::sometimes()
    }
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.target().starts_with("delta_arrow_reader") && *metadata.level() <= self.level
    }
    fn new_span(&self, attributes: &Attributes<'_>) -> Id {
        let mut fields = TraceFieldVisitor::default();
        attributes.record(&mut fields);
        static NEXT_SPAN: AtomicU64 = AtomicU64::new(1);
        let id = NEXT_SPAN.fetch_add(1, Ordering::Relaxed);
        let parent = attributes.parent().map(Id::into_u64).or_else(|| {
            attributes
                .is_contextual()
                .then(|| self.current_span_id())
                .flatten()
        });
        self.spans.lock().unwrap().by_id.insert(
            id,
            SpanState {
                metadata: attributes.metadata(),
                parent,
                follows_from: Vec::new(),
                reference_count: 1,
            },
        );
        self.capture(attributes.metadata(), fields, Some(id), parent);
        Id::from_u64(id)
    }
    fn event(&self, event: &Event<'_>) {
        let mut fields = TraceFieldVisitor::default();
        event.record(&mut fields);
        let parent = event.parent().map(Id::into_u64).or_else(|| {
            event
                .is_contextual()
                .then(|| self.current_span_id())
                .flatten()
        });
        self.capture(event.metadata(), fields, None, parent);
    }
    fn record(&self, id: &Id, values: &Record<'_>) {
        let mut fields = TraceFieldVisitor::default();
        values.record(&mut fields);
        self.records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(CapturedTraceRecord {
                name: "span.record".into(),
                span_id: Some(id.into_u64()),
                parent: self.spans.lock().unwrap().by_id[&id.into_u64()].parent,
                thread: thread::current().id(),
                level: Level::TRACE,
                target: "span.record".into(),
                is_span: true,
                fields: fields.0,
            });
    }
    fn record_follows_from(&self, id: &Id, follows_from: &Id) {
        self.spans
            .lock()
            .unwrap()
            .by_id
            .get_mut(&id.into_u64())
            .unwrap()
            .follows_from
            .push(follows_from.into_u64());
    }
    fn enter(&self, id: &Id) {
        let mut spans = self.spans.lock().unwrap();
        assert!(spans.by_id.contains_key(&id.into_u64()));
        spans
            .entered_by_thread
            .entry(thread::current().id())
            .or_default()
            .push(id.into_u64());
    }
    fn exit(&self, id: &Id) {
        assert_eq!(
            self.spans
                .lock()
                .unwrap()
                .entered_by_thread
                .get_mut(&thread::current().id())
                .unwrap()
                .pop(),
            Some(id.into_u64())
        );
    }
    fn current_span(&self) -> Current {
        match self.current_span_id() {
            Some(id) => Current::new(
                Id::from_u64(id),
                self.spans.lock().unwrap().by_id[&id].metadata,
            ),
            None => Current::none(),
        }
    }
    fn clone_span(&self, id: &Id) -> Id {
        self.spans
            .lock()
            .unwrap()
            .by_id
            .get_mut(&id.into_u64())
            .unwrap()
            .reference_count += 1;
        id.clone()
    }
    fn try_close(&self, id: Id) -> bool {
        let mut spans = self.spans.lock().unwrap();
        let state = spans.by_id.get_mut(&id.into_u64()).unwrap();
        state.reference_count -= 1;
        state.reference_count == 0
    }
}

#[derive(Default)]
struct TraceFieldVisitor(BTreeMap<String, Value>);
impl Visit for TraceFieldVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.0
            .insert(field.name().into(), json!(format!("{value:?}")));
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().into(), json!(value));
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.0.insert(field.name().into(), json!(value));
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.0.insert(field.name().into(), json!(value));
    }
    fn record_u128(&mut self, field: &Field, value: u128) {
        self.0.insert(field.name().into(), json!(value));
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.0.insert(field.name().into(), json!(value));
    }
}

#[derive(Default)]
struct SpanRegistry {
    by_id: HashMap<u64, SpanState>,
    entered_by_thread: HashMap<ThreadId, Vec<u64>>,
}
struct SpanState {
    metadata: &'static Metadata<'static>,
    parent: Option<u64>,
    follows_from: Vec<u64>,
    reference_count: usize,
}
