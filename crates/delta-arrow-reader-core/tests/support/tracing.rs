//! Captures typed fields and callback sites for diagnostic contract tests.

use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
use tracing::{
    Event, Level, Metadata, Subscriber,
    field::{Field, Visit},
    span::{Attributes, Id, Record},
    subscriber::Interest,
};

#[derive(Clone, Debug)]
pub struct Captured {
    pub level: Level,
    pub target: String,
    pub is_span: bool,
    pub fields: BTreeMap<String, Value>,
}

#[derive(Clone)]
pub struct Collector {
    level: Level,
    records: Arc<Mutex<Vec<Captured>>>,
    next_span: Arc<AtomicU64>,
    pub probe: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl Collector {
    pub fn new(level: Level) -> Self {
        Self {
            level,
            records: Arc::default(),
            next_span: Arc::new(AtomicU64::new(1)),
            probe: None,
        }
    }

    pub fn records(&self) -> Vec<Captured> {
        self.records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub fn events(&self) -> Vec<Captured> {
        self.records()
            .into_iter()
            .filter(|record| !record.is_span)
            .collect()
    }

    pub fn clear(&self) {
        self.records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
    }

    fn capture(&self, metadata: &Metadata<'_>, fields: Fields) {
        self.records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(Captured {
                level: *metadata.level(),
                target: metadata.target().into(),
                is_span: metadata.is_span(),
                fields: fields.0,
            });
        if let Some(probe) = &self.probe {
            probe();
        }
    }
}

impl Subscriber for Collector {
    fn register_callsite(&self, _: &'static Metadata<'static>) -> Interest {
        Interest::sometimes()
    }
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.target().starts_with("delta_arrow_reader") && *metadata.level() <= self.level
    }
    fn new_span(&self, attributes: &Attributes<'_>) -> Id {
        let mut fields = Fields::default();
        attributes.record(&mut fields);
        self.capture(attributes.metadata(), fields);
        Id::from_u64(self.next_span.fetch_add(1, Ordering::Relaxed))
    }
    fn event(&self, event: &Event<'_>) {
        let mut fields = Fields::default();
        event.record(&mut fields);
        self.capture(event.metadata(), fields);
    }
    fn record(&self, _: &Id, values: &Record<'_>) {
        let mut fields = Fields::default();
        values.record(&mut fields);
        self.records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(Captured {
                level: Level::TRACE,
                target: "span.record".into(),
                is_span: true,
                fields: fields.0,
            });
    }
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn enter(&self, _: &Id) {}
    fn exit(&self, _: &Id) {}
}

#[derive(Default)]
struct Fields(BTreeMap<String, Value>);
impl Visit for Fields {
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
