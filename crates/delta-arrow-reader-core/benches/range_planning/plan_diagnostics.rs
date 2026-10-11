//! Capture of the doc-hidden automatic range-planning trace event.

use std::sync::{Arc, Mutex};

use tracing::{
    Event, Level, Metadata, Subscriber,
    field::{Field as TracingField, Visit},
    span::{Attributes as TracingAttributes, Id, Record},
    subscriber::Interest,
};

const RANGE_PLANNING_DIAGNOSTIC_TARGET: &str =
    "delta_arrow_reader::diagnostics::parquet_range_planning";

#[derive(Debug, Default)]
pub(super) struct AutomaticRangePlanDiagnostic {
    pub(super) latency_sample_count: u64,
    pub(super) throughput_sample_count: u64,
    pub(super) estimated_request_latency_micros: Option<u128>,
    pub(super) estimated_shared_throughput_bytes_per_second: Option<u64>,
    pub(super) estimated_bandwidth_delay_bytes: Option<u128>,
    pub(super) exact_range_count: u64,
    pub(super) exact_bytes: u128,
    pub(super) exact_request_waves: u64,
    pub(super) baseline_range_count: u64,
    pub(super) baseline_bytes: u128,
    pub(super) baseline_request_waves: u64,
    pub(super) selected_range_count: u64,
    pub(super) selected_bytes: u128,
    pub(super) selected_request_waves: u64,
    selected_predicted_cost_bytes: Option<u128>,
    pub(super) observed_selected_plan_micros: u128,
    pub(super) decision: String,
}

impl AutomaticRangePlanDiagnostic {
    pub(super) fn predicted_micros(&self) -> Option<u128> {
        self.selected_predicted_cost_bytes?
            .checked_mul(1_000_000)?
            .checked_div(u128::from(
                self.estimated_shared_throughput_bytes_per_second?,
            ))
    }
}

#[derive(Clone, Default)]
pub(super) struct AutomaticRangePlanCollector(Arc<Mutex<Vec<AutomaticRangePlanDiagnostic>>>);

impl AutomaticRangePlanCollector {
    pub(super) fn install() -> Result<Self, tracing::subscriber::SetGlobalDefaultError> {
        let collector = Self::default();
        tracing::subscriber::set_global_default(collector.clone())?;
        Ok(collector)
    }

    pub(super) fn take(&self) -> Vec<AutomaticRangePlanDiagnostic> {
        std::mem::take(
            &mut *self
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }
}

impl Subscriber for AutomaticRangePlanCollector {
    fn register_callsite(&self, metadata: &'static Metadata<'static>) -> Interest {
        if self.enabled(metadata) {
            Interest::always()
        } else {
            Interest::never()
        }
    }

    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.target() == RANGE_PLANNING_DIAGNOSTIC_TARGET && *metadata.level() == Level::DEBUG
    }

    fn new_span(&self, _attributes: &TracingAttributes<'_>) -> Id {
        Id::from_u64(1)
    }

    fn record(&self, _span: &Id, _values: &Record<'_>) {}

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let mut diagnostic = AutomaticRangePlanDiagnostic::default();
        event.record(&mut AutomaticRangePlanVisitor(&mut diagnostic));
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(diagnostic);
    }

    fn enter(&self, _span: &Id) {}

    fn exit(&self, _span: &Id) {}
}

struct AutomaticRangePlanVisitor<'diagnostic>(&'diagnostic mut AutomaticRangePlanDiagnostic);

impl Visit for AutomaticRangePlanVisitor<'_> {
    fn record_u64(&mut self, field: &TracingField, value: u64) {
        match field.name() {
            "latency_sample_count" => self.0.latency_sample_count = value,
            "throughput_sample_count" => self.0.throughput_sample_count = value,
            "estimated_shared_throughput_bytes_per_second" => {
                self.0.estimated_shared_throughput_bytes_per_second = Some(value);
            }
            "exact_range_count" => self.0.exact_range_count = value,
            "exact_request_waves" => self.0.exact_request_waves = value,
            "baseline_range_count" => self.0.baseline_range_count = value,
            "baseline_request_waves" => self.0.baseline_request_waves = value,
            "selected_range_count" => self.0.selected_range_count = value,
            "selected_request_waves" => self.0.selected_request_waves = value,
            _ => {}
        }
    }

    fn record_u128(&mut self, field: &TracingField, value: u128) {
        match field.name() {
            "estimated_request_latency_micros" => {
                self.0.estimated_request_latency_micros = Some(value);
            }
            "estimated_bandwidth_delay_bytes" => {
                self.0.estimated_bandwidth_delay_bytes = Some(value);
            }
            "exact_bytes" => self.0.exact_bytes = value,
            "baseline_bytes" => self.0.baseline_bytes = value,
            "selected_bytes" => self.0.selected_bytes = value,
            "selected_predicted_cost_bytes" => {
                self.0.selected_predicted_cost_bytes = Some(value);
            }
            "observed_selected_plan_micros" => self.0.observed_selected_plan_micros = value,
            _ => {}
        }
    }

    fn record_str(&mut self, field: &TracingField, value: &str) {
        if field.name() == "decision" {
            self.0.decision = value.to_owned();
        }
    }

    fn record_debug(&mut self, _field: &TracingField, _value: &dyn std::fmt::Debug) {}
}
