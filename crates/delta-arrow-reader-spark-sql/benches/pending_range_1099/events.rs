//! Disposable observations for the real bounded executor. Never linked in product builds.

use std::{cell::RefCell, time::Instant};

const NAMES: [&str; 35] = [
    "input_batches",
    "input_rows",
    "input_batch_min",
    "input_batch_max",
    "partition_concat_calls",
    "partition_copied_rows",
    "input_concat_calls",
    "input_copied_rows",
    "output_concat_calls",
    "output_copied_rows",
    "argument_calls",
    "argument_array_rows",
    "filter_calls",
    "filter_rows",
    "order_calls",
    "order_rows",
    "range_calls",
    "upper_comparisons",
    "lower_comparisons",
    "peer_upper_comparisons",
    "peer_lower_comparisons",
    "target_calls",
    "completion_calls",
    "pending_deferrals",
    "committed_rows",
    "accumulator_update_calls",
    "accumulator_updated_rows",
    "accumulator_evaluations",
    "pruned_rows",
    "peak_input_rows",
    "peak_partition_rows",
    "peak_output_rows",
    "partition_copied_cells",
    "input_copied_cells",
    "pending_trace_omitted",
];

const PHASES: [&str; 9] = [
    "partition_concat_ns",
    "input_concat_ns",
    "arguments_ns",
    "filter_ns",
    "ordering_ns",
    "range_ns",
    "completion_ns",
    "accumulator_ns",
    "output_concat_ns",
];

struct Events {
    counts: [u64; 35],
    nanos: [u64; 9],
    pending: Vec<[usize; 8]>,
    timing: bool,
}

impl Default for Events {
    fn default() -> Self {
        Self {
            counts: [0; 35],
            nanos: [0; 9],
            pending: Vec::new(),
            timing: false,
        }
    }
}

thread_local! {
    static EVENTS: RefCell<Events> = RefCell::new(Events::default());
}

/// Clear observations; counts-only probes never read a clock.
pub fn reset(timing: bool) {
    EVENTS.with(|events| {
        *events.borrow_mut() = Events {
            timing,
            ..Events::default()
        }
    });
}

/// Increment one fixed counter; identifiers are indices in the reviewed overlay.
pub fn add(index: usize, value: usize) {
    EVENTS.with(|events| events.borrow_mut().counts[index] += value as u64);
}

/// Observe a retained row count without claiming unique allocated bytes.
pub fn peak(index: usize, value: usize) {
    EVENTS.with(|events| {
        let mut events = events.borrow_mut();
        events.counts[index] = events.counts[index].max(value as u64);
    });
}

/// Record actual input boundaries at the executor, after physical planning.
pub fn input_batch(rows: usize) {
    EVENTS.with(|events| {
        let mut events = events.borrow_mut();
        if events.counts[0] == 0 {
            events.counts[2] = rows as u64;
        } else {
            events.counts[2] = events.counts[2].min(rows as u64);
        }
        events.counts[0] += 1;
        events.counts[1] += rows as u64;
        events.counts[3] = events.counts[3].max(rows as u64);
    });
}

/// Keep the first eight and last eight deferrals, with cumulative comparison counts.
pub fn pending(
    idx: usize,
    length: usize,
    old: &std::ops::Range<usize>,
    new: &std::ops::Range<usize>,
) {
    EVENTS.with(|events| {
        let mut events = events.borrow_mut();
        events.counts[23] += 1;
        let row = [
            idx,
            length,
            old.start,
            old.end,
            new.start,
            new.end,
            events.counts[17] as usize,
            events.counts[18] as usize,
        ];
        if events.pending.len() == 16 {
            events.pending.remove(8);
            events.counts[34] += 1;
        }
        events.pending.push(row);
    });
}

/// Snapshot fixed counters and bounded diagnostics without resetting them.
pub fn snapshot() -> (Vec<(&'static str, u64)>, Vec<[usize; 8]>) {
    EVENTS.with(|events| {
        let events = events.borrow();
        let mut values = NAMES.into_iter().zip(events.counts).collect::<Vec<_>>();
        if events.timing {
            values.extend(PHASES.into_iter().zip(events.nanos));
        }
        (values, events.pending.clone())
    })
}

/// Coarse source-region timer, enabled only after exact native eligibility.
pub struct Span(usize, Option<Instant>);

impl Span {
    /// Start one nonoverlapping source region, or perform no clock read.
    pub fn new(index: usize) -> Self {
        Self(
            index,
            EVENTS
                .with(|events| events.borrow().timing)
                .then(Instant::now),
        )
    }
}

impl Drop for Span {
    fn drop(&mut self) {
        if let Some(started) = self.1 {
            let nanos = started.elapsed().as_nanos() as u64;
            EVENTS.with(|events| events.borrow_mut().nanos[self.0] += nanos);
        }
    }
}
