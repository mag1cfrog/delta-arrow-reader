//! Bounded, thread-local timing storage for the disposable #1390 overlay.

use std::{cell::RefCell, time::Instant};

const LIMIT: usize = 16_384;
thread_local! {
    static EVENTS: RefCell<(Vec<(String, u64)>, bool)> = RefCell::new((Vec::with_capacity(LIMIT), false));
}

/// Clear observations before a query, retaining the allocation.
pub fn reset() {
    EVENTS.with(|events| {
        let mut events = events.borrow_mut();
        events.0.clear();
        events.1 = false;
    });
}

/// Drain observations after the outer wall timer has stopped.
pub fn take() -> Result<Vec<(String, u64)>, &'static str> {
    EVENTS.with(|events| {
        let mut events = events.borrow_mut();
        if events.1 {
            return Err("#1390 timing storage exceeded its declared bound");
        }
        Ok(events.0.drain(..).collect())
    })
}

/// Record an inclusive interval; allocation and storage are outside this span.
pub struct Span {
    name: String,
    started: Option<Instant>,
}

impl Span {
    /// Start an interval after preparing its name.
    pub fn new(name: impl Into<String>) -> Self {
        Self::when(name, true)
    }

    /// Observe only statement parsing when a parser macro has other callers.
    pub fn when(name: impl Into<String>, enabled: bool) -> Self {
        let name = name.into();
        Self {
            name,
            started: enabled.then(Instant::now),
        }
    }
}

impl Drop for Span {
    fn drop(&mut self) {
        let Some(started) = self.started else { return };
        let elapsed = started.elapsed().as_nanos() as u64;
        EVENTS.with(|events| {
            let mut events = events.borrow_mut();
            if events.0.len() == LIMIT {
                events.1 = true;
            } else {
                events.0.push((std::mem::take(&mut self.name), elapsed));
            }
        });
    }
}
