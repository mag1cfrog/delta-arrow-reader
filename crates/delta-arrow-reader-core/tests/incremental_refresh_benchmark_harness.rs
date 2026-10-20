//! Test target for the incremental-refresh benchmark harness.

#![deny(missing_docs, clippy::expect_used, clippy::panic, clippy::unwrap_used)]
#[allow(dead_code)]
#[path = "../benches/incremental_refresh.rs"]
mod harness;
