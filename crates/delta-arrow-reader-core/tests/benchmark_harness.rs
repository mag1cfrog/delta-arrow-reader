//! Test target for the frozen reader benchmark harness.

#![deny(missing_docs, clippy::expect_used, clippy::panic, clippy::unwrap_used)]
#![cfg(feature = "datafusion")]

#[allow(dead_code)]
#[path = "../benches/reader.rs"]
mod harness;
