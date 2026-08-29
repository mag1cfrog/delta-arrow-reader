//! Test target for the frozen reader benchmark harness.

#![cfg(feature = "datafusion")]

#[allow(dead_code)]
#[path = "../benches/reader.rs"]
mod harness;
