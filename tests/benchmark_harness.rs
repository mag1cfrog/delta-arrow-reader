//! Test target for the frozen reader benchmark harness.

#![cfg(all(
    feature = "datafusion",
    feature = "native-async",
    feature = "official-kernel"
))]

#[allow(dead_code)]
#[path = "../benches/reader.rs"]
mod harness;
