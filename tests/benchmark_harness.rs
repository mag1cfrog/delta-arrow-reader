//! Test target for the frozen reader benchmark harness.

#![cfg(all(
    feature = "datafusion",
    feature = "experimental-parquet-metadata-preparation"
))]

#[allow(dead_code)]
#[path = "../benches/reader.rs"]
mod harness;
