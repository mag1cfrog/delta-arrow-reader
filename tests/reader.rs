//! Public reader integration tests.

#[cfg(feature = "datafusion")]
#[path = "reader/datafusion_adapter.rs"]
mod datafusion_adapter;
#[path = "reader/portable_fixtures.rs"]
mod portable_fixtures;
#[path = "reader/streaming_reader.rs"]
mod streaming_reader;
#[path = "reader/support.rs"]
mod support;
