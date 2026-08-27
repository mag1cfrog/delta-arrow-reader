//! Public reader integration tests.

#[cfg(feature = "datafusion")]
#[path = "reader/datafusion_adapter.rs"]
mod datafusion_adapter;
#[path = "reader/direct_reader.rs"]
mod direct_reader;
#[path = "reader/portable_fixtures.rs"]
mod portable_fixtures;
#[path = "reader/support.rs"]
mod support;
