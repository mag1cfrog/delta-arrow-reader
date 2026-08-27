//! Public reader integration tests.

#[cfg(all(feature = "datafusion", feature = "native-async"))]
#[path = "reader/datafusion_adapter.rs"]
mod datafusion_adapter;
#[cfg(any(feature = "native-async", feature = "official-kernel"))]
#[path = "reader/direct_reader.rs"]
mod direct_reader;
#[path = "reader/portable_fixtures.rs"]
mod portable_fixtures;
#[path = "reader/support.rs"]
mod support;
