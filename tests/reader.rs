//! Public reader integration tests.

#[path = "reader/compact_deletion_vectors.rs"]
mod compact_deletion_vectors;
#[path = "reader/data_file_location.rs"]
mod data_file_location;
#[cfg(feature = "datafusion")]
#[path = "reader/datafusion_adapter.rs"]
mod datafusion_adapter;
#[path = "reader/empty_projection.rs"]
mod empty_projection;
#[path = "reader/execution_capacities.rs"]
mod execution_capacities;
#[path = "reader/external_writer.rs"]
mod external_writer;
#[path = "reader/legacy_lists.rs"]
mod legacy_lists;
#[path = "reader/nested_nullability.rs"]
mod nested_nullability;
#[path = "reader/partition_predicates.rs"]
mod partition_predicates;
#[path = "reader/portable_fixtures.rs"]
mod portable_fixtures;
#[path = "reader/streaming_reader.rs"]
mod streaming_reader;
#[path = "reader/support.rs"]
mod support;
