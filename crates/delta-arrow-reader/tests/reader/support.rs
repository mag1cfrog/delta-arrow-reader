//! Shared real-Parquet integration-test fixtures.

#[path = "support/real_parquet_delta_table.rs"]
mod real_parquet_delta_table;

pub(crate) use real_parquet_delta_table::RealParquetDeltaTable;
