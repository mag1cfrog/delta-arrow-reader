// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
mod spark_ntile;

pub use spark_ntile::{SparkNtile, spark_ntile_udwf};
