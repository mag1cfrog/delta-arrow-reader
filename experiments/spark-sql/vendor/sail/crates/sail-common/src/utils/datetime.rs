// Modified from Sail v0.7.1 for the Delta reader experiment. See experiments/spark-sql/UPSTREAM.md in the host repository.
use arrow_schema::TimeUnit;

pub const fn time_unit_to_multiplier(time_unit: &TimeUnit) -> i64 {
    match time_unit {
        TimeUnit::Second => 1i64,
        TimeUnit::Millisecond => 1000i64,
        TimeUnit::Microsecond => 1_000_000i64,
        TimeUnit::Nanosecond => 1_000_000_000i64,
    }
}
