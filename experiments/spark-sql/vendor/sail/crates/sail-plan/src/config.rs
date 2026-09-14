// Modified from Sail v0.7.1 for the Delta reader experiment. See experiments/spark-sql/UPSTREAM.md in the host repository.
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

use crate::error::PlanResult;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd)]
pub enum DefaultTimestampType {
    TimestampLtz,
    TimestampNtz,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd)]
pub enum MapKeyDedupPolicy {
    #[default]
    Exception,
    LastWin,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub struct PlanConfig {
    /// The time zone of the session.
    pub session_timezone: Arc<str>,
    /// The locale of the session.
    pub session_locale: Arc<str>,
    /// The default timestamp type.
    pub default_timestamp_type: DefaultTimestampType,
    /// Whether to use large variable types in Arrow.
    pub arrow_use_large_var_types: bool,
    pub session_user_id: String,
    pub ansi_mode: bool,
    /// Policy for duplicate keys created by map functions.
    pub map_key_dedup_policy: MapKeyDedupPolicy,
    /// Whether to allow cartesian products (cross joins) without explicit `CROSS JOIN` syntax.
    pub cross_join_enabled: bool,
    /// Whether identifiers (e.g. column names) are matched case-sensitively.
    /// Spark defaults to case-insensitive matching (`spark.sql.caseSensitive=false`).
    pub case_sensitive: bool,
    /// The maximum number of distinct values collected for a pivot without an explicit
    /// value list (`spark.sql.pivotMaxValues`, default 10000). Exceeding it is an error.
    pub pivot_max_values: usize,
    /// Whether `COUNT()` is accepted with no arguments. Spark's legacy behavior returns zero;
    /// it does not interpret the call as `COUNT(*)`.
    pub legacy_allow_parameterless_count: bool,
}

impl PlanConfig {
    pub fn new() -> PlanResult<Self> {
        Ok(Self::default())
    }
}

impl Default for PlanConfig {
    fn default() -> Self {
        Self {
            session_timezone: Arc::from("UTC"),
            session_locale: Arc::from("en-US"),
            default_timestamp_type: DefaultTimestampType::TimestampLtz,
            arrow_use_large_var_types: false,
            session_user_id: "".to_string(),
            ansi_mode: true,
            map_key_dedup_policy: MapKeyDedupPolicy::Exception,
            cross_join_enabled: true,
            case_sensitive: false,
            pivot_max_values: 10000,
            legacy_allow_parameterless_count: false,
        }
    }
}
