// Modified from Sail v0.7.1 for the Delta reader experiment. See experiments/spark-sql/UPSTREAM.md in the host repository.
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

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
    /// Whether `COUNT()` is accepted with no arguments. Spark's legacy behavior returns zero;
    /// it does not interpret the call as `COUNT(*)`.
    pub legacy_allow_parameterless_count: bool,
}

impl Default for PlanConfig {
    fn default() -> Self {
        Self {
            session_timezone: Arc::from("UTC"),
            default_timestamp_type: DefaultTimestampType::TimestampLtz,
            arrow_use_large_var_types: false,
            session_user_id: "".to_string(),
            ansi_mode: true,
            map_key_dedup_policy: MapKeyDedupPolicy::Exception,
            cross_join_enabled: true,
            legacy_allow_parameterless_count: false,
        }
    }
}
