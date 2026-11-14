// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::collections::BTreeMap;
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
    /// The configured session zone. Public planning normalizes aliases on a cloned config
    /// and rejects zones that cannot be represented in Arrow timestamp annotations.
    pub session_timezone: Arc<str>,
    /// Configured spelling retained when the resolver normalizes its operational zone.
    /// Used by CURRENT_TIMEZONE and fixed-zone display names; callers normally leave None.
    pub original_session_timezone: Option<Arc<str>>,
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
    /// `spark.sql.legacy.sizeOfNull`: SIZE and CARDINALITY return -1 for NULL
    /// collections when this is true and ANSI mode is disabled.
    pub legacy_size_of_null: bool,
    /// Expand SQL configuration references before parsing; enabled by default.
    pub variable_substitution_enabled: bool,
    /// Explicit configuration values for `${name}` and the Spark/Hive config prefixes.
    /// Missing names expand to empty text. Environment and system properties are not read.
    /// Redaction overrides use native ASCII regex syntax and fail if unsupported.
    pub variable_substitution_values: BTreeMap<String, String>,
}

impl Default for PlanConfig {
    fn default() -> Self {
        Self {
            session_timezone: Arc::from("UTC"),
            original_session_timezone: None,
            default_timestamp_type: DefaultTimestampType::TimestampLtz,
            arrow_use_large_var_types: false,
            session_user_id: "".to_string(),
            ansi_mode: true,
            map_key_dedup_policy: MapKeyDedupPolicy::Exception,
            cross_join_enabled: true,
            legacy_allow_parameterless_count: false,
            legacy_size_of_null: true,
            variable_substitution_enabled: true,
            variable_substitution_values: BTreeMap::new(),
        }
    }
}
