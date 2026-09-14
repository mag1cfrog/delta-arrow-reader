// Modified from Sail v0.7.1 for the Delta reader experiment. See experiments/spark-sql/UPSTREAM.md in the host repository.
use std::sync::Arc;

use datafusion::catalog::TableFunction;

use crate::function::table::range::RangeTableFunction;

mod range;
mod range_exec;

pub(super) fn list_built_in_table_functions() -> Vec<(&'static str, Arc<TableFunction>)> {
    vec![("range", Arc::new(RangeTableFunction::new()))]
        .into_iter()
        .map(|(name, func)| (name, Arc::new(TableFunction::new(name.to_string(), func))))
        .collect()
}
