//! Selected physical nodes, output fields and logical scans in bounded plan JSON.

mod type_disclosure;

use std::sync::Arc;

use clap::error::ErrorKind;
use datafusion::{
    common::{stats::Precision, tree_node::TreeNodeRecursion},
    logical_expr::{LogicalPlan, TableProviderFilterPushDown, TableScan},
    physical_plan::ExecutionPlan,
};
use delta_arrow_reader::datafusion::collect_scan_metrics;
use serde_json::{Value, json};
use snafu::ResultExt;

use super::ExplainArgs;
use crate::{
    diagnostics::{Error, InputCorrection, QuerySnafu},
    query::error::PlanningSnafu,
};

impl ExplainArgs {
    pub(super) fn encode_selection(
        &self,
        logical: &LogicalPlan,
        physical: &Arc<dyn ExecutionPlan>,
        bindings: &[Value],
    ) -> Result<Vec<u8>, Error> {
        let nodes = index_physical_nodes(physical.as_ref());
        let mut scans = Vec::new();
        logical
            .apply_with_subqueries(|plan| {
                if let LogicalPlan::TableScan(scan) = plan {
                    scans.push(scan.clone());
                }
                Ok(TreeNodeRecursion::Continue)
            })
            .context(PlanningSnafu)
            .context(QuerySnafu)?;
        let mut result = json!({
            "format_version": 1,
            "kind": "query_plan",
            "bindings": bindings,
            "execution": "not_started",
        });
        if let Some(index) = self.scan {
            let scan = scans.get(index).ok_or_else(|| invalid_selection(
                "--scan", "an existing logical scan ID",
                "Read scan_count from the summary; IDs start at zero. Preserve SQL, bindings and budgets.",
            ))?;
            result["scan"] = describe_logical_scan(index, scan)?;
        }
        if self.node.is_some() || !self.fields.is_empty() || self.statistics {
            let index = self.node.unwrap_or(0);
            let node = nodes.get(index).ok_or_else(|| invalid_selection(
                "--node", "an existing physical node ID",
                "Read node_count from the summary or child IDs from --node 0. Preserve SQL, bindings and budgets.",
            ))?;
            let schema = node.plan.schema();
            let mut detail = json!({"id": index});
            if self.node.is_some() {
                detail["operator"] = json!(node.plan.name());
                detail["children"] = json!(node.children);
                detail["column_count"] = json!(schema.fields().len());
            }
            if !self.fields.is_empty() {
                let mut fields = Vec::with_capacity(self.fields.len());
                for (position, index) in self.fields.iter().copied().enumerate() {
                    if self.fields[..position].contains(&index) {
                        return Err(invalid_selection(
                            "--field",
                            "each position selected once",
                            "Remove repeated --field values.",
                        ));
                    }
                    let field = schema.fields().get(index).ok_or_else(|| invalid_selection(
                        "--field", "an existing output field position",
                        "Read column_count from the summary or selected node; positions start at zero.",
                    ))?;
                    fields.push(type_disclosure::describe_plan_field(index, field)?);
                }
                detail["fields"] = fields.into();
            }
            if self.statistics {
                detail["statistics"] = describe_statistics(node.plan)?;
            }
            result["node"] = detail;
        } else if self.scan.is_none() {
            result["node_count"] = json!(nodes.len());
            result["scan_count"] = json!(scans.len());
            result["column_count"] = json!(physical.schema().fields().len());
        }
        let mut bytes =
            serde_json::to_vec(&result).map_err(|source| Error::JsonOutput { source })?;
        bytes.push(b'\n');
        if bytes.len() > self.max_output_bytes.get() {
            return Err(Error::OutputLimit {
                reason: "plan_output_limit_exceeded",
            });
        }
        Ok(bytes)
    }
}

struct PhysicalPlanNode<'a> {
    plan: &'a dyn ExecutionPlan,
    children: Vec<usize>,
}

fn index_physical_nodes(root: &dyn ExecutionPlan) -> Vec<PhysicalPlanNode<'_>> {
    let mut nodes: Vec<PhysicalPlanNode<'_>> = Vec::new();
    let mut pending = vec![(root, None::<usize>)];
    while let Some((plan, parent)) = pending.pop() {
        let index = nodes.len();
        if let Some(parent) = parent {
            nodes[parent].children.push(index);
        }
        nodes.push(PhysicalPlanNode {
            plan,
            children: Vec::new(),
        });
        pending.extend(
            plan.children()
                .into_iter()
                .rev()
                .map(|child| (child.as_ref(), Some(index))),
        );
    }
    nodes
}

fn describe_logical_scan(index: usize, scan: &TableScan) -> Result<Value, Error> {
    let filters: Vec<_> = scan.filters.iter().collect();
    let decisions = scan
        .source
        .supports_filters_pushdown(&filters)
        .context(PlanningSnafu)
        .context(QuerySnafu)?;
    let mut result = json!({
        "id": index,
        "binding": scan.table_name.table(),
        "projection": scan.projection,
        "source_column_count": scan.source.schema().fields().len(),
        "pushed_filters": {
            "exact": decisions.iter().filter(|d| **d == TableProviderFilterPushDown::Exact).count(),
            "inexact": decisions.iter().filter(|d| **d == TableProviderFilterPushDown::Inexact).count(),
        },
    });
    if let Some(fetch) = scan.fetch {
        result["fetch"] = json!(fetch.to_string());
    }
    Ok(result)
}

fn describe_statistics(plan: &dyn ExecutionPlan) -> Result<Value, Error> {
    let statistics = plan
        .partition_statistics(None)
        .context(PlanningSnafu)
        .context(QuerySnafu)?;
    let mut result = json!({
        "rows": describe_precision(&statistics.num_rows),
        "relation_bytes": describe_precision(&statistics.total_byte_size),
    });
    if plan.name() == "DeltaScanExec"
        && let Some(metrics) = collect_scan_metrics(plan).first()
    {
        let snapshot = metrics.snapshot().reader_metrics;
        result["selected_files"] = json!({
            "files": snapshot.files_planned.to_string(),
            "input_rows_estimate": snapshot.estimated_input_rows.map(|n| n.to_string()),
            "input_bytes_estimate": snapshot.estimated_input_bytes.map(|n| n.to_string()),
        });
    }
    Ok(result)
}

fn describe_precision(value: &Precision<usize>) -> Value {
    match value {
        Precision::Exact(value) => json!({"precision": "exact", "value": value.to_string()}),
        Precision::Inexact(value) => json!({"precision": "estimated", "value": value.to_string()}),
        Precision::Absent => json!({"precision": "unknown"}),
    }
}

fn invalid_selection(argument: &str, expected: &str, hint: &'static str) -> Error {
    Error::Argument {
        kind: ErrorKind::InvalidValue,
        correction: Some(InputCorrection {
            argument: argument.into(),
            expected: expected.into(),
            hint,
            field_path: None,
            supplied: None,
        }),
    }
}

#[cfg(test)]
mod tests;
