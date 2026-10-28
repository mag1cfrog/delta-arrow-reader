//! Inspect a query's planned work before deciding whether to execute it.
//!
//! The default response identifies loaded snapshots and the available selections.
//! For example, --node 0 describes the root operation, --field 0 its first output
//! type, and --scan 0 the first logical scan's projection and filter pushdown.
//! Physical node IDs and logical scan IDs are separate, zero-based preorder lists.

mod type_disclosure;

use std::{num::NonZeroUsize, sync::Arc};

use clap::{Args, error::ErrorKind};
use datafusion::{
    common::{stats::Precision, tree_node::TreeNodeRecursion},
    logical_expr::{LogicalPlan, TableProviderFilterPushDown, TableScan},
    physical_plan::ExecutionPlan,
};
use delta_arrow_reader::datafusion::collect_scan_metrics;
use futures_util::{FutureExt, future::LocalBoxFuture};
use serde_json::{Value, json};
use snafu::ResultExt;

use super::{PlanningSnafu, QueryInputArgs, resources};
use crate::{
    Error, QuerySnafu, cli::parse_unsigned_decimal, diagnostics::InputCorrection,
    output::write_stdout, table_access::TableLoadRequest,
};

pub(crate) const DEFAULT_PLAN_BYTES: usize = 4096;

#[derive(Args)]
pub(crate) struct ExplainArgs {
    #[command(flatten)]
    input: QueryInputArgs,
    /// Physical plan node in zero-based preorder. Root: 0. Returns its operator and child IDs.
    #[arg(long, value_name = "N", value_parser = parse_unsigned_decimal::<usize>)]
    node: Option<usize>,
    /// Exact output type by zero-based position on --node (default: root); repeat to select fields.
    /// Expression names and metadata are omitted. Types containing private text fail safely.
    #[arg(long = "field", value_name = "N", value_parser = parse_unsigned_decimal::<usize>)]
    fields: Vec<usize>,
    /// Planner row/byte statistics for --node (default: root), with explicit precision.
    #[arg(long)]
    statistics: bool,
    /// Logical table scan in zero-based preorder: source projection and filter pushdown.
    /// Can combine with physical node selections; scan IDs use a separate list.
    #[arg(long, value_name = "N", value_parser = parse_unsigned_decimal::<usize>)]
    scan: Option<usize>,
    /// Complete JSON stdout ceiling, including newline; exceeding it fails before stdout.
    #[arg(long, default_value = "4096", value_name = "BYTES", value_parser = parse_unsigned_decimal::<NonZeroUsize>)]
    max_output_bytes: NonZeroUsize,
    /// DataFusion memory-pool ceiling; metadata/planner buffers and process RSS are outside it.
    #[arg(long, default_value = "268435456", value_name = "BYTES", value_parser = parse_unsigned_decimal::<NonZeroUsize>)]
    memory_limit_bytes: NonZeroUsize,
    /// Whole-command deadline, including input files, authorization, planning and output.
    #[arg(long, default_value = "30000", value_name = "N", value_parser = parse_unsigned_decimal::<NonZeroUsize>)]
    pub(crate) deadline_ms: NonZeroUsize,
}

impl ExplainArgs {
    pub(crate) fn prepare_planning(
        &self,
    ) -> Result<(bool, LocalBoxFuture<'_, Result<Value, Error>>), Error> {
        let (requests, sql) = self.input.prepare_inputs(&mut None)?;
        let uses_unity_catalog = requests
            .iter()
            .any(|(_, request)| matches!(request, TableLoadRequest::UnityCatalog { .. }));
        let (runtime, _) = resources::build_query_runtime(self.memory_limit_bytes, 0, None)?;
        Ok((
            uses_unity_catalog,
            async move {
                let mut tables = Vec::with_capacity(requests.len());
                let mut authorized_access = Vec::new();
                for (alias, request) in requests {
                    let (table, access) = request.load_table().await?;
                    authorized_access.extend(access);
                    tables.push((alias, table));
                }
                let bindings: Vec<_> = tables.iter()
                    .map(|(alias, table)| json!({"alias": alias, "table_version": table.version().to_string()}))
                    .collect();
                let result = async {
                    let (state, logical) = sql.plan(tables, runtime).await?.into_parts();
                    let optimized = state
                        .optimize(&logical)
                        .context(PlanningSnafu)
                        .context(QuerySnafu)?;
                    // Use the same optimized plan for disclosure and physical planning.
                    let physical = state
                        .query_planner()
                        .create_physical_plan(&optimized, &state)
                        .await
                        .context(PlanningSnafu)
                        .context(QuerySnafu)?;
                    let bytes = self.encode_selection(&optimized, &physical, &bindings)?;
                    write_stdout(&bytes)?;
                    Ok(json!({
                        "format_version": 3, "kind": "completion", "command": "explain",
                        "output_bytes": bytes.len().to_string(),
                    }))
                }
                .await;
                match result {
                    Ok(completion) => Ok(completion),
                    Err(mut error) => {
                        if matches!(error, Error::Reader { .. } | Error::Query { .. }) {
                            for access in &authorized_access {
                                error = access.map_read_error(error).await;
                                if matches!(error, Error::UnityCatalog { .. }) {
                                    break;
                                }
                            }
                        }
                        Err(Error::WithResolvedBindings {
                            source: Box::new(error),
                            bindings,
                        })
                    }
                }
            }
            .boxed_local(),
        ))
    }

    fn encode_selection(
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
mod tests {
    use super::*;
    use arrow_schema::Schema;
    use datafusion::physical_plan::{
        coalesce_partitions::CoalescePartitionsExec, empty::EmptyExec,
    };

    #[test]
    fn unknown_and_estimated_statistics_never_appear_exact() {
        assert_eq!(
            describe_precision(&Precision::Absent),
            json!({"precision": "unknown"})
        );
        assert_eq!(
            describe_precision(&Precision::Inexact(0)),
            json!({"precision": "estimated", "value": "0"})
        );
        assert_eq!(
            describe_precision(&Precision::Exact(0)),
            json!({"precision": "exact", "value": "0"})
        );
    }

    #[test]
    fn node_positions_follow_preorder_even_for_deep_plans() {
        let mut plan: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::new(Schema::empty())));
        for _ in 0..128 {
            plan = Arc::new(CoalescePartitionsExec::new(plan));
        }
        let nodes = index_physical_nodes(plan.as_ref());
        assert_eq!(nodes.len(), 129);
        for (index, node) in nodes.iter().enumerate() {
            assert_eq!(
                node.children,
                if index < 128 { vec![index + 1] } else { vec![] }
            );
        }
    }
}
