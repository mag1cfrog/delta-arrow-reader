//! Inspect a query's planned work before deciding whether to execute it.
//!
//! The default response identifies loaded snapshots and the available selections.
//! For example, --node 0 describes the root operation, --field 0 its first output
//! type, and --scan 0 the first logical scan's projection and filter pushdown.
//! Physical node IDs and logical scan IDs are separate, zero-based preorder lists.

mod selection;

use std::num::NonZeroUsize;

use clap::Args;
use futures_util::{FutureExt, future::LocalBoxFuture};
use serde_json::{Value, json};
use snafu::ResultExt;

use super::{QueryInputArgs, error::PlanningSnafu, resources};
use crate::{
    cli::parse_unsigned_decimal,
    diagnostics::{Error, QuerySnafu},
    output::write_stdout,
    table_access::TableLoadRequest,
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
}
