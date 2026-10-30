//! Query execution and plan inspection over named, immutable Core snapshots.

mod error;
pub(crate) mod explain;
mod input;
pub(crate) mod resources;
mod sql;

use std::{num::NonZeroUsize, path::PathBuf};

use crate::output::RowOutputFormat;
use clap::Args;
use futures_util::{FutureExt, TryStreamExt, future::LocalBoxFuture};
use serde_json::{Value, json};

use crate::{
    cli::parse_unsigned_decimal,
    diagnostics::{Error, build_completion_record, build_json_read_completion},
    output,
    table_access::TableLoadRequest,
};

pub(crate) use error::QueryError;
pub(crate) use input::QueryInputArgs;
pub(crate) use sql::{MAX_SQL_BYTES, ReadOnlySqlQuery, SQL_RECURSION_LIMIT};

/// Executes a declared query within explicit result and runtime resource ceilings.
#[derive(Args, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct QueryArgs {
    #[command(flatten)]
    #[serde(flatten)]
    pub(crate) input: QueryInputArgs,
    /// Arrow IPC stream or typed JSON; redirect stdout for Arrow unless using --output.
    #[arg(long = "format", value_enum, default_value = "arrow")]
    #[serde(rename = "format")]
    pub(crate) output_format: RowOutputFormat,
    /// Save one complete result without replacing an existing file.
    /// Stdout returns a small JSON receipt. Windows staging inherits the directory ACL.
    #[arg(long, value_name = "PATH")]
    #[serde(skip)]
    pub(crate) output: Option<PathBuf>,
    /// Also save PATH.manifest.json with the sources and options needed for replay.
    ///
    /// Replay details: capabilities --schema result-manifest --detail replay.
    #[arg(long, requires = "output")]
    #[serde(skip)]
    pub(crate) manifest: bool,
    /// Maximum result rows. Exceeding this ceiling fails; SQL LIMIT selects a smaller result.
    #[arg(long, default_value = "1000000", value_name = "N", value_parser = parse_unsigned_decimal::<usize>)]
    pub(crate) max_rows: usize,
    /// Complete result byte ceiling. Defaults: JSON stdout 8192, JSON file 16777216, Arrow 1073741824.
    #[arg(long, value_name = "BYTES", value_parser = parse_unsigned_decimal::<NonZeroUsize>)]
    pub(crate) max_output_bytes: Option<NonZeroUsize>,
    /// DataFusion memory-pool ceiling in bytes; reader/metadata buffers and process RSS are outside it.
    #[arg(long, default_value = "268435456", value_name = "BYTES", value_parser = parse_unsigned_decimal::<NonZeroUsize>)]
    pub(crate) memory_limit_bytes: NonZeroUsize,
    /// DataFusion spill budget in bytes. Zero disables spill; positive values require --spill-dir.
    /// Checked after writes, so temporary usage can exceed this budget before failure.
    #[arg(long, default_value = "0", value_name = "BYTES", value_parser = parse_unsigned_decimal::<u64>)]
    pub(crate) spill_limit_bytes: u64,
    /// Existing writable directory for query temporary files; requires a positive spill budget.
    /// Files may contain table data. On Windows, choose a directory with a private inherited ACL.
    #[arg(long, value_name = "PATH")]
    spill_dir: Option<PathBuf>,
    /// Whole-command deadline in milliseconds, including inputs, planning and output.
    #[arg(long, default_value = "300000", value_name = "N", value_parser = parse_unsigned_decimal::<NonZeroUsize>)]
    pub(crate) deadline_ms: NonZeroUsize,
}

impl QueryArgs {
    pub(crate) fn output_byte_limit(&self) -> usize {
        self.max_output_bytes.map(NonZeroUsize::get).unwrap_or(
            match (self.output_format, self.output.is_some()) {
                (RowOutputFormat::Json, false) => output::json::DEFAULT_MAX_BYTES,
                (RowOutputFormat::Json, true) => 16 * 1024 * 1024,
                (RowOutputFormat::Arrow, _) => 1024 * 1024 * 1024,
            },
        )
    }

    /// Admits all input before any table IO; execution loads each snapshot exactly once.
    /// Returns whether UC cancellation reporting applies, followed by the pending execution.
    pub(crate) fn prepare_execution<'a>(
        &'a self,
        spill_directory: &mut Option<tempfile::TempDir>,
        pending_artifact: &'a mut Option<output::artifact::PendingArtifactPublication>,
        cancellation: &futures_util::future::AbortHandle,
    ) -> Result<(bool, LocalBoxFuture<'a, Result<Value, Error>>), Error> {
        let mut artifact = self
            .output
            .as_ref()
            .map(|path| {
                let options = if self.manifest {
                    let mut options = serde_json::to_value(self)
                        .map_err(|source| Error::JsonOutput { source })?;
                    options["max-output-bytes"] = json!(self.output_byte_limit());
                    Some(options)
                } else {
                    None
                };
                output::artifact::StagedResult::new(
                    path,
                    self.output_format,
                    options,
                    cancellation.clone(),
                )
            })
            .transpose()?;
        let (requests, sql_query) = self.input.prepare_inputs(&mut artifact)?;
        let (runtime, directory) = resources::build_query_runtime(
            self.memory_limit_bytes,
            self.spill_limit_bytes,
            self.spill_dir.as_deref(),
        )?;
        *spill_directory = directory;
        let uses_unity_catalog = requests
            .iter()
            .any(|(_, request)| matches!(request, TableLoadRequest::UnityCatalog { .. }));
        let execution = async move {
            let mut tables = Vec::with_capacity(requests.len());
            let mut authorized_access = Vec::new();
            let mut bindings = Vec::with_capacity(requests.len());
            for (alias, request) in requests {
                let (table, access) = request.load_table().await?;
                if self.manifest
                    && let Some(access) = &access
                {
                    access
                        .validate_versioned_replay()
                        .map_err(|source| Error::UnityCatalog { source })?;
                }
                bindings.push(crate::table_access::resolved_snapshot_binding(
                    &alias,
                    table.version(),
                    access.as_ref(),
                ));
                authorized_access.extend(access);
                tables.push((alias, table));
            }
            let query_result = async {
                let stream = sql_query.execute_stream(tables, runtime).await?;
                let schema = stream.schema();
                let stream = stream.map_err(|source| Error::Query {
                    source: QueryError::Execution { source },
                });
                let limits = output::OutputLimits {
                    max_rows: self.max_rows as u64,
                    max_bytes: self.output_byte_limit() as u64,
                };
                if let Some(staged) = artifact.take() {
                    let (staged, stats) = staged
                        .write_rows(schema, stream, bindings.clone(), Some(limits))
                        .await?;
                    artifact = Some(staged);
                    return Ok(stats);
                }
                match self.output_format {
                    RowOutputFormat::Arrow => {
                        output::arrow::write_arrow_stream_to_stdout(schema, stream, Some(limits))
                            .await
                    }
                    RowOutputFormat::Json => {
                        output::json::write_query_to_stdout(
                            schema,
                            stream,
                            bindings.clone(),
                            limits,
                        )
                        .await
                    }
                }
            }
            .await;
            let query_result = match query_result {
                Err(mut error @ (Error::Reader { .. } | Error::Query { .. })) => {
                    for access in &authorized_access {
                        error = access.map_read_error(error).await;
                        if matches!(error, Error::UnityCatalog { .. }) {
                            break;
                        }
                    }
                    Err(error)
                }
                result => result,
            };
            let result_stats = query_result.map_err(|source| Error::WithResolvedBindings {
                source: Box::new(source),
                bindings: bindings.clone(),
            })?;
            if let Some(staged) = artifact {
                let mut completion = build_completion_record("query", None, result_stats, None);
                completion["bindings"] = bindings.into();
                *pending_artifact =
                    Some(staged.prepare_publication(&completion).map_err(|source| {
                        Error::WithResolvedBindings {
                            source: Box::new(source),
                            bindings: completion["bindings"]
                                .as_array()
                                .cloned()
                                .unwrap_or_default(),
                        }
                    })?);
                return Ok(completion);
            }
            let completion = match self.output_format {
                RowOutputFormat::Arrow => {
                    let mut completion = build_completion_record("query", None, result_stats, None);
                    completion["bindings"] = bindings.into();
                    completion
                }
                RowOutputFormat::Json => {
                    build_json_read_completion("query", bindings, result_stats)
                }
            };
            Ok(completion)
        }
        .boxed_local();
        Ok((uses_unity_catalog, execution))
    }
}
