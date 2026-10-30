//! Row reads shared by Arrow scans and typed JSON previews.

use futures_util::{TryStreamExt, future::AbortHandle};
use serde_json::{Value, json};
use snafu::ResultExt;

use super::preview::PreviewArgs;
use crate::{
    cli::Command,
    diagnostics::{
        ConfigurationSnafu, Error, ReaderSnafu, build_completion_record, build_json_read_completion,
    },
    input::{self, JsonInputKind},
    output,
    table_access::unity_catalog::UnityCatalogTableTarget,
};

pub(super) async fn execute(
    command: &Command,
    uc_target: Option<UnityCatalogTableTarget>,
    pending_artifact: &mut Option<output::artifact::PendingArtifactPublication>,
    cancellation: &AbortHandle,
) -> Result<Value, Error> {
    let read = match command {
        Command::Scan { read, .. } | Command::Preview(PreviewArgs { read, .. }) => read,
        _ => unreachable!("row reader command"),
    };
    let mut artifact = match command {
        Command::Scan {
            read,
            output: Some(path),
            manifest,
            limit,
            deadline_ms,
            max_rows,
            max_output_bytes,
        } => {
            let options = if *manifest {
                let mut options =
                    serde_json::to_value(read).map_err(|source| Error::JsonOutput { source })?;
                options["limit"] = json!(limit);
                options["deadline-ms"] = json!(deadline_ms);
                options["max-rows"] = json!(max_rows);
                options["max-output-bytes"] = json!(max_output_bytes);
                Some(options)
            } else {
                None
            };
            Some(output::artifact::StagedResult::new(
                path,
                output::RowOutputFormat::Arrow,
                options,
                cancellation.clone(),
            )?)
        }
        _ => None,
    };
    let predicate = match read.predicate_file.as_deref() {
        Some(path) => {
            let (value, bytes) = input::read_json_file_with_bytes::<input::PredicateInput>(
                path,
                JsonInputKind::Predicate,
            )?;
            if let Some(artifact) = &mut artifact {
                artifact.record_replay_input("--predicate-file", path, &bytes);
            }
            Some(value.into_predicate()?)
        }
        _ => None,
    };
    let execution_options = match read.execution_options_file.as_deref() {
        Some(path) => {
            let (input::JsonObject(options), bytes) = input::read_json_file_with_bytes::<
                input::JsonObject<input::ExecutionOptionsInput>,
            >(
                path, JsonInputKind::ExecutionOptions
            )?;
            if let Some(artifact) = &mut artifact {
                artifact.record_replay_input("--execution-options-file", path, &bytes);
            }
            Some(
                options
                    .into_execution_options()
                    .context(ConfigurationSnafu)?,
            )
        }
        _ => None,
    };
    let (table, authorized_access) = read.table.load_table(uc_target).await?;
    if matches!(command, Command::Scan { manifest: true, .. })
        && let Some(access) = &authorized_access
    {
        access
            .validate_versioned_replay()
            .map_err(|source| Error::UnityCatalog { source })?;
    }
    let version = table.version();
    let binding =
        crate::table_access::resolved_snapshot_binding("t", version, authorized_access.as_ref());
    let limit = match command {
        Command::Scan { limit, .. } => *limit,
        Command::Preview(args) => Some(args.limit),
        _ => unreachable!("row reader command"),
    };
    let mut builder = table.scan();
    if read.no_columns || !read.columns.is_empty() {
        builder = builder.with_projection(read.columns.iter().cloned());
    }
    if let Some(limit) = limit {
        builder = builder.with_limit(limit);
    }
    if let Some(predicate) = predicate {
        builder = builder.with_predicate(predicate);
    }
    if let Some(execution_options) = execution_options {
        builder = builder.with_execution_options(execution_options);
    }
    if let Some(target_partitions) = read.target_partitions {
        builder = builder
            .with_target_partitions(target_partitions.get())
            .context(ConfigurationSnafu)?;
    }
    let scan_result = async {
        let scan = builder.build().await.context(ReaderSnafu)?;
        let schema = scan.schema();
        let stream = scan
            .into_stream()
            .map_err(|source| Error::Reader { source });
        if let Command::Preview(args) = command {
            let stats = output::json::write_preview_to_stdout(
                schema,
                stream,
                binding.clone(),
                output::OutputLimits {
                    max_rows: args.limit as u64,
                    max_bytes: args.max_output_bytes.get() as u64,
                },
            )
            .await?;
            Ok(build_json_read_completion(
                "preview",
                vec![binding.clone()],
                stats,
            ))
        } else if let Some(staged) = artifact.take() {
            let (staged, stats) = staged
                .write_rows(
                    schema,
                    stream,
                    Vec::new(),
                    match command {
                        Command::Scan {
                            max_rows: Some(rows),
                            max_output_bytes: Some(bytes),
                            ..
                        } => Some(output::OutputLimits {
                            max_rows: *rows as u64,
                            max_bytes: bytes.get() as u64,
                        }),
                        _ => None,
                    },
                )
                .await?;
            let mut completion = build_completion_record("scan", Some(version), stats, limit);
            completion["bindings"] = json!([binding]);
            *pending_artifact = Some(staged.prepare_publication(&completion)?);
            Ok(completion)
        } else {
            let stats = output::arrow::write_arrow_stream_to_stdout(schema, stream, None).await?;
            let mut completion = build_completion_record("scan", Some(version), stats, limit);
            completion["bindings"] = json!([binding]);
            Ok(completion)
        }
    }
    .await;
    let result = match (scan_result, &authorized_access) {
        (Err(error @ Error::Reader { .. }), Some(access)) => {
            Err(access.map_read_error(error).await)
        }
        (result, _) => result,
    };
    if matches!(
        command,
        Command::Scan {
            output: Some(_),
            ..
        }
    ) {
        result.map_err(|source| Error::WithResolvedBindings {
            source: Box::new(source),
            bindings: vec![binding],
        })
    } else {
        result
    }
}
