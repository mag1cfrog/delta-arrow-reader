//! Read-only SQL admission and planning over named, immutable Core snapshots.

pub(crate) mod explain;
pub(crate) mod resources;
pub(crate) mod tables_manifest;

use std::{fmt, fs::File, io::Read, num::NonZeroUsize, ops::ControlFlow, path::PathBuf, sync::Arc};

use crate::output::RowOutputFormat;
use clap::Args;
use datafusion::{
    common::{DataFusionError, SchemaError},
    dataframe::DataFrame,
    execution::{
        context::{SQLOptions, SessionContext},
        runtime_env::RuntimeEnv,
    },
    physical_plan::SendableRecordBatchStream,
    prelude::SessionConfig,
    sql::{
        parser::{DFParserBuilder, Statement},
        sqlparser::ast::{
            Expr, Query, Select, SetExpr, Statement as SqlStatement, TableFactor, Visit, Visitor,
        },
    },
};
use delta_arrow_reader::{
    DeltaTable,
    datafusion::{ScanOptions, register_table},
    error_source_kind,
};
use futures_util::{FutureExt, TryStreamExt, future::LocalBoxFuture};
use serde_json::{Value, json};
use snafu::{ResultExt, Snafu};

use crate::{
    Error, InputIoSnafu, QuerySnafu, UnityCatalogSnafu, build_completion_record,
    build_json_read_completion,
    diagnostics::ErrorClassification,
    output, parse_unsigned_decimal,
    table_input::{TableLoadRequest, TableOptions},
    unity_catalog::UnityCatalogTableTarget,
};

pub(crate) const MAX_SQL_BYTES: u64 = 64 * 1024;
pub(crate) const SQL_RECURSION_LIMIT: usize = 128;

/// SQL and snapshot declarations shared by query execution and plan inspection.
#[derive(Args, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct QueryInputArgs {
    /// UTF-8 local path or storage URL, registered as t. Relative paths use the working directory.
    /// UC targets use uc://catalog.schema.table and require --databricks-profile.
    /// Use '--' before a table path beginning with '-'.
    #[arg(required_unless_present = "tables_file")]
    pub(crate) table: Option<String>,
    /// Local JSON manifest of 1-64 named tables; at most 1 MiB. Relative locations use its directory.
    /// Conflicts with TABLE, --table-version, --storage-options-file, --env-file, --auth-source, and --databricks-profile.
    #[arg(long, value_name = "PATH", conflicts_with_all = ["table", "table_version", "storage_options_file", "env_file", "auth_source", "databricks_profile"])]
    tables_file: Option<String>,
    #[command(flatten)]
    #[serde(flatten)]
    pub(crate) table_options: TableOptions,
    /// Existing Databricks CLI profile for positional uc:// TABLE; login must already be configured.
    #[arg(long, value_name = "PROFILE")]
    databricks_profile: Option<String>,
    /// UTF-8 file containing one read-only SQL query over the selected aliases; at most 64 KiB.
    #[arg(long, value_name = "PATH")]
    pub(crate) sql_file: String,
}

impl QueryInputArgs {
    /// Validates all bindings and SQL before loading any snapshot.
    pub(crate) fn prepare_inputs(
        &self,
        artifact: &mut Option<output::artifact::StagedResult>,
    ) -> Result<(Vec<(String, TableLoadRequest)>, ReadOnlySqlQuery), Error> {
        let requests = if let Some(path) = &self.tables_file {
            tables_manifest::read_table_requests(path, artifact)?
        } else {
            let location = self.table.as_deref().ok_or(Error::Argument {
                kind: clap::error::ErrorKind::MissingRequiredArgument,
                correction: None,
            })?;
            let uc_target =
                UnityCatalogTableTarget::parse(location, self.databricks_profile.as_deref())
                    .context(UnityCatalogSnafu)?;
            vec![(
                "t".to_owned(),
                self.table_options.prepare_load(location, uc_target, None)?,
            )]
        };
        let (sql_query, bytes) = ReadOnlySqlQuery::read_file_with_bytes(&self.sql_file)?;
        if let Some(artifact) = artifact {
            artifact.record_replay_input("--sql-file", &self.sql_file, &bytes);
        }
        Ok((requests, sql_query))
    }
}

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
            for (alias, request) in requests {
                let (table, access) = request.load_table().await?;
                authorized_access.extend(access);
                tables.push((alias, table));
            }
            let bindings: Vec<_> = tables
                .iter()
                .map(|(alias, table)| json!({"alias": alias, "table_version": table.version().to_string()}))
                .collect();
            let query_result = async {
                let stream = sql_query
                    .execute_stream(tables, runtime)
                    .await?;
                let schema = stream.schema();
                let stream = stream.map_err(|source| Error::Query {
                    source: QueryError::Execution { source },
                });
                let limits = output::OutputLimits {
                    max_rows: self.max_rows as u64,
                    max_bytes: self.output_byte_limit() as u64,
                };
                if let Some(staged) = artifact.take() {
                    let (staged, stats) = staged.write_rows(schema, stream, bindings.clone(), Some(limits)).await?;
                    artifact = Some(staged);
                    return Ok(stats);
                }
                match self.output_format {
                    RowOutputFormat::Arrow => {
                        output::arrow::write_arrow_stream_to_stdout(schema, stream, Some(limits))
                            .await
                    }
                    RowOutputFormat::Json => {
                        output::json::write_query_to_stdout(schema, stream, bindings.clone(), limits)
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
                *pending_artifact = Some(staged.prepare_publication(&completion).map_err(|source| Error::WithResolvedBindings {
                    source: Box::new(source), bindings: completion["bindings"].as_array().cloned().unwrap_or_default(),
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
        }.boxed_local();
        Ok((uses_unity_catalog, execution))
    }
}

/// SQL failures retain typed sources without printing query text or dependency messages.
#[derive(Snafu)]
pub(crate) enum QueryError {
    #[snafu(display(
        "Invalid SQL input (reason={reason}). Use one read-only query over the selected tables."
    ))]
    Input { reason: &'static str },
    #[snafu(display("Could not create private temporary storage in --spill-dir."))]
    SpillDirectory { source: std::io::Error },
    #[snafu(display("Could not remove the query's temporary spill files."))]
    SpillCleanup { source: std::io::Error },
    #[snafu(display("Could not initialize the SQL runtime."))]
    Initialization { source: DataFusionError },
    #[snafu(display("Could not plan the SQL query."))]
    Planning { source: DataFusionError },
    #[snafu(display("Could not execute the SQL query."))]
    Execution { source: DataFusionError },
}

impl fmt::Debug for QueryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl QueryError {
    pub(crate) fn classification(&self) -> ErrorClassification {
        match self {
            Self::Input { reason } => ErrorClassification {
                phase: "configuration",
                code: "invalid_sql_input",
                reason,
                source_kind: None,
            },
            Self::SpillDirectory { source } | Self::SpillCleanup { source } => {
                let (phase, code) = if matches!(self, Self::SpillDirectory { .. }) {
                    ("configuration", "spill_directory")
                } else {
                    ("datafusion", "spill_cleanup")
                };
                ErrorClassification {
                    phase,
                    code,
                    reason: code,
                    source_kind: Some(error_source_kind(source)),
                }
            }
            Self::Initialization { source }
            | Self::Planning { source }
            | Self::Execution { source } => {
                let code = match self {
                    Self::Initialization { .. } => "sql_initialization",
                    Self::Planning { .. } => "sql_planning",
                    _ => "sql_execution",
                };
                let source_kind =
                    if matches!(source.find_root(), DataFusionError::ResourcesExhausted(_)) {
                        "resource_exhausted"
                    } else {
                        error_source_kind(source)
                    };
                let reason = match source.find_root() {
                    DataFusionError::SchemaError(error, _)
                        if matches!(self, Self::Planning { .. }) =>
                    {
                        match error.as_ref() {
                            SchemaError::FieldNotFound { .. } => "column_not_found",
                            SchemaError::AmbiguousReference { .. } => "ambiguous_column",
                            _ => code,
                        }
                    }
                    // DataFusion 54 uses one string variant for both budgets. Unknown messages stay unclassified.
                    DataFusionError::ResourcesExhausted(message)
                        if message.starts_with("The used disk space during the spilling process has exceeded the allowable limit of ") => "spill_limit_exceeded",
                    DataFusionError::ResourcesExhausted(message)
                        if message.starts_with("Memory Exhausted while ")
                            && message.ends_with("(DiskManager is disabled)") => "spill_disabled",
                    DataFusionError::ResourcesExhausted(message)
                        if message.starts_with("Failed to allocate additional ") => "memory_limit_exceeded",
                    _ => code,
                };
                ErrorClassification {
                    phase: "datafusion",
                    code,
                    reason,
                    source_kind: Some(source_kind),
                }
            }
        }
    }

    pub(crate) fn exit_status(&self) -> u8 {
        match self {
            Self::Input { .. } | Self::SpillDirectory { .. } => 2,
            _ => 1,
        }
    }
}

/// Parsed and admitted SQL. The AST is kept private because it contains user literals.
pub(crate) struct ReadOnlySqlQuery {
    statement: Statement,
}

impl ReadOnlySqlQuery {
    /// Rejects malformed or disallowed SQL before opening the requested Delta table.
    pub(crate) fn read_file_with_bytes(path: &str) -> Result<(Self, Vec<u8>), crate::Error> {
        let file = File::open(path).context(InputIoSnafu {
            argument: "--sql-file",
        })?;
        let mut bytes = Vec::new();
        file.take(MAX_SQL_BYTES + 1)
            .read_to_end(&mut bytes)
            .context(InputIoSnafu {
                argument: "--sql-file",
            })?;
        if bytes.len() as u64 > MAX_SQL_BYTES {
            return Err(QueryError::Input {
                reason: "sql_file_too_large",
            })
            .context(QuerySnafu);
        }
        let sql = std::str::from_utf8(&bytes)
            .map_err(|_| QueryError::Input {
                reason: "sql_not_utf8",
            })
            .context(QuerySnafu)?;
        let query = Self::parse(sql).context(QuerySnafu)?;
        Ok((query, bytes))
    }

    fn parse(sql: &str) -> Result<Self, QueryError> {
        let mut statements = DFParserBuilder::new(sql)
            .with_recursion_limit(SQL_RECURSION_LIMIT)
            .build()
            .and_then(|mut parser| parser.parse_statements())
            .map_err(|_| QueryError::Input {
                reason: "sql_parse_failed",
            })?;
        if statements.len() != 1 {
            return Err(QueryError::Input {
                reason: "sql_requires_one_statement",
            });
        }
        let statement = statements.pop_front().ok_or(QueryError::Input {
            reason: "sql_requires_one_statement",
        })?;
        let Statement::Statement(sql_statement) = &statement else {
            return Err(QueryError::Input {
                reason: "sql_not_read_only",
            });
        };
        if let ControlFlow::Break(reason) =
            sql_statement.visit(&mut ReadOnlySqlValidator::default())
        {
            return Err(QueryError::Input { reason });
        }
        Ok(Self { statement })
    }

    /// Registers only the supplied names and snapshots. URL-based table discovery stays disabled.
    pub(crate) async fn plan(
        self,
        tables: impl IntoIterator<Item = (String, DeltaTable)>,
        runtime: Arc<RuntimeEnv>,
    ) -> Result<DataFrame, crate::Error> {
        let context = SessionContext::new_with_config_rt(SessionConfig::new(), runtime);
        for (alias, table) in tables {
            register_table(&context, alias, table, ScanOptions::default())
                .context(crate::ReaderSnafu)?;
        }
        let plan = context
            .state()
            .statement_to_plan(self.statement)
            .await
            .context(PlanningSnafu)
            .context(QuerySnafu)?;
        SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false)
            .with_allow_statements(false)
            .verify_plan(&plan)
            .context(PlanningSnafu)
            .context(QuerySnafu)?;
        context
            .execute_logical_plan(plan)
            .await
            .context(PlanningSnafu)
            .context(QuerySnafu)
    }

    pub(crate) async fn execute_stream(
        self,
        tables: impl IntoIterator<Item = (String, DeltaTable)>,
        runtime: Arc<RuntimeEnv>,
    ) -> Result<SendableRecordBatchStream, crate::Error> {
        self.plan(tables, runtime)
            .await?
            .execute_stream()
            .await
            .context(ExecutionSnafu)
            .context(QuerySnafu)
    }
}

/// Checks nested queries too: a top-level SELECT can contain recursive CTEs or SELECT INTO.
#[derive(Default)]
struct ReadOnlySqlValidator {
    expression_depth: usize,
}

impl Visitor for ReadOnlySqlValidator {
    type Break = &'static str;

    fn pre_visit_expr(&mut self, _: &Expr) -> ControlFlow<Self::Break> {
        // Parser recursion does not bound the AST depth of chains such as 1 + 1 + 1.
        self.expression_depth += 1;
        if self.expression_depth > SQL_RECURSION_LIMIT {
            ControlFlow::Break("sql_depth_limit_exceeded")
        } else {
            ControlFlow::Continue(())
        }
    }

    fn post_visit_expr(&mut self, _: &Expr) -> ControlFlow<Self::Break> {
        self.expression_depth -= 1;
        ControlFlow::Continue(())
    }

    fn pre_visit_statement(&mut self, statement: &SqlStatement) -> ControlFlow<Self::Break> {
        if matches!(statement, SqlStatement::Query(_)) {
            ControlFlow::Continue(())
        } else {
            ControlFlow::Break("sql_not_read_only")
        }
    }

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
        // UNION chains also grow an AST without increasing parser recursion.
        let mut bodies = vec![(query.body.as_ref(), 1)];
        while let Some((body, depth)) = bodies.pop() {
            if depth > SQL_RECURSION_LIMIT {
                return ControlFlow::Break("sql_depth_limit_exceeded");
            }
            if let SetExpr::SetOperation { left, right, .. } = body {
                bodies.push((right.as_ref(), depth + 1));
                bodies.push((left.as_ref(), depth + 1));
            }
        }
        if query.with.as_ref().is_some_and(|with| with.recursive) {
            ControlFlow::Break("recursive_sql_not_supported")
        } else if !query.locks.is_empty()
            || query.settings.is_some()
            || !query.pipe_operators.is_empty()
        {
            ControlFlow::Break("sql_not_read_only")
        } else {
            ControlFlow::Continue(())
        }
    }

    fn pre_visit_select(&mut self, select: &Select) -> ControlFlow<Self::Break> {
        if select.into.is_some() {
            ControlFlow::Break("sql_not_read_only")
        } else {
            ControlFlow::Continue(())
        }
    }

    fn pre_visit_table_factor(&mut self, table: &TableFactor) -> ControlFlow<Self::Break> {
        if matches!(
            table,
            TableFactor::Table { args: Some(_), .. }
                | TableFactor::Function { .. }
                | TableFactor::TableFunction { .. }
        ) {
            ControlFlow::Break("sql_table_functions_not_supported")
        } else {
            ControlFlow::Continue(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::{array::Int32Array, compute::concat_batches};
    use delta_arrow_reader::{DeltaTableBuilder, WarmupMode};
    use futures_util::TryStreamExt;

    #[tokio::test]
    async fn joins_use_distinct_snapshots_and_respect_deletion_vectors()
    -> Result<(), Box<dyn std::error::Error>> {
        let corpus = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../delta-arrow-reader-core/tests/reader/fixtures/external_writer/corpus"
        );
        let mut tables = Vec::new();
        for name in ["partitioned", "deletion_vectors"] {
            let table = DeltaTableBuilder::new(format!("{corpus}/{name}/table"))
                .with_warmup(WarmupMode::None)
                .load_table()
                .await?;
            tables.push((name.to_owned(), table));
        }
        let query = ReadOnlySqlQuery::parse(
            "SELECT p.id AS partitioned_id, d.id AS retained_id
             FROM partitioned AS p LEFT JOIN deletion_vectors AS d ON p.id = d.id
             WHERE p.id <= 12 ORDER BY p.id",
        )?;
        let runtime = datafusion::execution::runtime_env::RuntimeEnvBuilder::new().build_arc()?;
        let stream = query.execute_stream(tables, runtime).await?;
        let schema = stream.schema();
        let batches = stream.try_collect::<Vec<_>>().await?;
        let actual = concat_batches(&schema, &batches)?;
        assert_eq!(schema.field(0).name(), "partitioned_id");
        assert_eq!(schema.field(1).name(), "retained_id");
        assert_eq!(actual.num_columns(), 2);
        assert_eq!(
            actual.column(0).as_ref(),
            &Int32Array::from_iter_values(1..=12)
        );
        // The independent Spark fixture deleted IDs 2, 5, and 12 from the right table.
        assert_eq!(
            actual.column(1).as_ref(),
            &Int32Array::from(vec![
                Some(1),
                None,
                Some(3),
                Some(4),
                None,
                Some(6),
                Some(7),
                Some(8),
                Some(9),
                Some(10),
                Some(11),
                None,
            ])
        );
        Ok(())
    }

    #[test]
    fn admission_checks_nested_queries_and_one_read_only_statement() {
        for sql in [
            "SELECT count(*) FROM t",
            "WITH selected AS (SELECT id FROM t) SELECT id FROM selected ORDER BY id",
            "SELECT '; COPY t TO secret' AS text FROM t LIMIT 0",
            "SELECT id FROM t UNION ALL SELECT id FROM t",
        ] {
            assert!(ReadOnlySqlQuery::parse(sql).is_ok(), "{sql}");
        }
        for sql in [
            "",
            "-- only a comment",
            "SELECT 1; SELECT 2",
            "SELECT FROM",
            "CREATE TABLE secret (id INT)",
            "INSERT INTO t VALUES (1)",
            "DELETE FROM t",
            "UPDATE t SET id = 2",
            "COPY t TO 'secret'",
            "SET target_partitions = 1",
            "EXPLAIN ANALYZE SELECT * FROM t",
            "SELECT * INTO secret FROM t",
            "SELECT * FROM t FOR UPDATE",
            "WITH RECURSIVE x AS (SELECT 1 UNION ALL SELECT * FROM x) SELECT * FROM x",
            "SELECT * FROM (WITH RECURSIVE x AS (SELECT 1) SELECT * FROM x) nested",
            "SELECT * FROM read_csv('secret.csv')",
            "SELECT * FROM read_parquet('https://secret/file.parquet')",
        ] {
            assert!(ReadOnlySqlQuery::parse(sql).is_err(), "{sql}");
        }
        let nested = format!("SELECT {}1{}", "(".repeat(200), ")".repeat(200));
        assert!(ReadOnlySqlQuery::parse(&nested).is_err());
    }

    #[test]
    fn dependency_errors_are_redacted_and_memory_exhaustion_is_typed() {
        let error = QueryError::Execution {
            source: DataFusionError::Context(
                "secret SQL".into(),
                Box::new(DataFusionError::ResourcesExhausted(
                    "secret query literal".into(),
                )),
            ),
        };
        assert_eq!(
            error.classification().source_kind,
            Some("resource_exhausted")
        );
        assert!(!format!("{error} {error:?}").contains("secret"));
        assert_eq!(error.classification().reason, "sql_execution");
        assert!(Error::Query { source: error }.input_correction().is_none());
        let unrelated = QueryError::Execution {
            source: DataFusionError::Execution("Failed to allocate additional secret".into()),
        };
        assert_eq!(unrelated.classification().reason, "sql_execution");
    }

    #[test]
    fn sql_column_diagnostics_keep_typed_causes_and_redact_sources() {
        use datafusion::common::{Column, Diagnostic};

        for (source, reason) in [
            (
                SchemaError::FieldNotFound {
                    field: Box::new(Column::new(Some("secret_alias"), "secret_column")),
                    valid_fields: vec![Column::new(Some("secret_alias"), "secret_valid_column")],
                },
                "column_not_found",
            ),
            (
                SchemaError::AmbiguousReference {
                    field: Box::new(Column::new_unqualified("secret_column")),
                },
                "ambiguous_column",
            ),
        ] {
            let source = DataFusionError::SchemaError(
                Box::new(source),
                Box::new(Some("secret backtrace".into())),
            )
            .with_diagnostic(Diagnostic::new_error("secret diagnostic", None));
            let error = Error::Query {
                source: QueryError::Planning {
                    source: DataFusionError::Context(
                        "secret SQL https://user:secret@host/table?token=secret".into(),
                        Box::new(source),
                    ),
                },
            };
            let record = error.to_json_diagnostic();
            assert_eq!(record["code"], "sql_planning");
            assert_eq!(record["reason"], reason);
            assert_eq!(record["source_kind"], "schema");
            assert_eq!(record["recovery"], "fix_input");
            assert_eq!(record["retryable"], false);
            assert_eq!(record["argument"], "--sql-file");
            let text = crate::diagnostics::capture_tracing_output("warn", || {
                error.emit_tracing_diagnostic();
            });
            assert!(text.contains(&format!("error_reason=\"{reason}\"")));
            assert!(text.contains(record["hint"].as_str().unwrap()));
            for rendered in [record.to_string(), text, format!("{error:?}")] {
                assert!(!rendered.contains("secret"), "{rendered}");
            }
        }
    }

    #[test]
    fn other_sql_errors_do_not_offer_column_repairs() {
        for source in [
            DataFusionError::Plan("No field named secret_column".into()),
            DataFusionError::SchemaError(
                Box::new(SchemaError::DuplicateUnqualifiedField {
                    name: "secret_column".into(),
                }),
                Box::new(None),
            ),
        ] {
            let record = Error::Query {
                source: QueryError::Planning { source },
            }
            .to_json_diagnostic();
            assert_eq!(record["reason"], "sql_planning");
            assert_eq!(record["recovery"], "unknown");
            assert!(record["retryable"].is_null());
            assert!(record.get("argument").is_none());
            assert!(record.get("hint").is_none());
            assert!(!record.to_string().contains("secret"));
        }
        let error = Error::Query {
            source: QueryError::Execution {
                source: DataFusionError::SchemaError(
                    Box::new(SchemaError::FieldNotFound {
                        field: Box::new(datafusion::common::Column::new_unqualified(
                            "secret_column",
                        )),
                        valid_fields: Vec::new(),
                    }),
                    Box::new(None),
                ),
            },
        };
        assert_eq!(error.to_json_diagnostic()["reason"], "sql_execution");
        assert!(error.input_correction().is_none());
    }
}
