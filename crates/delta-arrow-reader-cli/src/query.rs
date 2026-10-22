//! Read-only SQL admission and planning over named, immutable Core snapshots.

pub(crate) mod tables_manifest;

use std::{fmt, fs::File, io::Read, num::NonZeroUsize, ops::ControlFlow, sync::Arc};

use clap::{Args, ValueEnum};
use datafusion::{
    common::DataFusionError,
    execution::{
        context::{SQLOptions, SessionContext},
        disk_manager::{DiskManagerBuilder, DiskManagerMode},
        memory_pool::FairSpillPool,
        runtime_env::RuntimeEnvBuilder,
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

#[derive(Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum QueryOutputFormat {
    Arrow,
    Json,
}

/// Selects positional table t or a manifest of named tables, with result resource ceilings.
#[derive(Args)]
pub(crate) struct QueryArgs {
    /// UTF-8 local path or storage URL, registered as t. Relative paths use the working directory.
    /// UC targets use uc://catalog.schema.table and require --databricks-profile.
    /// Use '--' before a table path beginning with '-'.
    #[arg(required_unless_present = "tables_file")]
    table: Option<String>,
    /// Local JSON manifest of 1-64 named tables; at most 1 MiB. Relative locations use its directory.
    /// Conflicts with TABLE, --table-version, --storage-options-file, --auth-source, and --databricks-profile.
    #[arg(long, value_name = "PATH", conflicts_with_all = ["table", "table_version", "storage_options_file", "auth_source", "databricks_profile"])]
    tables_file: Option<String>,
    #[command(flatten)]
    table_options: TableOptions,
    /// Existing Databricks CLI profile for positional uc:// TABLE; login must already be configured.
    #[arg(long, value_name = "PROFILE")]
    databricks_profile: Option<String>,
    /// UTF-8 file containing one read-only SQL query over the selected aliases; at most 64 KiB.
    #[arg(long, value_name = "PATH")]
    pub(crate) sql_file: String,
    /// Arrow IPC stream (requires redirected stdout), or bounded typed JSON.
    #[arg(long = "format", value_enum, default_value = "arrow")]
    pub(crate) output_format: QueryOutputFormat,
    /// Maximum result rows. Exceeding this ceiling fails; SQL LIMIT selects a smaller result.
    #[arg(long, default_value = "1000000", value_name = "N", value_parser = parse_unsigned_decimal::<usize>)]
    pub(crate) max_rows: usize,
    /// Whole-response byte ceiling, including schema and framing. JSON defaults to 8192; excess fails.
    #[arg(long, default_value = "1073741824", default_value_if("output_format", "json", "8192"), value_name = "BYTES", value_parser = parse_unsigned_decimal::<NonZeroUsize>)]
    pub(crate) max_output_bytes: NonZeroUsize,
    /// DataFusion memory-pool ceiling in bytes; not a process RSS limit. Disk spill is disabled.
    #[arg(long, default_value = "268435456", value_name = "BYTES", value_parser = parse_unsigned_decimal::<NonZeroUsize>)]
    pub(crate) memory_limit_bytes: NonZeroUsize,
}

impl QueryArgs {
    /// Admits all input before any table IO; execution loads each snapshot exactly once.
    /// Returns whether UC cancellation reporting applies, followed by the pending execution.
    pub(crate) fn prepare_execution(
        &self,
    ) -> Result<(bool, LocalBoxFuture<'_, Result<Value, Error>>), Error> {
        let requests = if let Some(path) = &self.tables_file {
            tables_manifest::read_table_requests(path)?
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
        let sql_query = ReadOnlySqlQuery::read_file(&self.sql_file)?;
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
                    .execute_stream(tables, self.memory_limit_bytes.get())
                    .await?;
                let schema = stream.schema();
                let stream = stream.map_err(|source| Error::Query {
                    source: QueryError::Execution { source },
                });
                let limits = output::OutputLimits {
                    max_rows: self.max_rows as u64,
                    max_bytes: self.max_output_bytes.get() as u64,
                };
                match self.output_format {
                    QueryOutputFormat::Arrow => {
                        output::arrow::write_arrow_stream_to_stdout(schema, stream, Some(limits))
                            .await
                    }
                    QueryOutputFormat::Json => {
                        output::json::write_query_to_stdout(schema, stream, bindings.clone(), limits)
                            .await
                    }
                }
            }
            .await;
            let stdout_stats = match query_result {
                Err(mut error @ (Error::Reader { .. } | Error::Query { .. })) => {
                    for access in &authorized_access {
                        error = access.map_read_error(error).await;
                        if matches!(error, Error::UnityCatalog { .. }) {
                            break;
                        }
                    }
                    return Err(error);
                }
                result => result?,
            };
            let completion = match self.output_format {
                QueryOutputFormat::Arrow => {
                    let mut completion = build_completion_record("query", None, stdout_stats, None);
                    completion["bindings"] = bindings.into();
                    completion
                }
                QueryOutputFormat::Json => {
                    build_json_read_completion("query", bindings, stdout_stats)
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
                ErrorClassification {
                    phase: "datafusion",
                    code,
                    reason: code,
                    source_kind: Some(source_kind),
                }
            }
        }
    }

    pub(crate) fn exit_status(&self) -> u8 {
        match self {
            Self::Input { .. } => 2,
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
    pub(crate) fn read_file(path: &str) -> Result<Self, crate::Error> {
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
        Self::parse(sql).context(QuerySnafu)
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
    pub(crate) async fn execute_stream(
        self,
        tables: impl IntoIterator<Item = (String, DeltaTable)>,
        memory_limit_bytes: usize,
    ) -> Result<SendableRecordBatchStream, crate::Error> {
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(FairSpillPool::new(memory_limit_bytes)))
            .with_disk_manager_builder(
                DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
            )
            .build_arc()
            .context(InitializationSnafu)
            .context(QuerySnafu)?;
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
        let data_frame = context
            .execute_logical_plan(plan)
            .await
            .context(PlanningSnafu)
            .context(QuerySnafu)?;
        data_frame
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
        let stream = query.execute_stream(tables, 16 * 1024 * 1024).await?;
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
    }
}
