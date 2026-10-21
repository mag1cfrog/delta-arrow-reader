//! Single-table SQL admission and planning over an immutable Core snapshot.

use std::{fmt, fs::File, io::Read, num::NonZeroUsize, ops::ControlFlow, sync::Arc};

use clap::Args;
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
use snafu::{ResultExt, Snafu};

use crate::{
    InputIoSnafu, QuerySnafu, TableArgs, diagnostics::ErrorClassification, parse_unsigned_decimal,
};

const MAX_SQL_BYTES: u64 = 64 * 1024;
const SQL_RECURSION_LIMIT: usize = 128;

/// Inputs and resource ceilings for a complete single-table query result.
#[derive(Args)]
pub(crate) struct QueryArgs {
    #[command(flatten)]
    pub(crate) table: TableArgs,
    /// UTF-8 file containing one read-only SQL query over table alias t; at most 64 KiB.
    #[arg(long, value_name = "PATH")]
    pub(crate) sql_file: String,
    /// Maximum result rows. Exceeding this ceiling fails; SQL LIMIT selects a smaller result.
    #[arg(long, default_value = "1000000", value_name = "N", value_parser = parse_unsigned_decimal::<usize>)]
    pub(crate) max_rows: usize,
    /// Maximum Arrow IPC bytes, including schema and framing. Exceeding this ceiling fails.
    #[arg(long, default_value = "1073741824", value_name = "BYTES", value_parser = parse_unsigned_decimal::<NonZeroUsize>)]
    pub(crate) max_output_bytes: NonZeroUsize,
    /// DataFusion memory-pool ceiling in bytes; not a process RSS limit. Disk spill is disabled.
    #[arg(long, default_value = "268435456", value_name = "BYTES", value_parser = parse_unsigned_decimal::<NonZeroUsize>)]
    pub(crate) memory_limit_bytes: NonZeroUsize,
}

/// SQL failures retain typed sources without printing query text or dependency messages.
#[derive(Snafu)]
pub(crate) enum QueryError {
    #[snafu(display("Invalid SQL input (reason={reason}). Use one read-only query over table t."))]
    Input { reason: &'static str },
    #[snafu(display("Unity Catalog SQL queries are not supported yet."))]
    UnsupportedUnityCatalogQuery,
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
            Self::UnsupportedUnityCatalogQuery => ErrorClassification {
                phase: "configuration",
                code: "unsupported_uc_query",
                reason: "unsupported_uc_query",
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
            Self::Input { .. } | Self::UnsupportedUnityCatalogQuery => 2,
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

    /// Registers only the supplied snapshot as t. URL-based table discovery is never enabled.
    pub(crate) async fn execute_stream(
        self,
        table: DeltaTable,
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
        register_table(&context, "t", table, ScanOptions::default()).context(crate::ReaderSnafu)?;
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
