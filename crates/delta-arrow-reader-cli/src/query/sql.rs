//! Read-only SQL admission and planning over named, immutable Core snapshots.

use std::{fs::File, io::Read, ops::ControlFlow, sync::Arc};

use datafusion::{
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
};
use snafu::ResultExt;

use super::{
    QueryError,
    error::{ExecutionSnafu, PlanningSnafu},
};
use crate::diagnostics::{Error, InputIoSnafu, QuerySnafu};

pub(crate) const MAX_SQL_BYTES: u64 = 64 * 1024;
pub(crate) const SQL_RECURSION_LIMIT: usize = 128;

/// Parsed and admitted SQL. The AST is kept private because it contains user literals.
pub(crate) struct ReadOnlySqlQuery {
    statement: Statement,
}

impl ReadOnlySqlQuery {
    /// Rejects malformed or disallowed SQL before opening the requested Delta table.
    pub(crate) fn read_file_with_bytes(path: &str) -> Result<(Self, Vec<u8>), Error> {
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
    ) -> Result<DataFrame, Error> {
        let context = SessionContext::new_with_config_rt(SessionConfig::new(), runtime);
        for (alias, table) in tables {
            register_table(&context, alias, table, ScanOptions::default())
                .context(crate::diagnostics::ReaderSnafu)?;
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
    ) -> Result<SendableRecordBatchStream, Error> {
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
mod tests;
