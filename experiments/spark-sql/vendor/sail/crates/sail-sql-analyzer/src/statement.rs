// Modified from Sail v0.7.1 for the Delta reader experiment. See experiments/spark-sql/UPSTREAM.md in the host repository.
use sail_common::spec;
use sail_sql_parser::ast::statement::Statement;

use crate::error::{SqlError, SqlResult};
use crate::query::from_ast_query;

/// Converts a query AST into a spec plan, rejecting commands before analyzing their bodies.
pub fn from_ast_statement(statement: Statement) -> SqlResult<spec::Plan> {
    match statement {
        Statement::Query(query) => Ok(spec::Plan::Query(from_ast_query(query)?)),
        _ => Err(SqlError::unsupported(
            "extraction probe accepts queries only",
        )),
    }
}
