// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use sail_common::spec;
use sail_sql_parser::ast::statement::{DatabasesKeyword, DescribeItem, ExplainFormat, Statement};

use crate::error::{SqlError, SqlResult};
use crate::expression::from_ast_object_name;
use crate::query::{from_ast_query, from_ast_query_table};
use crate::value::from_ast_string;

/// Converts a query AST into a spec plan, rejecting commands before analyzing their bodies.
pub fn from_ast_statement(statement: Statement) -> SqlResult<spec::QueryPlan> {
    match statement {
        Statement::Query(query) => from_ast_query(query),
        _ => Err(SqlError::unsupported(
            "only read-only queries are supported",
        )),
    }
}

/// Admitted metadata requests retain query plans without importing command execution services.
#[derive(Debug)]
pub enum ReadOnlyStatement {
    Query(spec::QueryPlan),
    Describe(spec::QueryPlan),
    ShowColumns(spec::QueryPlan),
    ShowTables {
        schema: Option<Vec<String>>,
        pattern: Option<String>,
    },
    ShowSchemas {
        pattern: Option<String>,
    },
    Explain(spec::QueryPlan),
}

/// Dispatch the read-only surface; query-only resolver callers retain `from_ast_statement`.
pub fn from_ast_read_only_statement(statement: Statement) -> SqlResult<ReadOnlyStatement> {
    match statement {
        query @ Statement::Query(_) => Ok(ReadOnlyStatement::Query(from_ast_statement(query)?)),
        Statement::Describe { item, .. } => match item {
            DescribeItem::Table {
                extended: None,
                name,
                partition: None,
                column: None,
                ..
            }
            | DescribeItem::BareTable { name } => {
                Ok(ReadOnlyStatement::Describe(from_ast_query_table(name)?))
            }
            DescribeItem::Query { item, .. } => {
                Ok(ReadOnlyStatement::Describe(from_ast_query(item)?))
            }
            _ => Err(SqlError::unsupported("this DESCRIBE form")),
        },
        Statement::ShowColumns {
            table: (_, name),
            database: None,
            ..
        } => Ok(ReadOnlyStatement::ShowColumns(from_ast_query_table(name)?)),
        Statement::ShowTables { from, like, .. } => {
            let schema = from
                .map(|(_, name)| {
                    Ok(from_ast_object_name(name)?
                        .parts()
                        .iter()
                        .map(|part| part.as_ref().to_string())
                        .collect())
                })
                .transpose()?;
            let pattern = like
                .map(|(_, pattern)| from_ast_string(pattern))
                .transpose()?;
            Ok(ReadOnlyStatement::ShowTables { schema, pattern })
        }
        Statement::ShowDatabases {
            databases: DatabasesKeyword::Databases(_) | DatabasesKeyword::Schemas(_),
            from: None,
            like,
            ..
        } => {
            let pattern = like
                .map(|(_, pattern)| from_ast_string(pattern))
                .transpose()?;
            Ok(ReadOnlyStatement::ShowSchemas { pattern })
        }
        Statement::Explain {
            format:
                None
                | Some(
                    ExplainFormat::Extended(_)
                    | ExplainFormat::Formatted(_)
                    | ExplainFormat::Cost(_)
                    | ExplainFormat::Codegen(_),
                ),
            statement,
            ..
        } => {
            let Statement::Query(query) = *statement else {
                return Err(SqlError::unsupported("EXPLAIN requires a read-only query"));
            };
            Ok(ReadOnlyStatement::Explain(from_ast_query(query)?))
        }
        _ => Err(SqlError::unsupported(
            "this statement; only read-only queries and supported metadata statements are supported",
        )),
    }
}
