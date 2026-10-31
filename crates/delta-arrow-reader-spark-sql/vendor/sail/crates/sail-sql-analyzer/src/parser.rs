// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use chumsky::Parser;
use chumsky::input::Input;
use chumsky::span::SimpleSpan;
use sail_sql_parser::ast::data_type::DataType;
use sail_sql_parser::ast::expression::IntervalLiteral;
use sail_sql_parser::ast::identifier::ObjectName;
use sail_sql_parser::ast::statement::Statement;
use sail_sql_parser::lexer::create_lexer;
use sail_sql_parser::options::ParserOptions;
use sail_sql_parser::parser::{
    create_data_type_parser, create_interval_literal_parser, create_object_name_parser,
    create_parser,
};
use sail_sql_parser::token::Token;

use crate::error::{SqlError, SqlResult};
use crate::literal::datetime::{
    DateValue, TimeValue, TimestampValue, create_date_parser, create_time_parser,
    create_timestamp_parser,
};
use crate::literal::interval::{IntervalValue, parse_unqualified_interval_string};

fn map_parser_input<'a, C>(
    (t, s): &'a (Token<'a>, SimpleSpan<usize, C>),
) -> (&'a Token<'a>, &'a SimpleSpan<usize, C>) {
    (t, s)
}

macro_rules! parse {
    ($input:ident, $parser:ident $(,)?) => {{
        let options = ParserOptions::default();
        let length = $input.len();
        let lexer = create_lexer::<_, chumsky::extra::Err<chumsky::error::Rich<_, _>>>(&options);
        let tokens = lexer
            .parse($input)
            .into_result()
            .map_err(SqlError::parser)?;
        let tokens = tokens
            .as_slice()
            .map((length..length).into(), map_parser_input);
        let parser = $parser::<_, chumsky::extra::Err<chumsky::error::Rich<_, _>>>(&options);
        parser.parse(tokens).into_result().map_err(SqlError::parser)
    }};
}

macro_rules! parse_simple {
    ($input:ident, $parser:ident $(,)?) => {{
        let parser = $parser::<chumsky::extra::Err<chumsky::error::Rich<_, _>>>();
        parser.parse($input).into_result().map_err(SqlError::parser)
    }};
}

pub fn parse_data_type(s: &str) -> SqlResult<DataType> {
    parse!(s, create_data_type_parser)
}

pub fn parse_statements(s: &str) -> SqlResult<Vec<Statement>> {
    parse!(s, create_parser)
}

/// Parses a SQL string containing exactly one statement into an AST.
pub fn parse_one_statement(s: &str) -> SqlResult<Statement> {
    let mut plan = parse_statements(s)?;
    match (plan.pop(), plan.is_empty()) {
        (Some(x), true) => Ok(x),
        _ => Err(SqlError::invalid("expected one statement")),
    }
}

pub fn parse_object_name(s: &str) -> SqlResult<ObjectName> {
    parse!(s, create_object_name_parser)
}

pub(crate) fn parse_interval_literal(s: &str) -> SqlResult<IntervalLiteral> {
    parse!(s, create_interval_literal_parser)
}

pub fn parse_interval(s: &str) -> SqlResult<IntervalValue> {
    parse_unqualified_interval_string(s, false)
}

pub fn parse_date(s: &str) -> SqlResult<DateValue> {
    parse_simple!(s, create_date_parser)
}

pub fn parse_timestamp(s: &str) -> SqlResult<TimestampValue<'_>> {
    parse_simple!(s, create_timestamp_parser)
}

pub fn parse_time(s: &str) -> SqlResult<TimeValue> {
    parse_simple!(s, create_time_parser)
}

#[cfg(test)]
mod tests {
    use sail_sql_parser::ast::query::Query;
    use sail_sql_parser::ast::statement::Statement;

    use crate::error::SqlResult;
    use crate::parser::{parse_one_statement, parse_statements};

    #[test]
    fn test_parse() -> SqlResult<()> {
        let sql = "/* */ ; SELECT 1;;; SELECT 2";
        let tree = parse_statements(sql)?;
        assert!(matches!(
            tree.as_slice(),
            [
                Statement::Query(Query { .. }),
                Statement::Query(Query { .. }),
            ]
        ));
        Ok(())
    }

    #[test]
    fn test_literal_and_variable_syntax() -> SqlResult<()> {
        for sql in [
            "/* */ SELECT 1+1",
            r"Select  2*3 +(4*5)AS a, b '\x01', $1,? -- comment",
            "SELECT foo(0), cast(1L as decimal(10, -1)) FROM a.b",
            r##"SELECT U&"a#2014b#+002014c"   UESCAPE '#'"##,
        ] {
            assert!(
                matches!(parse_one_statement(sql)?, Statement::Query(_)),
                "{sql}"
            );
        }
        Ok(())
    }

    #[test]
    fn test_signed_numeric_literals() -> SqlResult<()> {
        use crate::statement::from_ast_statement;
        use sail_common::spec::{Expr, Literal, QueryNode};

        for (text, expected) in [
            (
                "-128Y",
                Literal::Int8 {
                    value: Some(i8::MIN),
                },
            ),
            (
                "-32768S",
                Literal::Int16 {
                    value: Some(i16::MIN),
                },
            ),
            (
                "-2147483648",
                Literal::Int32 {
                    value: Some(i32::MIN),
                },
            ),
            (
                "-9223372036854775808L",
                Literal::Int64 {
                    value: Some(i64::MIN),
                },
            ),
            (
                "-9223372036854775808",
                Literal::Int64 {
                    value: Some(i64::MIN),
                },
            ),
            (
                "(-9223372036854775808L)",
                Literal::Int64 {
                    value: Some(i64::MIN),
                },
            ),
            (
                "- /* sign */ 9223372036854775808l",
                Literal::Int64 {
                    value: Some(i64::MIN),
                },
            ),
            (
                "2147483648",
                Literal::Int64 {
                    value: Some(2147483648),
                },
            ),
        ] {
            let plan = from_ast_statement(parse_one_statement(&format!("SELECT {text}"))?)?;
            assert!(
                matches!(plan.node, QueryNode::Project { expressions, .. }
                if expressions == vec![Expr::Literal(expected)]),
                "{text}"
            );
        }
        for text in [
            "128Y",
            "-129Y",
            "32768S",
            "-32769S",
            "9223372036854775808L",
            "-9223372036854775809L",
            "-(9223372036854775808L)",
        ] {
            assert!(
                from_ast_statement(parse_one_statement(&format!("SELECT {text}"))?).is_err(),
                "{text}"
            );
        }
        for text in [
            "-(2147483648)",
            "- +2147483648",
            "- -9223372036854775808L",
            "-x",
        ] {
            let plan = from_ast_statement(parse_one_statement(&format!("SELECT {text}"))?)?;
            assert!(
                matches!(plan.node, QueryNode::Project { expressions, .. }
                if matches!(expressions.as_slice(), [Expr::UnresolvedFunction(_)])),
                "{text}"
            );
        }
        for text in ["-0.0F", "-0.0D"] {
            let plan = from_ast_statement(parse_one_statement(&format!("SELECT {text}"))?)?;
            assert!(
                matches!(plan.node, QueryNode::Project { expressions, .. }
                if match expressions.as_slice() {
                    [Expr::Literal(Literal::Float32 { value: Some(v) })] => v.to_bits() == (-0.0f32).to_bits(),
                    [Expr::Literal(Literal::Float64 { value: Some(v) })] => v.to_bits() == (-0.0f64).to_bits(),
                    _ => false,
                }),
                "{text}"
            );
        }
        Ok(())
    }
}
