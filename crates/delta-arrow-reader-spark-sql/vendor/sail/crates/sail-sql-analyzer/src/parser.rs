// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::sync::LazyLock;

use chumsky::Parser;
use chumsky::error::{EmptyErr, Rich};
use chumsky::extra::Err;
use chumsky::input::{Input, Stream};
use chumsky::span::SimpleSpan;
use sail_sql_parser::ast::data_type::DataType;
use sail_sql_parser::ast::expression::IntervalLiteral;
use sail_sql_parser::ast::identifier::ObjectName;
use sail_sql_parser::ast::statement::Statement;
use sail_sql_parser::lexer::create_lexer;
use sail_sql_parser::options::ParserOptions;
use sail_sql_parser::parser::{
    create_data_type_parser, create_interval_literal_parser,
    create_numeric_interval_literal_parser, create_object_name_parser, create_parser,
    create_scoped_statement_parser,
};
use sail_sql_parser::token::{Punctuation, Token};

use crate::error::{SqlError, SqlResult};
use crate::literal::datetime::{
    DateValue, TimeValue, TimestampValue, create_date_parser, create_time_parser,
    create_timestamp_parser,
};
use crate::literal::interval::{
    IntervalValue, from_ast_unqualified_interval, parse_unqualified_interval_string,
};

#[cfg(test)]
thread_local! {
    static INTERVAL_LITERAL_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static STATEMENT_RICH_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

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
    if let Some(statements) = parse_statements_without_diagnostics(s) {
        return Ok(statements);
    }
    #[cfg(test)]
    STATEMENT_RICH_CALLS.with(|calls| calls.set(calls.get() + 1));
    parse!(s, create_parser)
}

fn parse_statements_without_diagnostics(s: &str) -> Option<Vec<Statement>> {
    let options = ParserOptions::default();
    let lexer = create_lexer::<_, chumsky::extra::Err<chumsky::error::EmptyErr>>(&options);
    let tokens = lexer.parse(s).into_result().ok()?;
    let tokens = tokens
        .as_slice()
        .map((s.len()..s.len()).into(), map_parser_input);
    let parser = create_scoped_statement_parser::<_, chumsky::extra::Err<chumsky::error::EmptyErr>>(
        &options,
    );
    // Only complete, error-free parses bypass the original Rich diagnostic path.
    parser.parse(tokens).into_result().ok()
}

/// Parses a SQL string containing exactly one statement into an AST.
pub fn parse_one_statement(s: &str) -> SqlResult<Statement> {
    let mut plan = parse_statements(s).map_err(|error| {
        unsupported_statement_kind(s).map_or(error, |kind| SqlError::unsupported(kind))
    })?;
    match (plan.pop(), plan.is_empty()) {
        (Some(x), true) => Ok(x),
        _ => Err(SqlError::invalid("expected one statement")),
    }
}

/// Give unparsed commands and external scripts an admission-specific diagnostic.
fn unsupported_statement_kind(sql: &str) -> Option<String> {
    let options = ParserOptions::default();
    let lexer = create_lexer::<_, chumsky::extra::Err<chumsky::error::Rich<_, _>>>(&options);
    let tokens = lexer.parse(sql).into_result().ok()?;
    let code = tokens
        .iter()
        .map(|(token, _)| token)
        .filter(|token| {
            !token.is_whitespace() && !matches!(token, Token::Punctuation(Punctuation::Semicolon))
        })
        .collect::<Vec<_>>();
    if let Some(Token::Word { raw, .. }) = code.first() {
        let kind = raw.to_ascii_uppercase();
        if matches!(
            kind.as_str(),
            "INSERT"
                | "UPDATE"
                | "DELETE"
                | "MERGE"
                | "TRUNCATE"
                | "COPY"
                | "CREATE"
                | "ALTER"
                | "DROP"
                | "OPTIMIZE"
                | "VACUUM"
                | "ANALYZE"
                | "REFRESH"
                | "CACHE"
                | "UNCACHE"
                | "CLEAR"
                | "SET"
                | "RESET"
                | "USE"
                | "EXPLAIN"
                | "DESC"
                | "DESCRIBE"
                | "SHOW"
        ) {
            let suffix = if matches!(kind.as_str(), "SHOW" | "DESC" | "DESCRIBE" | "EXPLAIN") {
                "form"
            } else {
                "statements"
            };
            return Some(format!("{kind} {suffix}"));
        }
        if kind == "WITH" {
            let mut depth = 0_usize;
            let mut after_group = false;
            for token in code.iter().skip(1) {
                match token {
                    Token::Punctuation(Punctuation::LeftParenthesis) => depth += 1,
                    Token::Punctuation(Punctuation::RightParenthesis) => {
                        depth = depth.saturating_sub(1);
                        after_group = depth == 0;
                    }
                    Token::Punctuation(Punctuation::Comma) if depth == 0 => after_group = false,
                    Token::Word { raw, .. } if depth == 0 && after_group => {
                        let command = raw.to_ascii_uppercase();
                        if matches!(command.as_str(), "SELECT" | "TABLE" | "VALUES") {
                            break;
                        }
                        if matches!(command.as_str(), "INSERT" | "UPDATE" | "DELETE" | "MERGE") {
                            return Some(format!("{command} statements"));
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    if code.windows(2).any(|tokens| matches!(tokens,
        [Token::Word { raw: select, .. }, Token::Word { raw: transform, .. }]
        if select.eq_ignore_ascii_case("SELECT") && transform.eq_ignore_ascii_case("TRANSFORM")))
        && code.windows(3).any(|tokens| matches!(tokens,
            [Token::Punctuation(Punctuation::RightParenthesis), Token::Word { raw, .. }, Token::String { .. }]
            if raw.eq_ignore_ascii_case("USING")))
    {
        return Some("external script queries".to_string());
    }
    None
}

pub fn parse_object_name(s: &str) -> SqlResult<ObjectName> {
    parse!(s, create_object_name_parser)
}

pub(crate) fn parse_interval_literal(s: &str) -> SqlResult<IntervalLiteral> {
    #[cfg(test)]
    INTERVAL_LITERAL_CALLS.with(|calls| calls.set(calls.get() + 1));
    let options = ParserOptions::default();
    let lexer = create_lexer::<_, Err<Rich<_, _>>>(&options);
    let tokens = lexer.parse(s).into_result().map_err(SqlError::parser)?;
    let input = || {
        tokens
            .as_slice()
            .map((s.len()..s.len()).into(), map_parser_input)
    };
    if let Ok(interval) = create_numeric_interval_literal_parser::<_, Err<EmptyErr>>(&options)
        .parse(input())
        .into_result()
    {
        return Ok(interval);
    }
    create_interval_literal_parser::<_, Err<Rich<_, _>>>(&options)
        .parse(input())
        .into_result()
        .map_err(SqlError::parser)
}

/// Parses an interval string converted at run time, such as a string cast or a `window`
/// duration. A multi-unit string chooses its family by non-zero values, unlike a SQL literal.
pub fn parse_interval(s: &str) -> SqlResult<IntervalValue> {
    parse_unqualified_interval_string(s, false)
}

pub fn parse_date(s: &str) -> SqlResult<DateValue> {
    let s = s.trim_matches(|c: char| c.is_ascii_control() || c == ' ');
    parse_simple!(s, create_date_parser)
}

pub fn parse_timestamp(s: &str) -> SqlResult<TimestampValue<'_>> {
    let s = s.trim_matches(|c: char| c.is_ascii_control() || c == ' ');
    parse_simple!(s, create_timestamp_parser)
}

/// Reuse the existing TIMESTAMP parser within an input batch; timezone text borrows that input.
pub fn timestamp_parser<'a>() -> impl FnMut(&'a str) -> SqlResult<TimestampValue<'a>> {
    let mut parser = None;
    move |s| {
        let s = s.trim_matches(|c: char| c.is_ascii_control() || c == ' ');
        parser
            .get_or_insert_with(
                create_timestamp_parser::<chumsky::extra::Err<chumsky::error::Rich<_, _>>>,
            )
            .parse(s)
            .into_result()
            .map_err(SqlError::parser)
    }
}

/// Reuse an interval lexer and parser for rows borrowing the same batch.
pub fn interval_parser<'a>() -> impl FnMut(&'a str) -> SqlResult<IntervalValue> {
    static OPTIONS: LazyLock<ParserOptions> = LazyLock::new(ParserOptions::default);
    let options = &*OPTIONS;
    let mut lexer = None;
    let mut numeric_parser = None;
    let mut parser = None;
    move |s| {
        let tokens = lexer
            .get_or_insert_with(|| {
                create_lexer::<_, chumsky::extra::Err<chumsky::error::Rich<_, _>>>(options)
            })
            .parse(s)
            .into_result()
            .map_err(SqlError::parser)?;
        // Owning the token vector avoids tying these parsers to a row-local slice.
        let input = |tokens| {
            Stream::from_iter(tokens).map((s.len()..s.len()).into(), |(token, span)| (token, span))
        };
        if let Ok(interval) = numeric_parser
            .get_or_insert_with(|| {
                create_numeric_interval_literal_parser::<_, Err<EmptyErr>>(options)
            })
            .parse(input(tokens.clone()))
            .into_result()
        {
            return from_ast_unqualified_interval(interval);
        }
        let interval =
            parser
                .get_or_insert_with(|| {
                    create_interval_literal_parser::<
                        _,
                        chumsky::extra::Err<chumsky::error::Rich<_, _>>,
                    >(options)
                })
                .parse(input(tokens))
                .into_result()
                .map_err(SqlError::parser)?;
        from_ast_unqualified_interval(interval)
    }
}

pub fn parse_time(s: &str) -> SqlResult<TimeValue> {
    parse_simple!(s, create_time_parser)
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::fmt::Debug;

    use chumsky::Parser;
    use chumsky::input::Input;
    use sail_sql_parser::ast::query::Query;
    use sail_sql_parser::ast::statement::Statement;
    use sail_sql_parser::options::ParserOptions;

    use crate::error::{SqlError, SqlResult};
    use crate::parser::{
        STATEMENT_RICH_CALLS, create_lexer, create_parser, map_parser_input, parse_one_statement,
        parse_statements, parse_time, parse_timestamp, unsupported_statement_kind,
    };
    use crate::statement::from_ast_read_only_statement;

    mod inputs {
        include!("../tests/fixtures/statement-parser-1390-inputs.rs");
    }

    // The pre-change public path, including the original lexer, spans and Rich error conversion.
    fn original_statements(sql: &str) -> SqlResult<Vec<Statement>> {
        parse!(sql, create_parser)
    }

    fn original_one_statement(
        sql: &str,
        statements: SqlResult<Vec<Statement>>,
    ) -> SqlResult<Statement> {
        let mut plan = statements.map_err(|error| {
            unsupported_statement_kind(sql).map_or(error, |kind| SqlError::unsupported(kind))
        })?;
        match (plan.pop(), plan.is_empty()) {
            (Some(statement), true) => Ok(statement),
            _ => Err(SqlError::invalid("expected one statement")),
        }
    }

    fn error_chain(error: &dyn Error) -> Vec<String> {
        let mut chain = vec![error.to_string()];
        let mut source = error.source();
        while let Some(error) = source {
            chain.push(error.to_string());
            source = error.source();
        }
        chain
    }

    fn assert_result<T: Debug>(actual: &SqlResult<T>, expected: &SqlResult<T>, label: &str) {
        // AST Debug compares every field, including source spans and identifier flags.
        assert_eq!(format!("{actual:?}"), format!("{expected:?}"), "{label}");
        if let (Err(actual), Err(expected)) = (actual, expected) {
            assert_eq!(
                std::mem::discriminant(actual),
                std::mem::discriminant(expected),
                "{label}"
            );
            assert_eq!(actual.to_string(), expected.to_string(), "{label}");
            assert_eq!(error_chain(actual), error_chain(expected), "{label}");
        }
    }

    fn assert_equivalent(label: &str, sql: &str, without_diagnostics: bool) {
        let expected = original_statements(sql);
        assert_eq!(
            expected.is_ok(),
            without_diagnostics,
            "{label}: reference route"
        );
        STATEMENT_RICH_CALLS.with(|calls| calls.set(0));
        let actual = parse_statements(sql);
        STATEMENT_RICH_CALLS.with(|calls| {
            assert_eq!(
                calls.replace(0),
                usize::from(!without_diagnostics),
                "{label}: statements route"
            )
        });
        assert_result(&actual, &expected, label);

        let actual = parse_one_statement(sql);
        STATEMENT_RICH_CALLS.with(|calls| {
            assert_eq!(
                calls.replace(0),
                usize::from(!without_diagnostics),
                "{label}: one-statement route"
            )
        });
        let expected = original_one_statement(sql, expected);
        assert_result(&actual, &expected, label);
        // Equal ASTs must also retain read-only admission and its exact errors.
        let actual = actual
            .and_then(from_ast_read_only_statement)
            .map(|statement| std::mem::discriminant(&statement));
        let expected = expected
            .and_then(from_ast_read_only_statement)
            .map(|statement| std::mem::discriminant(&statement));
        assert_result(&actual, &expected, label);
    }

    #[test]
    fn statement_scoped_and_rich_paths_preserve_boundaries() {
        for &sql in inputs::VALID.iter().chain(inputs::METADATA) {
            assert_equivalent(sql, sql, true);
        }
        let options = ParserOptions::default();
        let lexer = create_lexer::<_, chumsky::extra::Err<chumsky::error::Rich<_, _>>>(&options);
        for &sql in inputs::LEX_INVALID {
            assert!(
                lexer.parse(sql).into_result().is_err(),
                "{sql:?}: lexer route"
            );
            assert_equivalent(sql, sql, false);
        }
        for &sql in inputs::PARSER_INVALID.iter().chain(inputs::UNSUPPORTED) {
            assert!(
                lexer.parse(sql).into_result().is_ok(),
                "{sql:?}: parser route"
            );
            assert_equivalent(sql, sql, false);
        }
    }

    #[test]
    fn numeric_interval_grammar_preserves_original_asts_and_errors() {
        use super::{
            EmptyErr, Err, create_numeric_interval_literal_parser, parse_interval_literal,
        };
        use sail_sql_parser::parser::create_interval_literal_parser;

        #[allow(dead_code)]
        mod interval_inputs {
            include!("../tests/fixtures/string-parser-1389-inputs.rs");
        }

        fn original(s: &str) -> SqlResult<sail_sql_parser::ast::expression::IntervalLiteral> {
            parse!(s, create_interval_literal_parser)
        }

        let options = ParserOptions::default();
        let lexer = create_lexer::<_, Err<EmptyErr>>(&options);
        let numeric = create_numeric_interval_literal_parser::<_, Err<EmptyErr>>(&options);
        let ordinary = [
            "1 months",
            "2 days 2 hours",
            "3 months 1 day 2 seconds",
            "'1 months'",
        ];
        let additional_inputs = [
            "1E+1 seconds",
            "1.5d seconds",
            "2 /* comment */ DAYS 1 HOUR",
            "1 day TO HOUR",
            "1 AND 2 days",
            "1 day AND 2 hours",
            "1 months 'extra'",
            "\"'3 days'\"",
        ];
        for (text, require_numeric) in ordinary.into_iter().map(|text| (text, true)).chain(
            additional_inputs
                .into_iter()
                .chain(interval_inputs::INTERVALS.iter().copied())
                .map(|text| (text, false)),
        ) {
            let parsed = lexer.parse(text).into_result().ok().and_then(|tokens| {
                let input = tokens
                    .as_slice()
                    .map((text.len()..text.len()).into(), map_parser_input);
                numeric.parse(input).into_result().ok()
            });
            if require_numeric {
                assert!(parsed.is_some(), "{text:?} missed the numeric grammar");
            }
            let expected = original(text);
            if let Some(actual) = parsed {
                assert_result(&Ok(actual), &expected, text);
            }
            assert_result(&parse_interval_literal(text), &expected, text);
        }
    }

    #[test]
    fn decoded_interval_literals_retain_only_their_scoped_parser_calls() -> SqlResult<()> {
        super::INTERVAL_LITERAL_CALLS.with(|calls| calls.set(0));
        let mut parse = super::interval_parser();
        for (text, scoped_calls) in [("2 months", 0), ("'2 months'", 1), ("'''2 months'''", 3)] {
            assert!(matches!(
                parse(text)?,
                super::IntervalValue::YearMonth { months: 2, .. }
            ));
            super::INTERVAL_LITERAL_CALLS.with(|calls| assert_eq!(calls.get(), scoped_calls));
        }
        Ok(())
    }

    #[test]
    fn test_timestamp_date_time_separators() -> SqlResult<()> {
        // Spark accepts one space or uppercase T, and only after a complete date.
        for (text, (month, day), hour, timezone) in [
            ("2026-01-02T01:02:03Z", (1, 2), 1, "UTC"),
            (
                "2026-01-02 01:02:03 America/New_York",
                (1, 2),
                1,
                "America/New_York",
            ),
            ("2026-01-02T01", (1, 2), 1, ""),
            ("2026-01-02 ", (1, 2), 0, ""),
            ("2026-01", (1, 1), 0, ""),
        ] {
            let value = parse_timestamp(text)?;
            assert_eq!(
                (
                    value.date.month,
                    value.date.day,
                    value.time.hour,
                    value.timezone
                ),
                (month, day, hour, timezone),
                "{text}"
            );
        }
        for text in [
            "2026-01-02t01:02:03",
            "2026-01-02  01:02:03",
            "2026-01-02 T01:02:03",
            "2026-01-02T 01:02:03",
            "2026-01-02\t01:02:03",
            "2026-01 01:02:03",
            "2026T01:02:03",
            "2026-01-02T",
        ] {
            assert!(parse_timestamp(text).is_err(), "{text:?}");
        }
        Ok(())
    }

    #[test]
    fn timestamp_trim_and_fraction_do_not_change_time_precision() -> SqlResult<()> {
        let value = parse_timestamp("\t1969-12-31 23:59:59.123456789123Z\r\n")?;
        assert_eq!(value.time.nanoseconds, 123_456_789);
        assert_eq!(value.timezone, "UTC");
        assert_eq!(parse_time("23:59:59.123456789")?.nanoseconds, 123_456_789);
        assert!(parse_time("23:59:59.123456789123").is_err());
        Ok(())
    }

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
