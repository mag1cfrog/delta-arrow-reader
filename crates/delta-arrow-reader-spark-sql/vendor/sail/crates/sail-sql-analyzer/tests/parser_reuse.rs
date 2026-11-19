use chumsky::Parser;
use chumsky::input::{Input, Stream};
use chumsky::span::SimpleSpan;
use sail_sql_analyzer::error::{SqlError, SqlResult};
use sail_sql_analyzer::parser::{
    date_parser, interval_parser, parse_date, parse_interval, parse_timestamp, timestamp_parser,
};
use sail_sql_parser::ast::expression::IntervalLiteral;
use sail_sql_parser::lexer::create_lexer;
use sail_sql_parser::options::ParserOptions;
use sail_sql_parser::parser::create_interval_literal_parser;
use sail_sql_parser::token::Token;

#[path = "fixtures/string-parser-1389-inputs.rs"]
mod inputs;
#[path = "fixtures/interval-parser-1389-original.rs"]
mod original;

fn original_interval(s: &str) -> SqlResult<IntervalLiteral> {
    let options = ParserOptions::default();
    let lexer = create_lexer::<_, chumsky::extra::Err<chumsky::error::Rich<_, _>>>(&options);
    let tokens = lexer.parse(s).into_result().map_err(SqlError::parser)?;
    fn map_input<'a>(
        (token, span): &'a (Token<'a>, SimpleSpan),
    ) -> (&'a Token<'a>, &'a SimpleSpan) {
        (token, span)
    }
    let tokens = tokens.as_slice().map((s.len()..s.len()).into(), map_input);
    original::create_interval_literal_parser::<_, chumsky::extra::Err<chumsky::error::Rich<_, _>>>(
        &options,
    )
    .parse(tokens)
    .into_result()
    .map_err(SqlError::parser)
}

#[test]
fn reused_parsers_preserve_values_and_exact_errors() {
    let mut date = date_parser();
    let mut timestamp = timestamp_parser();
    let mut interval = interval_parser();
    for _ in 0..2 {
        for value in inputs::DATES {
            assert_eq!(
                format!("{:?}", date(value)),
                format!("{:?}", parse_date(value)),
                "{value:?}"
            );
        }
        for value in inputs::TIMESTAMPS {
            assert_eq!(
                format!("{:?}", timestamp(value)),
                format!("{:?}", parse_timestamp(value)),
                "{value:?}"
            );
        }
        for &value in inputs::INTERVALS {
            assert_eq!(
                format!("{:?}", interval(value)),
                format!("{:?}", parse_interval(value)),
                "{value:?}"
            );
        }
    }
}

#[test]
fn interval_graph_and_owned_input_preserve_recursive_grammar_and_rich_errors() -> SqlResult<()> {
    let options = ParserOptions::default();
    let lexer = create_lexer::<_, chumsky::extra::Err<chumsky::error::Rich<_, _>>>(&options);
    let parser = create_interval_literal_parser::<_, chumsky::extra::Err<chumsky::error::Rich<_, _>>>(
        &options,
    );
    for &value in inputs::INTERVALS {
        let actual = lexer
            .parse(value)
            .into_result()
            .map_err(SqlError::parser)
            .and_then(|tokens| {
                let tokens = Stream::from_iter(tokens)
                    .map((value.len()..value.len()).into(), |(token, span)| {
                        (token, span)
                    });
                parser.parse(tokens).into_result().map_err(SqlError::parser)
            });
        assert_eq!(
            format!("{actual:?}"),
            format!("{:?}", original_interval(value)),
            "{value:?}"
        );
    }
    Ok(())
}
