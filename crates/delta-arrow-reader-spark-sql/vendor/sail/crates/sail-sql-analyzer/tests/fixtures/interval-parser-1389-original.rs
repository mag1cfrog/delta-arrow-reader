// Frozen interval factory from be3c663a8380d5094cb52003f351d5b9a5d48383.
use chumsky::Parser;
use chumsky::extra::ParserExtra;
use chumsky::input::{Input, ValueInput};
use chumsky::label::LabelError;
use chumsky::prelude::{Recursive, any, end};
use sail_sql_parser::ast::data_type::DataType;
use sail_sql_parser::ast::expression::{Expr, IntervalLiteral};
use sail_sql_parser::ast::query::{Query, TableRelation};
use sail_sql_parser::options::ParserOptions;
use sail_sql_parser::span::TokenSpan;
use sail_sql_parser::token::{Token, TokenLabel};
use sail_sql_parser::tree::TreeParser;
fn whitespace<'a, I, E>() -> impl Parser<'a, I, (), E> + Clone
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    E: ParserExtra<'a, I> + 'a,
{
    any()
        .filter(|token: &Token| token.is_whitespace())
        .ignored()
}

fn expression<'a, I, E>(options: &'a ParserOptions) -> impl Parser<'a, I, Expr, E> + Clone
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: Into<TokenSpan> + Clone,
    E: ParserExtra<'a, I> + 'a,
    E::Error: LabelError<'a, I, TokenLabel>,
{
    let mut expression = Recursive::declare();
    let mut query = Recursive::declare();
    let mut data_type = Recursive::declare();
    let mut table_relation = Recursive::declare();

    expression.define(Expr::parser(
        (expression.clone(), query.clone(), data_type.clone()),
        options,
    ));
    query.define(Query::parser(
        (query.clone(), expression.clone(), table_relation.clone()),
        options,
    ));
    data_type.define(DataType::parser(data_type.clone(), options));
    table_relation.define(TableRelation::parser(
        (query.clone(), expression.clone(), table_relation.clone()),
        options,
    ));

    expression
}

fn interval_literal<'a, I, E>(
    options: &'a ParserOptions,
) -> impl Parser<'a, I, IntervalLiteral, E> + Clone
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: Into<TokenSpan> + Clone,
    E: ParserExtra<'a, I> + 'a,
    E::Error: LabelError<'a, I, TokenLabel>,
{
    IntervalLiteral::parser(expression(options), options)
}

pub fn create_interval_literal_parser<'a, I, E>(
    options: &'a ParserOptions,
) -> impl Parser<'a, I, IntervalLiteral, E> + Clone
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: Into<TokenSpan> + Clone,
    E: ParserExtra<'a, I> + 'a,
    E::Error: LabelError<'a, I, TokenLabel>,
{
    interval_literal(options)
        .padded_by(whitespace().repeated())
        .then_ignore(end())
}
