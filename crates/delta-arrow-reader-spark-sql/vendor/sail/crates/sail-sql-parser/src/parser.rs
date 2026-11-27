// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use chumsky::error::EmptyErr;
use chumsky::extra::{Err, ParserExtra};
use chumsky::input::{Input, InputRef, ValueInput};
use chumsky::label::LabelError;
use chumsky::prelude::{Recursive, custom, end, recursive};
use chumsky::{IterParser, Parser};

use crate::ast::data_type::DataType;
use crate::ast::expression::{AtomExpr, Expr, IntervalLiteral};
use crate::ast::identifier::ObjectName;
use crate::ast::literal::NumberLiteral;
use crate::ast::operator::Semicolon;
use crate::ast::query::{Query, TableRelation};
use crate::ast::statement::Statement;
use crate::options::ParserOptions;
use crate::span::TokenSpan;
use crate::token::{Keyword, Punctuation, Token, TokenLabel};
use crate::tree::TreeParser;
use crate::utils::{skip_whitespace, whitespace};

fn statement<'a, I, E>(options: &'a ParserOptions) -> impl Parser<'a, I, Statement, E> + Clone
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: Into<TokenSpan> + Clone,
    E: ParserExtra<'a, I> + 'a,
    E::Error: LabelError<'a, I, TokenLabel>,
{
    let mut statement = Recursive::declare();
    let mut query = Recursive::declare();
    let mut expression = Recursive::declare();
    let mut data_type = Recursive::declare();
    let mut table_relation = Recursive::declare();

    statement.define(Statement::parser(
        (
            statement.clone(),
            query.clone(),
            expression.clone(),
            data_type.clone(),
        ),
        options,
    ));
    query.define(Query::parser(
        (query.clone(), expression.clone(), table_relation.clone()),
        options,
    ));
    expression.define(Expr::parser(
        (expression.clone(), query.clone(), data_type.clone()),
        options,
    ));
    data_type.define(DataType::parser(data_type.clone(), options));
    table_relation.define(TableRelation::parser(
        (query.clone(), expression.clone(), table_relation.clone()),
        options,
    ));

    statement
}

fn statement_with_weak_backedges<'a, I, E, P>(
    expression: P,
    options: &'a ParserOptions,
) -> impl Parser<'a, I, Statement, E> + Clone
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: Into<TokenSpan> + Clone,
    E: ParserExtra<'a, I> + 'a,
    E::Error: LabelError<'a, I, TokenLabel>,
    P: Parser<'a, I, Expr, E> + Clone + 'a,
{
    recursive(|statement| {
        let data_type = recursive(|data_type| DataType::parser(data_type, options));
        // A separate top-level query keeps every recursive backedge weak.
        let query = query_with_weak_backedges(expression.clone(), options);
        Statement::parser((statement, query, expression, data_type), options)
    })
}

fn query_with_weak_backedges<'a, I, E, P>(
    expression: P,
    options: &'a ParserOptions,
) -> impl Parser<'a, I, Query, E> + Clone
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: Into<TokenSpan> + Clone,
    E: ParserExtra<'a, I> + 'a,
    E::Error: LabelError<'a, I, TokenLabel>,
    P: Parser<'a, I, Expr, E> + Clone + 'a,
{
    recursive(|query| {
        let table_relation = recursive(|table_relation| {
            TableRelation::parser((query.clone(), expression.clone(), table_relation), options)
        });
        Query::parser((query, expression, table_relation), options)
    })
}

fn data_type<'a, I, E>(options: &'a ParserOptions) -> impl Parser<'a, I, DataType, E> + Clone
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: Into<TokenSpan> + Clone,
    E: ParserExtra<'a, I> + 'a,
    E::Error: LabelError<'a, I, TokenLabel>,
{
    let mut data_type = Recursive::declare();
    data_type.define(DataType::parser(data_type.clone(), options));
    data_type
}

fn object_name<'a, I, E>(options: &'a ParserOptions) -> impl Parser<'a, I, ObjectName, E> + Clone
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: Into<TokenSpan> + Clone,
    E: ParserExtra<'a, I> + 'a,
    E::Error: LabelError<'a, I, TokenLabel>,
{
    ObjectName::parser((), options)
}

fn expression<'a, I, E>(options: &'a ParserOptions) -> impl Parser<'a, I, Expr, E> + Clone
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: Into<TokenSpan> + Clone,
    E: ParserExtra<'a, I> + 'a,
    E::Error: LabelError<'a, I, TokenLabel>,
{
    recursive(|expression| expression_with_weak_backedges(expression, options))
}

fn expression_with_weak_backedges<'a, I, E, P>(
    expression: P,
    options: &'a ParserOptions,
) -> impl Parser<'a, I, Expr, E> + Clone
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: Into<TokenSpan> + Clone,
    E: ParserExtra<'a, I> + 'a,
    E::Error: LabelError<'a, I, TokenLabel>,
    P: Parser<'a, I, Expr, E> + Clone + 'a,
{
    let data_type = recursive(|data_type| DataType::parser(data_type, options));
    let query = recursive(|query| {
        let table_relation = recursive(|table_relation| {
            TableRelation::parser((query.clone(), expression.clone(), table_relation), options)
        });
        Query::parser((query, expression.clone(), table_relation), options)
    });
    Expr::parser((expression, query, data_type), options)
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

fn numeric_interval_literal<'a, I, E>(
    options: &'a ParserOptions,
) -> impl Parser<'a, I, IntervalLiteral, E> + Clone
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: Into<TokenSpan> + Clone,
    E: ParserExtra<'a, I> + 'a,
    E::Error: LabelError<'a, I, TokenLabel>,
{
    let number = NumberLiteral::parser((), options)
        .map(|literal| Expr::Atom(AtomExpr::NumberLiteral(literal)));
    // Keep the same interval grammar and use a smaller parser for numeric operands.
    IntervalLiteral::parser(number, options)
}

pub fn create_parser<'a, I, E>(
    options: &'a ParserOptions,
) -> impl Parser<'a, I, Vec<Statement>, E> + Clone
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: Into<TokenSpan> + Clone,
    E: ParserExtra<'a, I> + 'a,
    E::Error: LabelError<'a, I, TokenLabel>,
{
    statement_list(statement(options), options)
}

pub fn create_scoped_statement_parser<'a, I, E>(
    options: &'a ParserOptions,
) -> impl Parser<'a, I, Vec<Statement>, E> + Clone
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: Into<TokenSpan> + Clone,
    E: ParserExtra<'a, I> + 'a,
    E::Error: LabelError<'a, I, TokenLabel>,
{
    statement_list(
        statement_with_weak_backedges(expression(options), options),
        options,
    )
}

/// The diagnostic-free statement path can skip precedence parsing for a
/// standalone integer when the next token proves the expression has ended.
pub fn create_scoped_statement_parser_without_diagnostics<'a, I>(
    options: &'a ParserOptions,
) -> impl Parser<'a, I, Vec<Statement>, Err<EmptyErr>> + Clone
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: Into<TokenSpan> + Clone,
{
    let expression = statement_expression_without_diagnostics(options);
    statement_list(statement_with_weak_backedges(expression, options), options)
}

/// Parse a statement list containing only queries with the scoped expression grammar.
pub fn create_scoped_query_statement_parser_without_diagnostics<'a, I>(
    options: &'a ParserOptions,
) -> impl Parser<'a, I, Vec<Statement>, Err<EmptyErr>> + Clone
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: Into<TokenSpan> + Clone,
{
    let expression = statement_expression_without_diagnostics(options);
    let query = query_with_weak_backedges(expression, options);
    statement_list(query.map(Statement::Query), options)
}

fn statement_expression_without_diagnostics<'a, I>(
    options: &'a ParserOptions,
) -> impl Parser<'a, I, Expr, Err<EmptyErr>> + Clone
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: Into<TokenSpan> + Clone,
{
    recursive(|expression| {
        terminated_integer().or(expression_with_weak_backedges(expression, options))
    })
}

fn terminated_integer<'a, I>() -> impl Parser<'a, I, Expr, Err<EmptyErr>> + Clone
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: Into<TokenSpan>,
{
    custom(|input: &mut InputRef<'a, '_, I, Err<EmptyErr>>| {
        let start = input.cursor();
        let Some(Token::Word { raw, keyword: None }) = input.next() else {
            return Err(EmptyErr::default());
        };
        if raw.is_empty() || !raw.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(EmptyErr::default());
        }
        let span = input.span_since(&start).into();
        skip_whitespace(input);
        match input.peek() {
            Some(Token::Punctuation(Punctuation::Comma | Punctuation::RightParenthesis))
            | Some(Token::Word {
                keyword: Some(Keyword::When | Keyword::Else | Keyword::End),
                ..
            }) => Ok(Expr::Atom(AtomExpr::NumberLiteral(NumberLiteral {
                span,
                value: raw.to_owned(),
                suffix: None,
            }))),
            _ => Err(EmptyErr::default()),
        }
    })
}

fn statement_list<'a, I, E, P>(
    statement: P,
    options: &'a ParserOptions,
) -> impl Parser<'a, I, Vec<Statement>, E> + Clone
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: Into<TokenSpan> + Clone,
    E: ParserExtra<'a, I> + 'a,
    E::Error: LabelError<'a, I, TokenLabel>,
    P: Parser<'a, I, Statement, E> + Clone,
{
    let semicolon = Semicolon::parser((), options);
    // We only need to consume whitespaces explicitly at the beginning.
    // All other whitespaces are consumed implicitly by the statement or semicolon parsers.
    whitespace()
        .repeated()
        .ignore_then(semicolon.clone().repeated())
        .ignore_then(
            statement
                .then_ignore(semicolon.clone().ignored().or(end()))
                .then_ignore(semicolon.repeated())
                .repeated()
                .collect(),
        )
        .then_ignore(end())
}

macro_rules! define_sub_parser {
    ($name:ident, $type:ty, $parse:ident $(,)?) => {
        pub fn $name<'a, I, E>(options: &'a ParserOptions) -> impl Parser<'a, I, $type, E> + Clone
        where
            I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
            I::Span: Into<TokenSpan> + Clone,
            E: ParserExtra<'a, I> + 'a,
            E::Error: LabelError<'a, I, TokenLabel>,
        {
            $parse(options)
                .padded_by(whitespace().repeated())
                .then_ignore(end())
        }
    };
}

define_sub_parser!(create_data_type_parser, DataType, data_type);
define_sub_parser!(create_object_name_parser, ObjectName, object_name);
define_sub_parser!(
    create_interval_literal_parser,
    IntervalLiteral,
    interval_literal,
);
define_sub_parser!(
    create_numeric_interval_literal_parser,
    IntervalLiteral,
    numeric_interval_literal,
);
