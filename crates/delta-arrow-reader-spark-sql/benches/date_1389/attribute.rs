//! Compare the fresh and reverted DATE parsers with the same row loops.

use std::hint::black_box;
use std::time::Instant;

use arrow::array::{Date32Array, StringArray};
use arrow::datatypes::Date32Type;
use chumsky::Parser;
use chumsky::error::Rich;
use chumsky::extra::Err;
use sail_sql_analyzer::error::{SqlError, SqlResult};
use sail_sql_analyzer::literal::datetime::{DateValue, create_date_parser};
use sail_sql_analyzer::parser::parse_date;

const SHAPES: [&str; 7] = [
    "array_fresh",
    "array_lazy",
    "parse_fresh",
    "parse_lazy",
    "parse_eager",
    "construct",
    "convert",
];

// The reverted per-batch factory from 1f6cb4fcd6ebbce3b993f411b9f66571ec6d230d.
fn date_parser<'a>() -> impl FnMut(&'a str) -> SqlResult<DateValue> {
    let mut parser = None;
    move |s| {
        let s = s.trim_matches(|c: char| c.is_ascii_control() || c == ' ');
        parser
            .get_or_insert_with(create_date_parser::<Err<Rich<_, _>>>)
            .parse(s)
            .into_result()
            .map_err(SqlError::parser)
    }
}

fn date32(value: DateValue) -> SqlResult<i32> {
    Ok(Date32Type::from_naive_date(value.try_into()?))
}

// This probe's strings are all shorter than ten bytes, so the canonical DATE
// branch can only decline. Leave its original dispatch in the array loop.
fn date_array<'a>(
    inputs: &'a StringArray,
    mut parse: impl FnMut(&'a str) -> SqlResult<DateValue>,
) -> SqlResult<Date32Array> {
    use arrow::compute::kernels::cast_utils::Parser as ArrowParser;

    inputs
        .iter()
        .map(|value| {
            value
                .map(|value| {
                    let bytes = value.as_bytes();
                    if bytes.len() == 10
                        && bytes.iter().enumerate().all(|(index, byte)| match index {
                            4 | 7 => *byte == b'-',
                            _ => byte.is_ascii_digit(),
                        })
                        && let Some(date) = Date32Type::parse(value)
                    {
                        return Ok(date);
                    }
                    date32(parse(value)?)
                })
                .transpose()
        })
        .collect()
}

fn parse_values<'a>(
    input: &'a [&'a str],
    mut parse: impl FnMut(&'a str) -> SqlResult<DateValue>,
) -> SqlResult<()> {
    for text in input {
        black_box(parse(black_box(text))?);
    }
    Ok(())
}

fn measure(
    case: &str,
    strings: &StringArray,
    input: &[&str],
    values: &[DateValue],
    expected: &Date32Array,
    batches: usize,
) -> SqlResult<u128> {
    let start = Instant::now();
    for _ in 0..batches {
        match case {
            "array_fresh" => assert_eq!(&date_array(black_box(strings), parse_date)?, expected),
            "array_lazy" => assert_eq!(&date_array(black_box(strings), date_parser())?, expected),
            "parse_fresh" => parse_values(input, parse_date)?,
            "parse_lazy" => parse_values(input, date_parser())?,
            "parse_eager" => {
                let parser = create_date_parser::<Err<Rich<_, _>>>();
                parse_values(input, |s| {
                    let s = s.trim_matches(|c: char| c.is_ascii_control() || c == ' ');
                    parser.parse(s).into_result().map_err(SqlError::parser)
                })?;
            }
            "construct" => {
                for _ in input {
                    black_box(create_date_parser::<Err<Rich<_, _>>>());
                }
            }
            "convert" => {
                for value in values {
                    black_box(date32(black_box(value.clone()))?);
                }
            }
            _ => unreachable!("unsupported attribution case {case:?}"),
        }
    }
    Ok(start.elapsed().as_nanos())
}

fn main() -> SqlResult<()> {
    let dates = (1992..=1998)
        .map(|year| format!("{year}-2-9"))
        .collect::<Vec<_>>();
    let input = dates
        .iter()
        .map(String::as_str)
        .cycle()
        .take(8192)
        .collect::<Vec<_>>();
    let values = input
        .iter()
        .map(|s| parse_date(s))
        .collect::<SqlResult<Vec<_>>>()?;
    let strings: StringArray = input
        .iter()
        .enumerate()
        .map(|(index, s)| (!index.is_multiple_of(97)).then_some(*s))
        .collect();
    let expected = date_array(&strings, parse_date)?;
    for case in SHAPES {
        measure(case, &strings, &input, &values, &expected, 1)?;
    }
    println!("sample,operation,rows,elapsed_ns");
    for sample in 0..7 {
        for position in 0..SHAPES.len() {
            let case = SHAPES[(position + sample) % SHAPES.len()];
            let ns = measure(case, &strings, &input, &values, &expected, 16)?;
            println!("{sample},{case},131072,{ns}");
        }
    }
    Ok(())
}
