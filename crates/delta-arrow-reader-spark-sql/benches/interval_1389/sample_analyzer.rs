//! Runs the runtime parser without the SQL executor for bounded CPU sampling.

use std::hint::black_box;

use sail_sql_analyzer::parser::interval_parser;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let shape = std::env::args().nth(1).ok_or("missing interval shape")?;
    let suffix = match shape.as_str() {
        "interval_year_month" => " months",
        "interval_day_time" => " days 2 hours",
        "interval_calendar" => " months 1 day 2 seconds",
        _ => return Err(format!("unknown interval shape {shape:?}").into()),
    };
    let input: Vec<String> = (1..=7).map(|line| format!("{line}{suffix}")).collect();
    for _ in 0..64 {
        let mut parse = interval_parser();
        for text in input.iter().cycle().take(8192) {
            black_box(parse(black_box(text))?);
        }
    }
    Ok(())
}
