//! Runs the interval parser on the SF1 string expressions for CPU sampling.

use std::hint::black_box;

use sail_sql_analyzer::parser::interval_parser;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let shape = std::env::args().nth(1).ok_or("missing interval shape")?;
    let inputs: Vec<_> = (1..=7)
        .map(|line| match shape.as_str() {
            "year_month" => Ok(format!("{line} months")),
            "day_time" => Ok(format!("{line} days 2 hours")),
            "calendar" => Ok(format!("{line} months 1 day 2 seconds")),
            "quoted_year_month" => Ok(format!("'{line} months'")),
            _ => Err("unknown interval shape"),
        })
        .collect::<Result<_, _>>()?;
    for _ in 0..256 {
        let mut parse = interval_parser();
        for text in inputs.iter().cycle().take(8192) {
            black_box(parse(black_box(text))?);
        }
    }
    println!("{}", 256 * 8192);
    Ok(())
}
