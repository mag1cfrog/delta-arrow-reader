//! Isolates the input shape check before Arrow's existing DATE32 parser.

use std::hint::black_box;
use std::time::Instant;

use arrow::compute::kernels::cast_utils::Parser;
use arrow::datatypes::Date32Type;

const DATES: [&str; 7] = [
    "1992-2-9", "1993-2-9", "1994-2-9", "1995-2-9", "1996-2-9", "1997-2-9", "1998-2-9",
];

fn parse_guarded(text: &str) -> Option<i32> {
    if has_unpadded_date_shape(text.as_bytes()) {
        Date32Type::parse(text)
    } else {
        None
    }
}

fn measure(rows: usize, parse: impl Fn(&str) -> Option<i32>) -> u128 {
    let start = Instant::now();
    for text in DATES.iter().cycle().take(rows) {
        black_box(parse(black_box(text)));
    }
    start.elapsed().as_nanos()
}

fn main() {
    const ROWS: usize = 131_072;
    for text in DATES {
        assert!(parse_guarded(text).is_some());
        assert_eq!(parse_guarded(text), Date32Type::parse(text));
    }
    measure(8192, parse_guarded);
    measure(8192, Date32Type::parse);
    println!("sample,rows,arrow_ns,guarded_ns");
    for sample in 0..7 {
        let (arrow, guarded) = if sample % 2 == 0 {
            (
                measure(ROWS, Date32Type::parse),
                measure(ROWS, parse_guarded),
            )
        } else {
            let guarded = measure(ROWS, parse_guarded);
            (measure(ROWS, Date32Type::parse), guarded)
        };
        println!("{sample},{ROWS},{arrow},{guarded}");
    }
}
