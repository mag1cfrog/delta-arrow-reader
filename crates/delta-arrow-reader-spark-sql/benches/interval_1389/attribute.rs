
mod interval_attribution {
    use std::cell::Cell;
    use std::hint::black_box;
    use std::time::Instant;

    use super::interval_parser;

    #[derive(Clone, Copy, Default)]
    struct Phases {
        ns: [u128; 4],
        constructions: u64,
    }

    thread_local! {
        static PHASES: Cell<Phases> = Cell::new(Phases::default());
    }

    pub(super) fn timed<T>(phase: usize, measure: impl FnOnce() -> T) -> T {
        let before = PHASES.get();
        let start = Instant::now();
        let result = measure();
        let elapsed = start.elapsed().as_nanos();
        let mut after = PHASES.get();
        let nested = after.ns.iter().sum::<u128>() - before.ns.iter().sum::<u128>();
        after.ns[phase] += elapsed.saturating_sub(nested);
        after.constructions += u64::from(phase == 0);
        PHASES.set(after);
        result
    }

    #[test]
    fn split_interval_conversion() -> Result<(), Box<dyn std::error::Error>> {
        const ROWS: usize = 8192;
        let shapes = [
            ("year_month", " months", 0),
            ("day_time", " days 2 hours", 0),
            ("calendar", " months 1 day 2 seconds", 0),
            ("quoted_year_month", " months", 1),
            ("double_quoted_year_month", " months", 2),
        ];
        let inputs: Vec<Vec<String>> = shapes
            .iter()
            .map(|(_, suffix, quotes)| {
                (1..=7)
                    .map(|line| {
                        let value = format!("{line}{suffix}");
                        match quotes {
                            1 => format!("'{value}'"),
                            2 => format!("\"'{value}'\""),
                            _ => value,
                        }
                    })
                    .collect()
            })
            .collect();
        for (shape, inputs) in inputs.iter().enumerate() {
            let mut parse = interval_parser();
            for input in inputs.iter().cycle().take(256) {
                black_box(parse(black_box(input))?);
            }
            assert_eq!(PHASES.get().constructions >= 2, true, "shape {shape}");
        }
        println!("sample,shape,rows,total_ns,construction_ns,lex_ns,parse_ns,value_ns,constructions");
        for sample in 0..7 {
            for position in 0..shapes.len() {
                let shape = (sample + position) % shapes.len();
                PHASES.set(Phases::default());
                let start = Instant::now();
                let mut parse = interval_parser();
                for input in inputs[shape].iter().cycle().take(ROWS) {
                    black_box(parse(black_box(input))?);
                }
                drop(parse);
                let elapsed = start.elapsed().as_nanos();
                let Phases {
                    ns: [construction, lex, parse, value],
                    constructions,
                } = PHASES.get();
                let (name, _, quotes) = shapes[shape];
                assert_eq!(constructions, 2 * (1 + (ROWS * quotes) as u64));
                println!("{sample},{name},{ROWS},{elapsed},{construction},{lex},{parse},{value},{constructions}");
            }
        }
        Ok(())
    }
}
