#[cfg(test)]
mod canonical_date_attribution {
    use std::hint::black_box;
    use std::time::Instant;

    use datafusion::arrow::array::{ArrayRef, StringArray};
    use datafusion::arrow::datatypes::Field;
    use datafusion::common::config::ConfigOptions;

    use super::*;

    const ROWS: usize = 8192;
    const BATCHES: usize = 64;
    const CASES: [&str; 4] = ["native", "spark", "row_guarded", "row_without_guard"];

    fn date_array(
        strings: &StringArray,
        parse: impl Fn(&str, bool) -> Result<Option<i32>>,
    ) -> Result<Date32Array> {
        strings
            .iter()
            .map(|text| {
                text.map(|text| parse(text, false))
                    .transpose()
                    .map(|result| result.flatten())
            })
            .collect()
    }

    #[test]
    fn attribute_complete_date_conversion() -> Result<()> {
        let input = (0..ROWS)
            .map(|row| (!(row.is_multiple_of(16))).then(|| format!("2020-01-{:02}", row % 7 + 1)))
            .collect::<Vec<_>>();
        let strings = StringArray::from(input);
        let array: ArrayRef = Arc::new(strings.clone());
        let date = SparkDate::new(false);
        let run = |case| -> Result<ArrayRef> {
            match case {
                "native" => Ok(datafusion::arrow::compute::cast(
                    black_box(array.as_ref()),
                    &DataType::Date32,
                )?),
                "spark" => date
                    .invoke_with_args(ScalarFunctionArgs {
                        args: vec![ColumnarValue::Array(black_box(array.clone()))],
                        arg_fields: vec![Arc::new(Field::new("text", DataType::Utf8, true))],
                        return_field: Arc::new(Field::new("date", DataType::Date32, true)),
                        number_rows: ROWS,
                        config_options: Arc::new(ConfigOptions::default()),
                    })?
                    .into_array(ROWS),
                "row_guarded" => Ok(Arc::new(date_array(
                    black_box(&strings),
                    SparkDate::string_to_date32,
                )?)),
                "row_without_guard" => Ok(Arc::new(date_array(
                    black_box(&strings),
                    SparkDate::string_to_date32_without_shape_check,
                )?)),
                _ => unreachable!("unsupported test case {case}"),
            }
        };
        let reference = run("native")?;
        for case in CASES {
            assert_eq!(run(case)?.to_data(), reference.to_data(), "{case}");
        }
        println!("sample,phase,rows,elapsed_ns");
        for sample in 0..7 {
            for position in 0..CASES.len() {
                let case = CASES[(sample + position) % CASES.len()];
                let start = Instant::now();
                for _ in 0..BATCHES {
                    black_box(run(case)?);
                }
                println!(
                    "{sample},{case},{},{}",
                    ROWS * BATCHES,
                    start.elapsed().as_nanos()
                );
            }
        }
        Ok(())
    }
}
