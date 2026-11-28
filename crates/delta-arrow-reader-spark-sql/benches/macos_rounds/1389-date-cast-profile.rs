#[cfg(test)]
mod macos_date_cast_attribution {
    use std::hint::black_box;
    use std::time::Instant;

    use datafusion::arrow::array::{ArrayRef, StringArray};
    use datafusion::arrow::datatypes::Field;
    use datafusion::common::config::ConfigOptions;

    use super::*;

    const ROWS: usize = 8192;
    const BATCHES: usize = 32;
    const OPERATIONS: [&str; 8] = [
        "short_spark",
        "short_native",
        "short_guarded",
        "short_arrow_rows",
        "canonical_spark",
        "canonical_native",
        "canonical_guarded",
        "canonical_arrow_rows",
    ];

    fn strings_into_view(values: Vec<Option<String>>) -> Result<ArrayRef> {
        Ok(datafusion::arrow::compute::cast(
            &StringArray::from(values),
            &DataType::Utf8View,
        )?)
    }

    fn row_cast(input: &ArrayRef, parse: impl Fn(&str) -> Result<Option<i32>>) -> Result<ArrayRef> {
        let dates: Date32Array = as_string_view_array(input)?
            .iter()
            .map(|value| value.map(&parse).transpose().map(Option::flatten))
            .collect::<Result<_>>()?;
        Ok(Arc::new(dates))
    }

    #[test]
    fn attribute_date_casts() -> Result<()> {
        let short = strings_into_view(
            (0..ROWS)
                .map(|index| {
                    (!index.is_multiple_of(97)).then(|| format!("{}-2-9", 1992 + index % 7))
                })
                .collect(),
        )?;
        let canonical = strings_into_view(
            (0..ROWS)
                .map(|index| Some(format!("{}-02-{:02}", 1992 + index % 7, 1 + index % 28)))
                .collect(),
        )?;
        let strict = SparkDate::new(false);
        let safe = SparkDate::new(true);
        let input_field = Arc::new(Field::new("text", DataType::Utf8View, true));
        let output_field = Arc::new(Field::new("date", DataType::Date32, true));
        let config = Arc::new(ConfigOptions::default());
        let spark = |input: &ArrayRef, udf: &SparkDate| {
            udf.invoke_with_args(ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(input.clone())],
                arg_fields: vec![input_field.clone()],
                return_field: output_field.clone(),
                number_rows: ROWS,
                config_options: config.clone(),
            })?
            .into_array(ROWS)
        };
        let native = |input: &ArrayRef| -> Result<ArrayRef> {
            Ok(datafusion::arrow::compute::cast(
                input.as_ref(),
                &DataType::Date32,
            )?)
        };
        let guarded =
            |input: &ArrayRef| row_cast(input, |text| SparkDate::string_to_date32(text, false));
        let arrow_rows = |input: &ArrayRef| row_cast(input, |text| Ok(Date32Type::parse(text)));
        for input in [&short, &canonical] {
            let expected = native(input)?;
            for observed in [
                spark(input, &strict)?,
                spark(input, &safe)?,
                guarded(input)?,
                arrow_rows(input)?,
            ] {
                assert_eq!(observed.to_data(), expected.to_data());
            }
        }

        let run = |operation| -> Result<()> {
            let output = match operation {
                "short_spark" => spark(black_box(&short), &strict)?,
                "short_native" => native(black_box(&short))?,
                "short_guarded" => guarded(black_box(&short))?,
                "short_arrow_rows" => arrow_rows(black_box(&short))?,
                "canonical_spark" => spark(black_box(&canonical), &strict)?,
                "canonical_native" => native(black_box(&canonical))?,
                "canonical_guarded" => guarded(black_box(&canonical))?,
                "canonical_arrow_rows" => arrow_rows(black_box(&canonical))?,
                _ => unreachable!("unsupported probe operation"),
            };
            black_box(output);
            Ok(())
        };

        for operation in OPERATIONS {
            run(operation)?;
        }
        println!("sample,phase,rows,elapsed_ns");
        for sample in 0..7 {
            for position in 0..OPERATIONS.len() {
                let operation = OPERATIONS[(sample + position) % OPERATIONS.len()];
                let start = Instant::now();
                for _ in 0..BATCHES {
                    run(operation)?;
                }
                println!(
                    "{sample},{operation},{},{}",
                    ROWS * BATCHES,
                    start.elapsed().as_nanos()
                );
            }
        }
        Ok(())
    }
}
