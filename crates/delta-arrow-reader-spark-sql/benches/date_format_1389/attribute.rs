#[cfg(test)]
mod date_format_attribution {
    use std::hint::black_box;
    use std::time::Instant;

    use datafusion::arrow::array::ArrayRef;
    use datafusion::arrow::datatypes::Field;
    use datafusion::common::config::ConfigOptions;
    use datafusion_functions::datetime::to_char::ToCharFunc;

    use super::*;

    const ZONE: &str = "UTC";
    const SPARK_PATTERN: &str = "yyyy/MM/dd HH:mm";
    const NATIVE_PATTERN: &str = "%Y/%m/%d %H:%M";
    const ROWS: usize = 8192;
    const BATCHES: usize = 32;
    const OPERATIONS: [&str; 5] = [
        "spark_parse",
        "native_parse",
        "spark_format",
        "native_format",
        "spark_grammar",
    ];

    fn native_parse(input: &dyn Array, timestamp_type: &DataType) -> Result<ArrayRef> {
        Ok(datafusion::arrow::compute::cast(input, timestamp_type)?)
    }

    fn native_format(formatter: &ToCharFunc, timestamp: &ArrayRef) -> Result<ColumnarValue> {
        formatter.invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(timestamp.clone()),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(NATIVE_PATTERN.to_owned()))),
            ],
            arg_fields: vec![
                Arc::new(Field::new("timestamp", timestamp.data_type().clone(), true)),
                Arc::new(Field::new("pattern", DataType::Utf8, false)),
            ],
            return_field: Arc::new(Field::new("text", DataType::Utf8, true)),
            number_rows: timestamp.len(),
            config_options: Arc::new(ConfigOptions::default()),
        })
    }

    #[test]
    fn split_string_parsing_and_formatting() -> Result<()> {
        let start = chrono::NaiveDate::from_ymd_opt(1992, 1, 1)
            .ok_or_else(|| exec_datafusion_err!("invalid probe start date"))?;
        let data = (0..ROWS)
            .map(|index| {
                (!index.is_multiple_of(97)).then(|| {
                    let date = start + chrono::Duration::days(((index * 669) % 2557) as i64);
                    format!("{date} 12:34:56")
                })
            })
            .collect::<Vec<_>>();
        let strings = StringArray::from(data);
        let input: ArrayRef = Arc::new(strings.clone());
        let tz: Arc<str> = Arc::from(ZONE);
        let timestamp_type = DataType::Timestamp(TimeUnit::Microsecond, Some(tz.clone()));
        let timestamp = native_parse(input.as_ref(), &timestamp_type)?;
        let spark_timestamp: ArrayRef =
            Arc::new(parse_string_array_to_timestamp(&input, ZONE)?.with_timezone(tz.clone()));
        assert_eq!(spark_timestamp.to_data(), timestamp.to_data());
        let spark_pattern = DateTimeFormat::for_formatting(SPARK_PATTERN)?;
        let native_formatter = ToCharFunc::new();
        let spark_result =
            format_timestamp_array_microsecond(&timestamp, Some(&tz), &spark_pattern)?;
        let native_result = native_format(&native_formatter, &timestamp)?.into_array(ROWS)?;
        assert_eq!(native_result.data_type(), &DataType::Utf8);
        assert_eq!(spark_result.to_data(), native_result.to_data());

        let run = |operation| -> Result<()> {
            match operation {
                "spark_parse" => {
                    black_box(parse_string_array_to_timestamp(black_box(&input), ZONE)?);
                }
                "native_parse" => {
                    black_box(native_parse(black_box(input.as_ref()), &timestamp_type)?);
                }
                "spark_format" => {
                    black_box(format_timestamp_array_microsecond(
                        black_box(&timestamp),
                        Some(&tz),
                        &spark_pattern,
                    )?);
                }
                "native_format" => {
                    black_box(native_format(&native_formatter, black_box(&timestamp))?);
                }
                "spark_grammar" => {
                    let mut parse = timestamp_parser();
                    for text in strings.iter().flatten() {
                        let value = parse(black_box(text))
                            .and_then(TimestampValue::into_naive)
                            .map_err(|error| exec_datafusion_err!("{error}"))?;
                        black_box(value);
                    }
                }
                _ => unreachable!("unsupported attribution operation"),
            }
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
