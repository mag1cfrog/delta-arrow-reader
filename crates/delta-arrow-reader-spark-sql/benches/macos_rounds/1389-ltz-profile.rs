#[cfg(test)]
mod macos_ltz_attribution {
    use std::hint::black_box;
    use std::time::Instant;

    use datafusion::arrow::array::{ArrayRef, StringArray};
    use datafusion::arrow::datatypes::Field;
    use datafusion::common::config::ConfigOptions;

    use super::*;

    const ROWS: usize = 8192;
    const BATCHES: usize = 32;
    const OPERATIONS: [&str; 5] = [
        "spark_array",
        "native_array",
        "spark_localize",
        "utc_direct",
        "spark_fields",
    ];

    #[test]
    fn attribute_ordinary_utc_timestamp() -> Result<()> {
        let start = NaiveDate::from_ymd_opt(1992, 1, 1)
            .ok_or_else(|| exec_datafusion_err!("invalid probe start date"))?;
        let dates = (0..ROWS)
            .map(|i| start + chrono::Duration::days(((i * 669) % 2557) as i64))
            .map(|date| {
                date.and_hms_opt(12, 34, 56)
                    .ok_or_else(|| exec_datafusion_err!("invalid probe clock"))
            })
            .collect::<Result<Vec<_>>>()?;
        let strings = StringArray::from(
            dates
                .iter()
                .enumerate()
                .map(|(index, datetime)| {
                    (!index.is_multiple_of(97))
                        .then(|| datetime.format("%Y-%m-%d %H:%M:%S").to_string())
                })
                .collect::<Vec<_>>(),
        );
        let array = datafusion::arrow::compute::cast(&strings, &DataType::Utf8View)?;
        let zone: Arc<str> = Arc::from("UTC");
        let result_type = DataType::Timestamp(TimeUnit::Microsecond, Some(zone.clone()));
        let cast = SparkTimestamp::try_new(Some(zone), true, false)?;
        let spark = |udf: &SparkTimestamp| {
            udf.invoke_with_args(ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(black_box(array.clone()))],
                arg_fields: vec![Arc::new(Field::new("text", DataType::Utf8View, true))],
                return_field: Arc::new(Field::new("timestamp", result_type.clone(), true)),
                number_rows: ROWS,
                config_options: Arc::new(ConfigOptions::default()),
            })?
            .into_array(ROWS)
        };
        let native = || -> Result<ArrayRef> {
            Ok(datafusion::arrow::compute::cast(
                black_box(array.as_ref()),
                &result_type,
            )?)
        };
        let native_result: ArrayRef = native()?;
        assert_eq!(spark(&cast)?.to_data(), native_result.to_data());
        let nonansi = SparkTimestamp::try_new(cast.timezone.clone(), false, false)?;
        assert_eq!(spark(&nonansi)?.to_data(), native_result.to_data());
        for (text, date) in strings.iter().zip(&dates) {
            if let Some(text) = text {
                assert_eq!(parse_ascii_whole_second_timestamp(text), Some(*date));
                assert_eq!(
                    cast.parser.localize(*date, "", false)?,
                    Some(date.and_utc().timestamp_micros())
                );
            }
        }

        let run = |operation| -> Result<()> {
            match operation {
                "spark_array" => {
                    black_box(spark(&cast)?);
                }
                "native_array" => {
                    black_box(native()?);
                }
                "spark_localize" => {
                    for (i, date) in dates.iter().enumerate() {
                        if !i.is_multiple_of(97) {
                            black_box(cast.parser.localize(black_box(*date), "", false)?);
                        }
                    }
                }
                "utc_direct" => {
                    for (i, date) in dates.iter().enumerate() {
                        if !i.is_multiple_of(97) {
                            black_box(black_box(*date).and_utc().timestamp_micros());
                        }
                    }
                }
                "spark_fields" => {
                    for text in strings.iter().flatten() {
                        black_box(parse_ascii_whole_second_timestamp(black_box(text)));
                    }
                }
                _ => unreachable!("unsupported probe operation"),
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
