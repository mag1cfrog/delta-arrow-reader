//! Parser reuse retains the frozen row conversion results and connected Spark SQL behavior.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, DictionaryArray, Int32Array, StringViewArray, new_null_array};
use arrow::compute::{cast, concat};
use arrow::datatypes::{DataType, Field, Int32Type, IntervalUnit, TimeUnit};
use arrow::record_batch::RecordBatch;
use datafusion::common::{Result, ScalarValue, config::ConfigOptions};
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Volatility, create_udf,
};
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use sail_function::scalar::datetime::spark_date::SparkDate;
use sail_function::scalar::datetime::spark_date_format::SparkDateFormat;
use sail_function::scalar::datetime::spark_interval::{
    SparkCalendarInterval, SparkDayTimeInterval, SparkYearMonthInterval,
};
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;

#[path = "../vendor/sail/crates/sail-sql-analyzer/tests/fixtures/string-parser-1389-inputs.rs"]
mod inputs;
#[path = "fixtures/row-parser-1389-original.rs"]
mod original;

const DATES: &[&str] = &[
    "2024-02-29",
    "2024-2-9",
    "2024",
    "2024-2",
    " 2024-02-29 ",
    "\t2024-2-9\r\n",
    "2024-02-29 12:34:56",
    "2024-02-29Tanything",
    "0000-1-1",
    "-0001-1-1",
    "+10000-1-1",
    "9999999-12-31",
    "1900-02-29",
    "2000-02-29",
    "2024-13-1",
    "2024-1-32",
    "2024-00-1",
    "2024-1-0",
    "2024-02-29x",
    "2024-2 12:00:00",
    "",
    " ",
    "bad",
    "202",
    "2024/2/9",
    "2024-2-9\0",
    "\u{a0}2024-2-9",
    "2024-2-9\u{a0}",
    "2024-02-29T",
    "2024-02-29 t",
];

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone, Copy, Debug)]
enum StringConversion {
    Date,
    TimestampLtz,
    TimestampNtz,
    YearMonthInterval,
    DayTimeInterval,
    CalendarInterval,
    DateFormatDisplay,
}

impl StringConversion {
    fn data_type(self, timezone: &str) -> DataType {
        match self {
            Self::Date => DataType::Date32,
            Self::TimestampLtz => {
                DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from(timezone)))
            }
            Self::TimestampNtz => DataType::Timestamp(TimeUnit::Microsecond, None),
            Self::YearMonthInterval => DataType::Interval(IntervalUnit::YearMonth),
            Self::DayTimeInterval => DataType::Duration(TimeUnit::Microsecond),
            Self::CalendarInterval => DataType::Interval(IntervalUnit::MonthDayNano),
            Self::DateFormatDisplay => DataType::Utf8,
        }
    }

    fn udf(self, ansi: bool, is_try: bool, timezone: &str) -> Result<Arc<dyn ScalarUDFImpl>> {
        Ok(match self {
            Self::Date => Arc::new(SparkDate::new(is_try || !ansi)),
            Self::TimestampLtz | Self::TimestampNtz => Arc::new(SparkTimestamp::try_new(
                matches!(self, Self::TimestampLtz).then(|| Arc::from(timezone)),
                ansi,
                is_try,
            )?),
            Self::YearMonthInterval => Arc::new(SparkYearMonthInterval::new()),
            Self::DayTimeInterval => Arc::new(SparkDayTimeInterval::new()),
            Self::CalendarInterval => Arc::new(SparkCalendarInterval::new()),
            Self::DateFormatDisplay => Arc::new(SparkDateFormat::new(Arc::from(timezone))),
        })
    }

    fn corpus(self) -> &'static [&'static str] {
        match self {
            Self::Date => DATES,
            Self::TimestampLtz | Self::TimestampNtz | Self::DateFormatDisplay => inputs::TIMESTAMPS,
            _ => inputs::INTERVALS,
        }
    }

    fn sql(self, is_try: bool) -> String {
        let target = match self {
            Self::Date => "DATE",
            Self::TimestampLtz => "TIMESTAMP",
            Self::TimestampNtz => "TIMESTAMP_NTZ",
            Self::YearMonthInterval => "INTERVAL YEAR TO MONTH",
            Self::DayTimeInterval => "INTERVAL DAY TO SECOND",
            Self::CalendarInterval => "INTERVAL",
            Self::DateFormatDisplay => {
                return "date_format(s,'yyyy/MM/dd HH:mm:ss.SSSSSS XXX')".into();
            }
        };
        format!(
            "{}(s AS {target})",
            if is_try { "TRY_CAST" } else { "CAST" }
        )
    }
}

fn invoke(udf: &dyn ScalarUDFImpl, args: Vec<ColumnarValue>, rows: usize) -> Result<ColumnarValue> {
    let types = args
        .iter()
        .map(ColumnarValue::data_type)
        .collect::<Vec<_>>();
    udf.invoke_with_args(ScalarFunctionArgs {
        arg_fields: types
            .iter()
            .map(|t| Arc::new(Field::new("input", t.clone(), true)))
            .collect(),
        return_field: Arc::new(Field::new("r", udf.return_type(&types)?, true)),
        args,
        number_rows: rows,
        config_options: Arc::new(ConfigOptions::default()),
    })
}

fn original_scalar_conversion(
    kind: StringConversion,
    value: Option<&str>,
    ansi: bool,
    is_try: bool,
    timezone: &str,
) -> Result<ScalarValue> {
    let Some(value) = value else {
        return ScalarValue::try_from(&kind.data_type(timezone));
    };
    Ok(match kind {
        StringConversion::Date => {
            ScalarValue::Date32(original::string_to_date32(value, is_try || !ansi)?)
        }
        StringConversion::TimestampLtz | StringConversion::TimestampNtz => {
            let zone = matches!(kind, StringConversion::TimestampLtz).then(|| Arc::from(timezone));
            ScalarValue::TimestampMicrosecond(
                original::timestamp(value, zone.clone(), is_try || !ansi)?,
                zone,
            )
        }
        StringConversion::YearMonthInterval => {
            ScalarValue::IntervalYearMonth(Some(original::string_to_year_month_interval(value)?))
        }
        StringConversion::DayTimeInterval => {
            ScalarValue::DurationMicrosecond(Some(original::string_to_day_time_interval(value)?))
        }
        StringConversion::CalendarInterval => {
            ScalarValue::IntervalMonthDayNano(Some(original::string_to_calendar_interval(value)?))
        }
        StringConversion::DateFormatDisplay => {
            let timestamp = ScalarValue::TimestampMicrosecond(
                original::parse_timestamp_string(value, timezone)?,
                Some(Arc::from(timezone)),
            );
            let udf = SparkDateFormat::new(Arc::from(timezone));
            let result = invoke(
                &udf,
                vec![
                    ColumnarValue::Scalar(timestamp),
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                        "yyyy/MM/dd HH:mm:ss.SSSSSS XXX".into(),
                    ))),
                ],
                1,
            )?
            .into_array(1)?;
            ScalarValue::try_from_array(&result, 0)?
        }
    })
}

fn original_array_conversion(
    kind: StringConversion,
    values: &[Option<&str>],
    ansi: bool,
    is_try: bool,
    timezone: &str,
) -> Result<ArrayRef> {
    if values.is_empty() {
        return Ok(new_null_array(&kind.data_type(timezone), 0));
    }
    ScalarValue::iter_to_array(
        values
            .iter()
            .map(|value| original_scalar_conversion(kind, *value, ansi, is_try, timezone))
            .collect::<Result<Vec<_>>>()?,
    )
}

fn assert_same_result(actual: Result<ArrayRef>, expected: Result<ArrayRef>, context: &str) {
    assert_eq!(
        actual.is_ok(),
        expected.is_ok(),
        "{context}: actual={actual:?}, expected={expected:?}"
    );
    match (actual, expected) {
        (Ok(actual), Ok(expected)) => assert_eq!(actual.as_ref(), expected.as_ref(), "{context}"),
        (Err(actual), Err(expected)) => {
            assert_eq!(
                std::mem::discriminant(actual.find_root()),
                std::mem::discriminant(expected.find_root()),
                "{context}"
            );
            assert_eq!(
                actual.find_root().to_string(),
                expected.find_root().to_string(),
                "{context}"
            );
        }
        _ => {}
    }
}

#[test]
fn direct_row_kernels_match_frozen_unformatted_conversions() -> TestResult {
    for kind in [
        StringConversion::Date,
        StringConversion::TimestampLtz,
        StringConversion::TimestampNtz,
        StringConversion::YearMonthInterval,
        StringConversion::DayTimeInterval,
        StringConversion::CalendarInterval,
        StringConversion::DateFormatDisplay,
    ] {
        for timezone in ["UTC", "America/New_York"] {
            for ansi in [false, true] {
                for is_try in [false, true] {
                    let udf = kind.udf(ansi, is_try, timezone)?;
                    let corpus = kind.corpus();
                    let all = corpus
                        .iter()
                        .copied()
                        .map(Some)
                        .chain([None])
                        .collect::<Vec<_>>();
                    let mut groups = vec![vec![], vec![None, None], all];
                    groups.extend(corpus.iter().map(|value| vec![None, Some(*value), None]));
                    for values in groups {
                        for storage in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
                            let array = cast(&StringViewArray::from(values.clone()), &storage)?;
                            let mut args = vec![ColumnarValue::Array(array)];
                            if matches!(kind, StringConversion::DateFormatDisplay) {
                                args.push(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                                    "yyyy/MM/dd HH:mm:ss.SSSSSS XXX".into(),
                                ))));
                            }
                            let actual = invoke(udf.as_ref(), args, values.len());
                            if let Ok(value) = &actual {
                                assert!(matches!(value, ColumnarValue::Array(_)));
                            }
                            assert_same_result(
                                actual.and_then(|v| v.into_array(values.len())),
                                original_array_conversion(kind, &values, ansi, is_try, timezone),
                                &format!(
                                    "{kind:?}, {values:?}, {storage}, {ansi}, {is_try}, {timezone}"
                                ),
                            );
                        }
                    }
                    for value in corpus.iter().copied().map(Some).chain([None]) {
                        let mut args = vec![ColumnarValue::Scalar(ScalarValue::Utf8(
                            value.map(str::to_owned),
                        ))];
                        if matches!(kind, StringConversion::DateFormatDisplay) {
                            args.push(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                                "yyyy/MM/dd HH:mm:ss.SSSSSS XXX".into(),
                            ))));
                        }
                        let actual = invoke(udf.as_ref(), args, 1);
                        if let Ok(value) = &actual {
                            assert!(matches!(value, ColumnarValue::Scalar(_)));
                        }
                        assert_same_result(
                            actual.and_then(|v| v.into_array(1)),
                            original_array_conversion(kind, &[value], ansi, is_try, timezone),
                            &format!("scalar {kind:?} {value:?}"),
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

async fn query(
    session: &SessionContext,
    expression: &str,
    ansi: bool,
    timezone: &str,
) -> std::result::Result<ArrayRef, (&'static str, datafusion::common::DataFusionError)> {
    let planned = plan_query(
        session,
        &format!("SELECT {expression} AS r FROM inputs"),
        &SparkSqlOptions {
            ansi_mode: ansi,
            session_timezone: timezone.into(),
            ..Default::default()
        },
    )
    .await
    .map_err(|error| ("resolve", error))?;
    let physical = planned
        .create_physical_plan(session)
        .await
        .map_err(|error| ("physical planning", error))?;
    let data_type = physical.schema().field(0).data_type().clone();
    let batches = collect(physical, session.task_ctx())
        .await
        .map_err(|error| ("execution", error))?;
    if batches.is_empty() {
        return Ok(new_null_array(&data_type, 0));
    }
    concat(
        &batches
            .iter()
            .map(|batch| batch.column(0).as_ref())
            .collect::<Vec<_>>(),
    )
    .map_err(|error| ("result concatenation", error.into()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spark_casts_and_date_format_match_frozen_conversion_paths() -> TestResult {
    for kind in [
        StringConversion::Date,
        StringConversion::TimestampLtz,
        StringConversion::TimestampNtz,
        StringConversion::YearMonthInterval,
        StringConversion::DayTimeInterval,
        StringConversion::CalendarInterval,
        StringConversion::DateFormatDisplay,
    ] {
        for storage in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
            for dictionary in [false, true] {
                for timezone in ["UTC", "America/New_York"] {
                    let values = kind
                        .corpus()
                        .iter()
                        .copied()
                        .map(Some)
                        .chain([None])
                        .collect::<Vec<_>>();
                    let decoded = cast(&StringViewArray::from(values), &storage)?;
                    let input: ArrayRef = if dictionary {
                        Arc::new(DictionaryArray::<Int32Type>::try_new(
                            Int32Array::from_iter_values(0..decoded.len() as i32),
                            decoded,
                        )?)
                    } else {
                        decoded
                    };
                    let session = SessionContext::new_with_state(
                        configure_spark_sql(
                            SessionStateBuilder::new()
                                .with_default_features()
                                .with_config(
                                    SessionConfig::new()
                                        .with_target_partitions(1)
                                        .with_batch_size(32),
                                ),
                        )
                        .build(),
                    );
                    session.register_batch(
                        "inputs",
                        RecordBatch::try_from_iter(vec![("s", input.clone())])?,
                    )?;
                    for ansi in [false, true] {
                        for is_try in [false, true] {
                            let original_type = input.data_type().clone();
                            let result_type = kind.data_type(timezone);
                            let zone = timezone.to_owned();
                            session.register_udf(create_udf(
                                "original_1389",
                                vec![original_type],
                                result_type,
                                Volatility::Immutable,
                                Arc::new(move |args| {
                                    let arrays = ColumnarValue::values_to_arrays(args)?;
                                    let mut values = Vec::new();
                                    for row in 0..arrays[0].len() {
                                        let mut value =
                                            ScalarValue::try_from_array(&arrays[0], row)?;
                                        if let ScalarValue::Dictionary(_, inner) = value {
                                            value = *inner;
                                        }
                                        values
                                            .push(value.try_as_str().flatten().map(str::to_owned));
                                    }
                                    let values = values
                                        .iter()
                                        .map(|value| value.as_deref())
                                        .collect::<Vec<_>>();
                                    Ok(ColumnarValue::Array(original_array_conversion(
                                        kind, &values, ansi, is_try, &zone,
                                    )?))
                                }),
                            ));
                            let context = format!(
                                "SQL {kind:?}, {storage}, dictionary={dictionary}, ansi={ansi}, try={is_try}, {timezone}"
                            );
                            let actual = query(&session, &kind.sql(is_try), ansi, timezone).await;
                            let expected =
                                query(&session, "original_1389(s)", ansi, timezone).await;
                            assert_eq!(
                                actual.is_ok(),
                                expected.is_ok(),
                                "{context}: actual={actual:?}, expected={expected:?}"
                            );
                            match (actual, expected) {
                                (Ok(actual), Ok(expected)) => {
                                    assert_same_result(Ok(actual), Ok(expected), &context)
                                }
                                (Err((actual_phase, actual)), Err((expected_phase, expected))) => {
                                    assert_eq!(actual_phase, expected_phase, "{context}");
                                    assert_same_result(Err(actual), Err(expected), &context);
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(())
}
