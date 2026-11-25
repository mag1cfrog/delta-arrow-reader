//! Preserve timestamp values and argument short-circuiting across zone conversions.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, Date32Array, Int64Array, StringArray, Time64MicrosecondArray,
    TimestampMicrosecondArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, TimeUnit, TimestampMicrosecondType};
use arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::error::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use sail_function::scalar::datetime::convert_tz::ConvertTz;
use serde_json::{Value, json};

const QUERIES: &[(&str, &str, &str)] = &[
    ("utc", "convert_timezone('UTC','UTC',ntz)", ""),
    (
        "named",
        "convert_timezone('UTC','America/Los_Angeles',ntz)",
        "",
    ),
    (
        "dst_local",
        "convert_timezone('America/Los_Angeles','UTC',ntz)",
        "",
    ),
    ("offset", "convert_timezone('UTC','+05:45',ntz)", ""),
    (
        "offset_limits",
        "convert_timezone('+18:00','-18:00',ntz)",
        "",
    ),
    ("implicit_source", "convert_timezone('+05:45',ntz)", ""),
    ("legacy_alias", "convert_timezone('PST','UTC',ntz)", ""),
    ("dynamic", "convert_timezone(zone_name,'UTC',ntz)", ""),
    (
        "null_source",
        "convert_timezone(CAST(NULL AS STRING),'Bad/Zone',ntz)",
        "",
    ),
    (
        "null_target",
        "convert_timezone('Bad/Zone',CAST(NULL AS STRING),ntz)",
        "",
    ),
    (
        "null_timestamp",
        "convert_timezone('Bad/Zone','UTC',ntz)",
        " WHERE id = 0",
    ),
    (
        "invalid_source",
        "convert_timezone('Bad/Zone','UTC',ntz)",
        " WHERE id = 1",
    ),
    (
        "invalid_target",
        "convert_timezone('UTC','Bad/Zone',ntz)",
        " WHERE id = 1",
    ),
    (
        "invalid_timestamp",
        "convert_timezone('UTC','UTC',CAST('not a timestamp' AS TIMESTAMP_NTZ))",
        " WHERE id = 1",
    ),
    (
        "null_source_skips_later_expressions",
        "convert_timezone(CAST(NULL AS STRING),CAST(CAST(concat('bad',CAST(id AS STRING)) AS INT) AS STRING),CAST(concat('bad',CAST(id AS STRING)) AS TIMESTAMP_NTZ))",
        " WHERE id = 1",
    ),
    (
        "null_target_skips_timestamp",
        "convert_timezone('UTC',CAST(NULL AS STRING),CAST(concat('bad',CAST(id AS STRING)) AS TIMESTAMP_NTZ))",
        " WHERE id = 1",
    ),
    (
        "invalid_source_precedes_invalid_target",
        "convert_timezone('Bad/Source','Bad/Target',ntz)",
        " WHERE id = 1",
    ),
    (
        "empty_with_invalid_timestamp",
        "convert_timezone('UTC','UTC',CAST('not a timestamp' AS TIMESTAMP_NTZ))",
        " WHERE false",
    ),
    ("empty", "convert_timezone('UTC','UTC',ntz)", " WHERE false"),
    (
        "from_utc_named",
        "from_utc_timestamp(ltz,'America/Los_Angeles')",
        "",
    ),
    (
        "to_utc_named",
        "to_utc_timestamp(ltz,'America/Los_Angeles')",
        "",
    ),
];

fn conversion_session() -> Result<SessionContext> {
    let session = SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(
                    SessionConfig::new()
                        .with_batch_size(3)
                        .with_target_partitions(1),
                ),
        )
        .build(),
    );
    let timestamps = TimestampMicrosecondArray::from(vec![
        None,
        Some(0),
        Some(1_710_037_800_000_000), // Wall clock in the Los Angeles spring gap.
        Some(1_730_597_400_000_000), // Wall clock in the fall overlap.
        Some(1_710_066_600_000_000),
        Some(1_730_626_200_000_000),
        Some(i64::MIN),
        Some(i64::MAX),
        Some(-1),
    ]);
    let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(0..timestamps.len() as i64));
    let ltz: ArrayRef = Arc::new(timestamps.clone().with_timezone("UTC"));
    let ntz: ArrayRef = Arc::new(timestamps);
    let zones: ArrayRef = Arc::new(StringArray::from(vec![
        Some("Bad/Zone"),
        None,
        Some("America/Los_Angeles"),
        Some("America/Los_Angeles"),
        Some("+05:45"),
        Some("UTC"),
        Some("UTC"),
        Some("UTC"),
        Some("-18:00"),
    ]));
    session.register_batch(
        "observations",
        RecordBatch::try_from_iter([
            ("id", ids),
            ("ntz", ntz),
            ("ltz", ltz),
            ("zone_name", zones),
        ])?,
    )?;
    Ok(session)
}

async fn execute_conversion(
    session: &SessionContext,
    sql: &str,
    options: &SparkSqlOptions,
) -> Result<Value> {
    let query = plan_query(session, sql, options).await?;
    let plan = query.create_physical_plan(session).await?;
    let schema = plan.schema();
    let types: Vec<_> = schema
        .fields()
        .iter()
        .map(|field| format!("{:?}", field.data_type()))
        .collect();
    let mut rows = Vec::new();
    for batch in collect(plan, session.task_ctx()).await? {
        assert_eq!(batch.schema().fields(), schema.fields(), "{sql}");
        for row in 0..batch.num_rows() {
            let values = batch
                .columns()
                .iter()
                .map(|col| match ScalarValue::try_from_array(col, row)? {
                    ScalarValue::Int64(value)
                    | ScalarValue::TimestampMicrosecond(value, _)
                    | ScalarValue::TimestampNanosecond(value, _) => Ok(json!(value)),
                    other => Err(DataFusionError::Internal(format!(
                        "Unexpected conversion result type {}",
                        other.data_type()
                    ))),
                })
                .collect::<Result<Vec<_>>>()?;
            rows.push(values);
        }
    }
    Ok(json!({"types":types,"rows":rows}))
}

fn check_staging(observations: Vec<Value>) -> Result<()> {
    let staging: Value = serde_json::from_str(include_str!(
        "fixtures/timezone-conversion-values/staging.json"
    ))
    .map_err(|error| DataFusionError::External(Box::new(error)))?;
    assert_eq!(Value::Array(observations), staging["observations"]);
    Ok(())
}

#[tokio::test]
async fn timezone_conversions_match_staging_in_both_ansi_modes() -> Result<()> {
    let session = conversion_session()?;
    let mut observations = Vec::new();
    for ansi_mode in [false, true] {
        for session_zone in ["UTC", "America/Los_Angeles"] {
            let options = SparkSqlOptions {
                ansi_mode,
                session_timezone: session_zone.into(),
                ..SparkSqlOptions::default()
            };
            for (name, expression, filter) in QUERIES {
                let sql = format!(
                    "SELECT id,{expression} AS converted FROM observations{filter} ORDER BY id"
                );
                let result = execute_conversion(&session, &sql, &options)
                    .await
                    .unwrap_or_else(|error| json!({"error":error.to_string()}));
                observations.push(json!({"name":name,"ansi_mode":ansi_mode,"session_zone":session_zone,"result":result}));
            }
        }
    }
    check_staging(observations)
}

fn check_fixed_zone_staging(observations: Vec<Value>) -> Result<()> {
    let staging: Value = serde_json::from_str(include_str!(
        "fixtures/timezone-conversion-values/fixed-zones-staging.json"
    ))
    .map_err(|error| DataFusionError::External(Box::new(error)))?;
    assert_eq!(Value::Array(observations), staging["observations"]);
    Ok(())
}

#[tokio::test]
async fn fixed_zone_conversions_preserve_timestamps_and_labels() -> Result<()> {
    let session = conversion_session()?;
    let mut observations = Vec::new();
    for ansi_mode in [false, true] {
        for session_zone in ["UTC", "America/Los_Angeles"] {
            let options = SparkSqlOptions {
                ansi_mode,
                session_timezone: session_zone.into(),
                ..SparkSqlOptions::default()
            };
            for (expression, filter) in [
                ("from_utc_timestamp(ltz,'UTC')", ""),
                ("to_utc_timestamp(ltz,'UTC')", ""),
                ("from_utc_timestamp(ltz,'UTC')", " WHERE id <= 5"),
                ("to_utc_timestamp(ltz,'UTC')", " WHERE id <= 5"),
                ("from_utc_timestamp(ltz,'+05:45')", ""),
                ("to_utc_timestamp(ltz,'+05:45')", ""),
                ("from_utc_timestamp(ltz,'-18:00')", ""),
                ("to_utc_timestamp(ltz,'+18:00')", ""),
                ("convert_timezone('UTC','Z',ntz)", ""),
                ("convert_timezone('Z','Z',ntz)", ""),
            ] {
                let sql = format!(
                    "SELECT id,{expression} AS converted FROM observations{filter} ORDER BY id"
                );
                let result = execute_conversion(&session, &sql, &options)
                    .await
                    .unwrap_or_else(|error| json!({"error":error.to_string()}));
                observations.push(json!({
                    "expression":expression,"filter":filter,"ansi_mode":ansi_mode,
                    "session_zone":session_zone,"result":result
                }));
            }
        }
    }
    check_fixed_zone_staging(observations)
}

fn boundary_records(input: &str) -> Result<Vec<Value>> {
    input
        .lines()
        .map(serde_json::from_str)
        .collect::<std::result::Result<_, _>>()
        .map_err(|error| DataFusionError::External(Box::new(error)))
}

#[tokio::test]
async fn calendar_overflows_match_spark_at_both_ends_of_the_range() -> Result<()> {
    let records = boundary_records(include_str!(
        "fixtures/time-zone-range-1513/spark-outcomes.jsonl"
    ))?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records.len(), 4_637);
    let session = conversion_session()?;
    let mut batches: BTreeMap<_, Vec<&Value>> = BTreeMap::new();
    for row in &records[1..] {
        let key = (
            row["route"].as_str().unwrap(),
            row["ansi"].as_bool().unwrap(),
            row["result"]["status"].as_str().unwrap(),
            (row["result"]["status"] == "error").then(|| row["id"].as_i64().unwrap()),
        );
        batches.entry(key).or_default().push(row);
    }
    for ((route, ansi_mode, status, _), cases) in batches {
        let id: ArrayRef = Arc::new(Int64Array::from_iter_values(
            cases.iter().map(|row| row["id"].as_i64().unwrap()),
        ));
        let times = TimestampMicrosecondArray::from_iter_values(
            cases
                .iter()
                .map(|row| row["input_micros"].as_i64().unwrap()),
        );
        let from: ArrayRef = Arc::new(StringArray::from_iter_values(
            cases.iter().map(|row| row["from"].as_str().unwrap()),
        ));
        let to: ArrayRef = Arc::new(StringArray::from_iter_values(
            cases.iter().map(|row| row["to"].as_str().unwrap()),
        ));
        if route == "non_classic" {
            // The Spark composition in the capture models the internal two-zone conversion.
            let arguments = [from, to, Arc::new(times) as ArrayRef];
            let fields = arguments
                .iter()
                .map(|arg| Arc::new(Field::new("input", arg.data_type().clone(), false)))
                .collect();
            let dtype = DataType::Timestamp(TimeUnit::Microsecond, None);
            let output = ConvertTz::new(false)
                .invoke_with_args(ScalarFunctionArgs {
                    args: arguments.into_iter().map(ColumnarValue::Array).collect(),
                    arg_fields: fields,
                    number_rows: cases.len(),
                    return_field: Arc::new(Field::new("result", dtype.clone(), true)),
                    config_options: Arc::clone(session.state().config_options()),
                })?
                .into_array(cases.len())?;
            assert_eq!(output.data_type(), &dtype);
            let values = output.as_primitive::<TimestampMicrosecondType>();
            assert_eq!(values.len(), cases.len());
            for (value, case) in values.iter().zip(cases) {
                assert_eq!(value, case["result"]["micros"].as_i64(), "{case}");
            }
            continue;
        }

        let (expression, times, expected_type): (&str, ArrayRef, &str) = match route {
            "classic" => (
                "convert_timezone(source_zone,target_zone,ts)",
                Arc::new(times),
                "Timestamp(Microsecond, None)",
            ),
            "from_utc" => (
                "from_utc_timestamp(ts,target_zone)",
                Arc::new(times.with_timezone("UTC")),
                "Timestamp(Microsecond, Some(\"UTC\"))",
            ),
            _ => unreachable!("unexpected route {route}"),
        };
        session.register_batch(
            "calendar_boundary_inputs",
            RecordBatch::try_from_iter([
                ("id", id),
                ("source_zone", from),
                ("target_zone", to),
                ("ts", times),
            ])?,
        )?;
        let sql = format!("SELECT id,{expression} FROM calendar_boundary_inputs ORDER BY id");
        let options = SparkSqlOptions {
            ansi_mode,
            ..SparkSqlOptions::default()
        };
        let outcome = execute_conversion(&session, &sql, &options).await;
        session.deregister_table("calendar_boundary_inputs")?;
        if status == "error" {
            let error = outcome.unwrap_err();
            assert!(
                error.to_string().contains("[INVALID_TIMEZONE]"),
                "{cases:?}: {error}"
            );
            continue;
        }
        let observed = outcome?;
        assert_eq!(observed["types"], json!(["Int64", expected_type]));
        let rows = observed["rows"].as_array().unwrap();
        assert_eq!(rows.len(), cases.len());
        for (row, case) in rows.iter().zip(cases) {
            assert_eq!(
                row,
                &json!([case["id"], case["result"]["micros"]]),
                "{case}"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn time_zone_sql_callers_recover_the_recorded_panics() -> Result<()> {
    let spark = boundary_records(include_str!(
        "fixtures/time-zone-range-1513/spark-public.jsonl"
    ))?;
    let dar = boundary_records(include_str!(
        "fixtures/time-zone-range-1513/dar-public.jsonl"
    ))?;
    assert_eq!(spark[0]["spark_version"], "4.2.0");
    assert_eq!(dar.len(), 56);
    assert_eq!(spark.len(), dar.len() + 1);
    let session = conversion_session()?;
    for (expected, before) in spark[1..].iter().zip(dar) {
        for key in ["id", "ansi", "input_micros", "session", "sql"] {
            assert_eq!(expected[key], before[key], "{key}");
        }
        let micros = expected["input_micros"].as_i64().unwrap();
        let ntz = TimestampMicrosecondArray::from(vec![micros]);
        let ltz: ArrayRef = Arc::new(ntz.clone().with_timezone("UTC"));
        let date: ArrayRef = Arc::new(Date32Array::from(vec![
            i32::try_from(micros.div_euclid(86_400_000_000)).unwrap(),
        ]));
        let time: ArrayRef = Arc::new(Time64MicrosecondArray::from(vec![
            micros.rem_euclid(86_400_000_000),
        ]));
        session.register_batch(
            "probe",
            RecordBatch::try_from_iter([
                ("ntz", Arc::new(ntz) as ArrayRef),
                ("ltz", ltz),
                ("d", date),
                ("tod", time),
            ])?,
        )?;
        let zone = expected["session"].as_str().unwrap();
        let options = SparkSqlOptions {
            ansi_mode: expected["ansi"].as_bool().unwrap(),
            session_timezone: zone.into(),
            ..SparkSqlOptions::default()
        };
        let sql = format!(
            "SELECT {} AS result FROM probe",
            expected["sql"].as_str().unwrap()
        );
        let observed = execute_conversion(&session, &sql, &options).await;
        session.deregister_table("probe")?;
        if before["exit_code"] == 2 {
            let original_error = before["outcome"]
                .as_str()
                .unwrap()
                .strip_prefix("error ")
                .unwrap();
            assert_eq!(
                observed.unwrap_err().to_string(),
                original_error,
                "{expected}"
            );
        } else if expected["result"]["status"] == "error" {
            assert!(observed.is_err(), "{expected}");
        } else {
            let dtype = match expected["result"]["type"].as_str().unwrap() {
                "timestamp_ntz" => "Timestamp(Microsecond, None)".to_owned(),
                "timestamp" => format!("Timestamp(Microsecond, Some({zone:?}))"),
                _ => unreachable!("unexpected Spark type"),
            };
            let observed = observed?;
            assert_eq!(observed["types"], json!([dtype]), "{expected}");
            assert_eq!(
                observed["rows"],
                json!([[expected["result"]["micros"]]]),
                "{expected}"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn fixed_session_zone_keeps_the_intermediate_timestamp_unit_check() -> Result<()> {
    let session = conversion_session()?;
    session.register_batch(
        "nanosecond_input",
        RecordBatch::try_from_iter([(
            "value",
            Arc::new(arrow::array::TimestampNanosecondArray::from(vec![i64::MAX])) as ArrayRef,
        )])?,
    )?;
    for ansi_mode in [false, true] {
        let options = SparkSqlOptions {
            ansi_mode,
            session_timezone: "-01:00".into(),
            ..SparkSqlOptions::default()
        };
        let sql = "SELECT CAST(from_utc_timestamp(value,'-01:00') AS BIGINT) FROM nanosecond_input";
        let error = execute_conversion(&session, sql, &options)
            .await
            .unwrap_err();
        assert!(
            error.to_string().to_ascii_lowercase().contains("overflow"),
            "{error}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn time_zone_boundary_repairs_keep_ordinary_and_null_rows() -> Result<()> {
    let spark = boundary_records(include_str!(
        "fixtures/time-zone-range-1513/spark-public.jsonl"
    ))?;
    let session = conversion_session()?;
    for (case_id, ordinary_micros) in [
        ("from_positive_at_max", 64_800_000_000_i64),
        ("to_ntz_underflow", -64_800_000_000),
        ("make_named_session_at_min", -3_600_000_000),
    ] {
        for ansi_mode in [false, true] {
            let case = spark
                .iter()
                .find(|row| row["id"] == case_id && row["ansi"] == ansi_mode)
                .unwrap();
            let edge = case["input_micros"].as_i64().unwrap();
            let values = [Some(0), Some(edge), None, Some(1)];
            let ntz = TimestampMicrosecondArray::from(values.to_vec());
            let ltz: ArrayRef = Arc::new(ntz.clone().with_timezone("UTC"));
            let dates = Date32Array::from(
                values
                    .map(|tick| {
                        tick.map(|tick| i32::try_from(tick.div_euclid(86_400_000_000)).unwrap())
                    })
                    .to_vec(),
            );
            let times = Time64MicrosecondArray::from(
                values
                    .map(|tick| tick.map(|tick| tick.rem_euclid(86_400_000_000)))
                    .to_vec(),
            );
            session.register_batch(
                "mixed_boundary_inputs",
                RecordBatch::try_from_iter([
                    (
                        "id",
                        Arc::new(Int64Array::from(vec![0, 1, 2, 3])) as ArrayRef,
                    ),
                    ("ntz", Arc::new(ntz) as ArrayRef),
                    ("ltz", ltz),
                    ("d", Arc::new(dates) as ArrayRef),
                    ("tod", Arc::new(times) as ArrayRef),
                ])?,
            )?;
            let zone = case["session"].as_str().unwrap();
            let options = SparkSqlOptions {
                ansi_mode,
                session_timezone: zone.into(),
                ..SparkSqlOptions::default()
            };
            let sql = format!(
                "SELECT id,{} AS result FROM mixed_boundary_inputs ORDER BY id",
                case["sql"].as_str().unwrap()
            );
            let observed = execute_conversion(&session, &sql, &options).await?;
            session.deregister_table("mixed_boundary_inputs")?;
            assert_eq!(
                observed["types"],
                json!(["Int64", format!("Timestamp(Microsecond, Some({zone:?}))")])
            );
            assert_eq!(
                observed["rows"],
                json!([
                    [0, ordinary_micros],
                    [1, case["result"]["micros"]],
                    [2, Value::Null],
                    [3, ordinary_micros + 1],
                ]),
                "{case_id} ANSI={ansi_mode}"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn literal_zone_identities_preserve_conversion_results() -> Result<()> {
    let session = conversion_session()?;
    for ansi_mode in [false, true] {
        for session_zone in ["UTC", "America/Los_Angeles"] {
            let options = SparkSqlOptions {
                ansi_mode,
                session_timezone: session_zone.into(),
                ..SparkSqlOptions::default()
            };
            for (function, timestamp, zone) in [
                ("convert_timezone", "ntz", "UTC"),
                ("convert_timezone", "ntz", "Z"),
                ("convert_timezone", "CAST(ltz AS TIMESTAMP_NTZ)", "UTC"),
                ("from_utc_timestamp", "ltz", "UTC"),
                ("to_utc_timestamp", "ltz", "UTC"),
                ("convert_timezone", "ntz", "America/Los_Angeles"),
            ] {
                let query = |zone: &str| {
                    let call = if function == "convert_timezone" {
                        format!("{function}({zone}, {zone}, {timestamp})")
                    } else {
                        format!("{function}({timestamp}, {zone})")
                    };
                    format!(
                        "SELECT id, {call} AS converted FROM observations WHERE id <= 5 ORDER BY id"
                    )
                };
                let sql = query(&format!("'{zone}'"));
                let runtime_zone = query(&format!(
                    "CASE WHEN id >= 0 THEN '{zone}' ELSE zone_name END"
                ));
                let resolved = plan_query(&session, &sql, &options).await?;
                let plan = resolved.logical_plan.display_indent().to_string();
                assert!(plan.contains("convert_tz"), "{sql}\n{plan}");
                assert_eq!(
                    execute_conversion(&session, &sql, &options).await?,
                    execute_conversion(&session, &runtime_zone, &options).await?,
                    "{sql}"
                );
            }
            let empty_input = "SELECT convert_timezone('UTC','UTC', ts) AS converted FROM \
                (SELECT CAST(input_text AS TIMESTAMP_NTZ) AS ts FROM \
                (SELECT 'not a timestamp' AS input_text FROM observations WHERE id < 0))";
            assert_eq!(
                execute_conversion(&session, empty_input, &options).await?,
                json!({"types":["Timestamp(Microsecond, None)"],"rows":[]})
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn literal_zone_identity_keeps_distinct_grouping_expressions() -> Result<()> {
    let session = conversion_session()?;
    for ansi_mode in [false, true] {
        let options = SparkSqlOptions {
            ansi_mode,
            ..SparkSqlOptions::default()
        };
        let sql = "SELECT grouping(ntz) FROM observations \
                   GROUP BY ROLLUP(convert_timezone('UTC','UTC',ntz))";
        let error = plan_query(&session, sql, &options)
            .await
            .err()
            .ok_or_else(|| {
                DataFusionError::Internal("a different expression matched the grouping key".into())
            })?;
        assert!(error.to_string().contains("GROUPING_COLUMN_MISMATCH"));
    }
    Ok(())
}

#[tokio::test]
async fn literal_zone_conversions_keep_nanosecond_rounding() -> Result<()> {
    let session = conversion_session()?;
    let times = TimestampNanosecondArray::from(vec![Some(1_501), Some(-1_501), None]);
    let ntz: ArrayRef = Arc::new(times.clone());
    let ltz: ArrayRef = Arc::new(times.with_timezone("UTC"));
    session.register_batch(
        "nanos",
        RecordBatch::try_from_iter([("ntz", ntz), ("ltz", ltz)])?,
    )?;
    for ansi_mode in [false, true] {
        let options = SparkSqlOptions {
            ansi_mode,
            ..SparkSqlOptions::default()
        };
        let result = execute_conversion(
            &session,
            "SELECT convert_timezone('UTC','UTC',ntz) AS local_time, \
             from_utc_timestamp(ltz,'UTC') AS from_utc, \
             to_utc_timestamp(ltz,'UTC') AS to_utc FROM nanos",
            &options,
        )
        .await?;
        assert_eq!(
            result,
            json!({
                "types": [
                    "Timestamp(Nanosecond, None)",
                    "Timestamp(Nanosecond, Some(\"UTC\"))",
                    "Timestamp(Nanosecond, Some(\"UTC\"))"
                ],
                "rows": [[1000, 1000, 1000], [-1000, -1000, -1000], [null, null, null]]
            })
        );
    }
    Ok(())
}
