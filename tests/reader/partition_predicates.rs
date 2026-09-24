//! Partition predicates must be evaluated with Delta metadata, including SQL NULL semantics.

use std::{error::Error, fs};

use arrow::{array::Int32Array, record_batch::RecordBatch};
use delta_arrow_reader::{
    DeltaComparison as C, DeltaPredicate as P, DeltaScalar as S, DeltaScanExecutionOptions,
    DeltaScanMetricsSnapshot, DeltaTable, DeltaTableBuilder, ParquetReaderBackend, WarmupMode,
};
use futures_util::TryStreamExt;
use serde_json::{Value, json};

use super::support::RealParquetDeltaTable;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

fn compare(column: &str, op: C, value: S) -> P {
    P::Compare {
        column: column.into(),
        op,
        value,
    }
}

fn west() -> P {
    compare("region", C::Eq, S::Utf8("us-west".into()))
}

fn data_match() -> P {
    compare("customer_name", C::Eq, S::Utf8("z".into()))
}

fn not(predicate: P) -> P {
    P::Not(Box::new(predicate))
}

fn ids(batches: &[RecordBatch]) -> TestResult<Vec<i32>> {
    let mut values = Vec::new();
    for batch in batches {
        let column = batch.column_by_name("id").ok_or("missing id")?;
        let column = column
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or("expected Int32 id")?;
        values.extend(column.values());
    }
    Ok(values)
}

async fn read(
    table: &DeltaTable,
    predicate: P,
    projection: &[&str],
    backend: ParquetReaderBackend,
    limit: Option<usize>,
) -> TestResult<(Vec<RecordBatch>, DeltaScanMetricsSnapshot)> {
    let mut builder = table
        .scan()
        .with_predicate(predicate)
        .with_projection(projection.iter().copied())
        .with_target_partitions(2)?
        .with_execution_options(DeltaScanExecutionOptions::new().with_parquet_backend(backend));
    if let Some(limit) = limit {
        builder = builder.with_limit(limit);
    }
    let scan = builder.build().await?;
    let schema = scan.schema();
    assert_eq!(
        schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>(),
        projection
    );
    let stream = scan.into_stream();
    let metrics = stream.metrics();
    let batches = stream.try_collect::<Vec<_>>().await?;
    assert!(batches.iter().all(|batch| batch.schema() == schema));
    Ok((batches, metrics.snapshot()))
}

// Each expected set is the SQL WHERE truth table, independent of either reader backend.
// ids 1..=3 have NULL region, 4..=6 west, 7..=9 east. Within each file the
// customer values are NULL, 'a', 'z', so partition and data truth values vary independently.
fn boolean_cases() -> Vec<(&'static str, P, Vec<i32>)> {
    let p = west();
    let d = data_match();
    let high = compare("id", C::GtEq, S::Int32(5));
    vec![
        ("partition", p.clone(), vec![4, 5, 6]),
        ("data", d.clone(), vec![3, 6, 9]),
        ("and", P::And(vec![p.clone(), d.clone()]), vec![6]),
        ("and reversed", P::And(vec![d.clone(), p.clone()]), vec![6]),
        ("or", P::Or(vec![p.clone(), d.clone()]), vec![3, 4, 5, 6, 9]),
        (
            "or reversed",
            P::Or(vec![d.clone(), p.clone()]),
            vec![3, 4, 5, 6, 9],
        ),
        ("not partition", not(p.clone()), vec![7, 8, 9]),
        (
            "not and",
            not(P::And(vec![p.clone(), d.clone()])),
            vec![2, 5, 7, 8, 9],
        ),
        ("not or", not(P::Or(vec![p.clone(), d.clone()])), vec![8]),
        (
            "double not",
            not(not(P::And(vec![p.clone(), d.clone()]))),
            vec![6],
        ),
        (
            "and not data",
            P::And(vec![p.clone(), not(d.clone())]),
            vec![5],
        ),
        (
            "or not data",
            P::Or(vec![p.clone(), not(d.clone())]),
            vec![2, 4, 5, 6, 8],
        ),
        (
            "not partition or data",
            P::Or(vec![not(p.clone()), d.clone()]),
            vec![3, 6, 7, 8, 9],
        ),
        (
            "partial child under or",
            P::Or(vec![
                P::And(vec![p.clone(), d.clone()]),
                compare("id", C::Eq, S::Int32(2)),
            ]),
            vec![2, 6],
        ),
        (
            "partial child under not",
            not(P::And(vec![p.clone(), high.clone()])),
            vec![1, 2, 3, 4, 7, 8, 9],
        ),
        (
            "safe sibling of mixed or",
            P::And(vec![P::Or(vec![p.clone(), d.clone()]), high.clone()]),
            vec![5, 6, 9],
        ),
        (
            "nested data or",
            P::And(vec![
                p.clone(),
                P::Or(vec![d.clone(), compare("id", C::Eq, S::Int32(5))]),
            ]),
            vec![5, 6],
        ),
        (
            "nested and",
            P::And(vec![p.clone(), P::And(vec![d.clone(), high])]),
            vec![6],
        ),
        ("empty and", P::And(vec![]), (1..=9).collect()),
        ("empty or", P::Or(vec![]), vec![]),
        (
            "partition and false",
            P::And(vec![p.clone(), P::Constant(false)]),
            vec![],
        ),
        (
            "mixed and false",
            P::And(vec![p.clone(), d.clone(), P::Constant(false)]),
            vec![],
        ),
        (
            "partition and constant subtree",
            P::And(vec![p.clone(), not(P::Or(vec![]))]),
            vec![4, 5, 6],
        ),
        (
            "true and partition",
            P::And(vec![P::Constant(true), p.clone()]),
            vec![4, 5, 6],
        ),
        (
            "false or partition",
            P::Or(vec![P::Constant(false), p.clone()]),
            vec![4, 5, 6],
        ),
        (
            "partition contradiction",
            P::And(vec![p.clone(), not(p.clone())]),
            vec![],
        ),
        (
            "nullable excluded middle",
            P::Or(vec![p.clone(), not(p)]),
            vec![4, 5, 6, 7, 8, 9],
        ),
    ]
}

#[tokio::test]
async fn partition_predicates_preserve_mixed_boolean_truth_tables() -> TestResult {
    let fixture = RealParquetDeltaTable::new_with_partition_truth_table("partition-boolean")?;
    for warmup in [WarmupMode::None, WarmupMode::QueryPlanning] {
        let table = DeltaTableBuilder::new(fixture.path().to_string_lossy())
            .with_warmup(warmup)
            .load_table()
            .await?;
        for backend in [
            ParquetReaderBackend::Direct,
            ParquetReaderBackend::DeltaKernel,
        ] {
            for (name, predicate, expected) in boolean_cases() {
                // Both predicate columns are hidden from the visible output.
                let (batches, _) = read(&table, predicate, &["id"], backend, None)
                    .await
                    .map_err(|e| format!("{name}, {backend:?}, {warmup:?}: {e}"))?;
                let mut actual = ids(&batches)?;
                actual.sort_unstable();
                assert_eq!(actual, expected, "{name}, {backend:?}, {warmup:?}");
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn partition_predicates_keep_file_pruning_and_data_row_filtering() -> TestResult {
    let fixture = RealParquetDeltaTable::new_with_partition_truth_table("partition-pruning")?;
    let table = DeltaTableBuilder::new(fixture.path().to_string_lossy())
        .load_table()
        .await?;
    let cases = [
        (west(), vec![4, 5, 6], 1, 3),
        (
            compare("region", C::Eq, S::Utf8("missing".into())),
            vec![],
            0,
            0,
        ),
        (
            P::IsNull {
                column: "region".into(),
            },
            vec![1, 2, 3],
            1,
            3,
        ),
        (
            P::IsNotNull {
                column: "region".into(),
            },
            vec![4, 5, 6, 7, 8, 9],
            2,
            6,
        ),
        (P::And(vec![west(), data_match()]), vec![6], 1, 1),
        (data_match(), vec![3, 6, 9], 3, 3),
    ];
    for (predicate, expected, files, emitted) in cases {
        let (batches, metrics) = read(
            &table,
            predicate,
            &["id"],
            ParquetReaderBackend::Direct,
            None,
        )
        .await?;
        let mut actual = ids(&batches)?;
        actual.sort_unstable();
        assert_eq!(actual, expected);
        assert_eq!(metrics.files_planned, files);
        assert_eq!(metrics.file_tasks_started, files);
        assert_eq!(metrics.scheduler_rows_emitted, emitted);
        if files == 0 {
            assert_eq!(metrics.parquet_data_file_range_get_operations, Some(0));
            assert_eq!(metrics.parquet_data_file_full_get_operations, Some(0));
        }
    }
    Ok(())
}

// Add a metadata-only partition column to an existing real-Parquet fixture. The caller
// supplies logical/physical identities explicitly, including the mapped-name collision case.
fn add_partition(
    fixture: &RealParquetDeltaTable,
    name: &str,
    data_type: Value,
    value: Option<&str>,
    physical_name: Option<&str>,
) -> TestResult {
    for version in [0, 1] {
        let log = fixture
            .path()
            .join(format!("_delta_log/{version:020}.json"));
        let mut actions = fs::read_to_string(&log)?
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<Result<Vec<_>, _>>()?;
        for action in &mut actions {
            if data_type == "timestamp_ntz" && action.get("protocol").is_some() {
                action["protocol"] = json!({
                    "minReaderVersion":3,"minWriterVersion":7,
                    "readerFeatures":["timestampNtz"],"writerFeatures":["timestampNtz"]
                });
            }
            if let Some(metadata) = action.get_mut("metaData") {
                let mut schema: Value =
                    serde_json::from_str(metadata["schemaString"].as_str().ok_or("schema")?)?;
                let field_metadata = physical_name.map_or(json!({}), |physical| {
                    json!({
                        "delta.columnMapping.id": 3, "delta.columnMapping.physicalName": physical
                    })
                });
                schema["fields"]
                    .as_array_mut()
                    .ok_or("fields")?
                    .push(json!({
                        "name":name,"type":data_type,"nullable":true,"metadata":field_metadata
                    }));
                metadata["schemaString"] = json!(schema.to_string());
                metadata["partitionColumns"] = json!([name]);
                if physical_name.is_some() {
                    metadata["configuration"]["delta.columnMapping.maxColumnId"] = json!("3");
                }
            }
            if let Some(add) = action.get_mut("add") {
                add["partitionValues"] = json!({physical_name.unwrap_or(name):value});
            }
        }
        fs::write(
            log,
            actions
                .iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join("\n")
                + "\n",
        )?;
    }
    Ok(())
}

#[tokio::test]
async fn partition_predicates_use_logical_identity_before_column_mapping() -> TestResult {
    // A partition's logical name equals a data column's physical name. Its physical
    // name also equals the data column's logical name. Classifying after renaming is wrong.
    let fixture = RealParquetDeltaTable::new_with_column_mapping("partition-mapping")?;
    add_partition(
        &fixture,
        "phys_id",
        json!("string"),
        Some("west"),
        Some("id"),
    )?;
    let table = DeltaTableBuilder::new(fixture.path().to_string_lossy())
        .load_table()
        .await?;
    let partition = compare("phys_id", C::Eq, S::Utf8("west".into()));
    for backend in [
        ParquetReaderBackend::Direct,
        ParquetReaderBackend::DeltaKernel,
    ] {
        for (predicate, expected) in [
            (partition.clone(), vec![1, 2, 3]),
            (
                P::And(vec![partition.clone(), compare("id", C::Gt, S::Int32(1))]),
                vec![2, 3],
            ),
            (
                P::Or(vec![partition.clone(), compare("id", C::Eq, S::Int32(2))]),
                vec![1, 2, 3],
            ),
        ] {
            let (batches, _) =
                read(&table, predicate, &["customer_name", "id"], backend, None).await?;
            assert_eq!(ids(&batches)?, expected);
        }
    }
    Ok(())
}

#[tokio::test]
async fn partition_predicates_preserve_typed_partition_values() -> TestResult {
    let cases = [
        ("boolean", "true", S::Boolean(true)),
        ("byte", "12", S::Int8(12)),
        ("short", "123", S::Int16(123)),
        ("integer", "1234", S::Int32(1234)),
        ("long", "12345", S::Int64(12345)),
        ("float", "1.5", S::Float32(1.5)),
        ("double", "2.5", S::Float64(2.5)),
        ("string", "west", S::Utf8("west".into())),
        ("binary", "\u{0001}\u{0002}", S::Binary(vec![1, 2])),
        ("date", "1970-01-02", S::Date32(1)),
        (
            "decimal(10,2)",
            "12.34",
            S::Decimal128 {
                value: 1234,
                precision: 10,
                scale: 2,
            },
        ),
        (
            "timestamp",
            "1970-01-01 00:00:01",
            S::TimestampMicrosecond {
                value: 1_000_000,
                timezone: Some("UTC".into()),
            },
        ),
        (
            "timestamp_ntz",
            "1970-01-01 00:00:01",
            S::TimestampMicrosecond {
                value: 1_000_000,
                timezone: None,
            },
        ),
    ];
    for (data_type, text, scalar) in cases {
        // Delta's partition-value serialization treats an empty string as NULL for
        // every type: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#partition-value-serialization
        for value in [Some(text), Some(""), None] {
            let has_value = value.is_some_and(|text| !text.is_empty());
            let fixture = RealParquetDeltaTable::new_default("partition-typed")?;
            add_partition(&fixture, "part", json!(data_type), value, None)?;
            let table = DeltaTableBuilder::new(fixture.path().to_string_lossy())
                .load_table()
                .await?;
            for op in [C::Eq, C::NotEq, C::Lt, C::LtEq, C::Gt, C::GtEq] {
                let predicate = compare("part", op, scalar.clone());
                let (batches, _) = read(
                    &table,
                    predicate,
                    &["id", "part"],
                    ParquetReaderBackend::Direct,
                    None,
                )
                .await
                .map_err(|e| format!("{data_type}, {value:?}, {op:?}: {e}"))?;
                let expected = if has_value && matches!(op, C::Eq | C::LtEq | C::GtEq) {
                    vec![1, 2, 3]
                } else {
                    vec![]
                };
                assert_eq!(ids(&batches)?, expected, "{data_type}, {value:?}, {op:?}");
            }
            let (batches, _) = read(
                &table,
                P::IsNull {
                    column: "part".into(),
                },
                &["id"],
                ParquetReaderBackend::Direct,
                None,
            )
            .await?;
            assert_eq!(
                ids(&batches)?,
                if !has_value { vec![1, 2, 3] } else { vec![] }
            );
            let mixed = P::Or(vec![
                compare("part", C::Eq, scalar.clone()),
                compare("id", C::Eq, S::Int32(2)),
            ]);
            let (batches, _) =
                read(&table, mixed, &["id"], ParquetReaderBackend::Direct, None).await?;
            assert_eq!(
                ids(&batches)?,
                if has_value { vec![1, 2, 3] } else { vec![2] }
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn partition_predicates_preserve_dv_projection_and_limit_order() -> TestResult {
    let fixture = RealParquetDeltaTable::new_with_rows_and_deletion_vector(
        "partition-dv",
        12,
        &[0, 3, 4, 8, 11],
    )?;
    add_partition(&fixture, "region", json!("string"), Some("us-west"), None)?;
    let table = DeltaTableBuilder::new(fixture.path().to_string_lossy())
        .load_table()
        .await?;
    let predicate = P::And(vec![west(), compare("id", C::GtEq, S::Int32(5))]);
    let expected = [6, 7, 8, 10, 11];
    for backend in [
        ParquetReaderBackend::Direct,
        ParquetReaderBackend::DeltaKernel,
    ] {
        for projection in [vec!["id"], vec!["region"], vec![]] {
            for limit in [None, Some(0), Some(1), Some(3), Some(30)] {
                let (batches, metrics) =
                    read(&table, predicate.clone(), &projection, backend, limit).await?;
                let rows = expected.len().min(limit.unwrap_or(usize::MAX));
                assert_eq!(
                    batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
                    rows
                );
                if projection == ["id"] {
                    assert_eq!(ids(&batches)?, expected[..rows]);
                }
                if limit == Some(0) {
                    assert_eq!(metrics.file_tasks_started, 0);
                }
            }
        }
    }
    Ok(())
}

#[cfg(feature = "datafusion")]
#[tokio::test]
async fn partition_predicates_datafusion_keeps_mixed_and_null_results() -> TestResult {
    use datafusion::prelude::SessionContext;
    use delta_arrow_reader::datafusion::{ScanOptions, register_table};

    let fixture = RealParquetDeltaTable::new_with_partition_truth_table("partition-datafusion")?;
    for backend in [
        ParquetReaderBackend::Direct,
        ParquetReaderBackend::DeltaKernel,
    ] {
        let table = DeltaTableBuilder::new(fixture.path().to_string_lossy())
            .load_table()
            .await?;
        let context = SessionContext::new();
        register_table(
            &context,
            "t",
            table,
            ScanOptions {
                execution_options: DeltaScanExecutionOptions::new().with_parquet_backend(backend),
                ..ScanOptions::default()
            },
        )?;
        for (condition, expected) in [
            ("region = 'us-west'", vec![4, 5, 6]),
            ("region IS NULL", vec![1, 2, 3]),
            ("region = 'us-west' AND customer_name = 'z'", vec![6]),
            (
                "region = 'us-west' OR customer_name = 'z'",
                vec![3, 4, 5, 6, 9],
            ),
            (
                "NOT (region = 'us-west' AND customer_name = 'z')",
                vec![2, 5, 7, 8, 9],
            ),
            ("NOT (region = 'us-west' OR customer_name = 'z')", vec![8]),
        ] {
            let batches = context
                .sql(&format!("SELECT id FROM t WHERE {condition} ORDER BY id"))
                .await?
                .collect()
                .await?;
            assert_eq!(ids(&batches)?, expected, "{backend:?}, {condition}");
        }
    }
    Ok(())
}

#[tokio::test]
async fn partition_predicates_filter_partition_only_and_empty_projections() -> TestResult {
    for deleted in [&[][..], &[0, 2][..], &[0, 1, 2][..]] {
        let fixture = RealParquetDeltaTable::new_with_partition_value_and_deletion_vector(
            "partition-only-output",
            "us-west",
            deleted,
        )?;
        let table = DeltaTableBuilder::new(fixture.path().to_string_lossy())
            .load_table()
            .await?;
        for projection in [vec![], vec!["region"]] {
            let (batches, _) = read(
                &table,
                west(),
                &projection,
                ParquetReaderBackend::Direct,
                None,
            )
            .await?;
            assert_eq!(
                batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
                3 - deleted.len()
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn partition_predicates_keep_unconvertible_data_terms_in_the_residual() -> TestResult {
    let fixture = RealParquetDeltaTable::new_with_supported_types("partition-residual")?;
    add_partition(&fixture, "region", json!("string"), Some("us-west"), None)?;
    let table = DeltaTableBuilder::new(fixture.path().to_string_lossy())
        .load_table()
        .await?;
    // A comparison to floating zero cannot be converted to Kernel without losing
    // Arrow's signed-zero semantics. The fixture's scores are 10.5, -20.25, NULL.
    let zero = compare("score_f64", C::Eq, S::Float64(0.0));
    let high = compare("id", C::GtEq, S::Int32(2));
    for backend in [
        ParquetReaderBackend::Direct,
        ParquetReaderBackend::DeltaKernel,
    ] {
        for (predicate, expected) in [
            (
                P::And(vec![west(), high.clone(), not(zero.clone())]),
                vec![2],
            ),
            (
                P::Or(vec![P::And(vec![west(), high.clone()]), zero.clone()]),
                vec![2, 3],
            ),
            (
                not(P::And(vec![west(), high.clone(), zero.clone()])),
                vec![1, 2],
            ),
        ] {
            let (batches, _) = read(&table, predicate, &["id"], backend, None).await?;
            assert_eq!(ids(&batches)?, expected);
        }
    }
    Ok(())
}

#[tokio::test]
async fn partition_predicates_support_multiple_partition_columns() -> TestResult {
    let fixture = RealParquetDeltaTable::new_with_two_partition_columns("partition-multiple")?;
    let table = DeltaTableBuilder::new(fixture.path().to_string_lossy())
        .load_table()
        .await?;
    let date = compare("event_date", C::Eq, S::Date32(20_454)); // 2026-01-01
    for backend in [
        ParquetReaderBackend::Direct,
        ParquetReaderBackend::DeltaKernel,
    ] {
        for (predicate, expected) in [
            (P::And(vec![west(), date.clone()]), vec![1]),
            (P::Or(vec![west(), date.clone()]), vec![1, 2, 3]),
            (not(P::Or(vec![west(), date.clone()])), vec![4]),
            (
                P::And(vec![
                    P::Or(vec![west(), date.clone()]),
                    compare("id", C::Gt, S::Int32(1)),
                ]),
                vec![2, 3],
            ),
        ] {
            let (batches, _) = read(&table, predicate, &["id"], backend, None).await?;
            let mut actual = ids(&batches)?;
            actual.sort_unstable();
            assert_eq!(actual, expected);
        }
    }
    Ok(())
}

/// Run optimized, with --ignored --nocapture --test-threads=1. Freeze this test binary
/// before changing production code for matched before/after runs. Every emitted JSON row
/// includes planning/read timings and I/O counters; failures have no comparable read time.
#[tokio::test]
#[ignore = "manual before/after partition-filter performance measurement"]
async fn partition_predicates_benchmark() -> TestResult {
    use std::time::Instant;

    let samples: usize = std::env::var("PARTITION_BENCH_SAMPLES")
        .unwrap_or_else(|_| "24".into())
        .parse()?;
    let rows: usize = std::env::var("PARTITION_BENCH_ROWS")
        .unwrap_or_else(|_| "131072".into())
        .parse()?;
    if !(1..=1000).contains(&samples) || rows < 16 {
        return Err("samples must be 1..=1000 and rows at least 16".into());
    }
    let fixture = RealParquetDeltaTable::new_with_two_large_files("partition-bench", rows)?;
    add_partition(&fixture, "region", json!("string"), Some("us-west"), None)?;
    let total = i32::try_from(rows.checked_mul(2).ok_or("row count overflow")?)?;
    let selective = compare("id", C::Gt, S::Int32(total - 16));
    let cases = [
        ("unfiltered", None, 1),
        ("data", Some(selective.clone()), total - 15),
        ("partition", Some(west()), 1),
        (
            "and",
            Some(P::And(vec![west(), selective.clone()])),
            total - 15,
        ),
        ("or", Some(P::Or(vec![west(), selective])), 1),
    ];
    let selected_case = std::env::var("PARTITION_BENCH_CASE").ok();
    if let Some(selected) = &selected_case
        && !cases.iter().any(|(name, _, _)| name == selected)
    {
        return Err("unknown PARTITION_BENCH_CASE".into());
    }
    let table = DeltaTableBuilder::new(fixture.path().to_string_lossy())
        .with_warmup(WarmupMode::QueryPlanning)
        .load_table()
        .await?;
    for backend in [
        ParquetReaderBackend::Direct,
        ParquetReaderBackend::DeltaKernel,
    ] {
        // Alternate case order each round so persistent first/last-case effects are visible.
        for sample in 0..samples + 4 {
            let mut order = (0..cases.len()).collect::<Vec<_>>();
            if sample % 2 == 1 {
                order.reverse();
            }
            for index in order {
                let (name, predicate, first) = &cases[index];
                if selected_case
                    .as_deref()
                    .is_some_and(|selected| selected != *name)
                {
                    continue;
                }
                let options = DeltaScanExecutionOptions::new().with_parquet_backend(backend);
                let start = Instant::now();
                let mut builder = table
                    .scan()
                    .with_projection(["id", "customer_name"])
                    .with_execution_options(options)
                    .with_target_partitions(2)?;
                if let Some(predicate) = predicate {
                    builder = builder.with_predicate(predicate.clone());
                }
                let scan = builder.build().await?;
                let planning_ns = start.elapsed().as_nanos();
                let stream = scan.into_stream();
                let metrics = stream.metrics();
                let start = Instant::now();
                let result = stream.try_collect::<Vec<_>>().await;
                let read_ns = start.elapsed().as_nanos();
                let status = match result {
                    Ok(batches) => {
                        let mut actual = ids(&batches)?;
                        actual.sort_unstable();
                        assert_eq!(actual, (*first..=total).collect::<Vec<_>>());
                        "ok".to_owned()
                    }
                    Err(error) => format!("error: {error}"),
                };
                let metrics = metrics.snapshot();
                if sample >= 4 {
                    println!(
                        "{}",
                        json!({
                            "case":name,"backend":format!("{backend:?}"),"sample":sample-4,
                            "rows_per_file":rows,"status":status,"planning_ns":planning_ns,
                            "read_ns":if status == "ok" {Some(read_ns)} else {None},
                            "files":metrics.file_tasks_started,"emitted":metrics.scheduler_rows_emitted,
                            "range_gets":metrics.parquet_data_file_range_get_operations,
                            "full_gets":metrics.parquet_data_file_full_get_operations,
                            "bytes":metrics.parquet_data_file_bytes_received,
                        })
                    );
                }
            }
        }
    }
    Ok(())
}
