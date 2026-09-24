//! Typed footer regression matrix for issue #118.
//!
//! The oracle casts actual Arrow values, then evaluates comparisons on those values.
//! It does not call the statistics conversion under test. Singleton row groups must
//! be pruned exactly; multi-row groups must retain every matching row. This catches
//! both wrong results and a fix that simply disables pruning for widened types.
//!
//! Coverage: scalar widening families accepted by schema alignment; decimal precision and
//! scale changes; all timestamp units/timezone directions; pre-epoch timestamps;
//! null-only groups; all six comparisons; exact boundaries; absent statistics.
//! NaN ordering and INT96 value decoding belong to issues #120 and #119.

#![allow(clippy::unwrap_used)]

use std::{error::Error, fs, sync::Arc};

use arrow::{
    array::*,
    compute::{cast, kernels::cmp},
    datatypes::{DataType as ArrowType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use bytes::Bytes;
use delta_kernel::{
    engine::arrow_conversion::scalar::extract_primitive_scalar,
    expressions::{ColumnName, Expression, Predicate, Scalar as KernelScalar},
};
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    data_type::{ByteArray, FixedLenByteArray},
    file::{
        metadata::{ColumnChunkMetaData, FileMetaData, ParquetMetaData, RowGroupMetaData},
        properties::{EnabledStatistics, WriterProperties},
        statistics::Statistics,
    },
    schema::{parser::parse_message_type, types::SchemaDescriptor},
};

use futures_util::TryStreamExt;
use serde_json::{Value, json};

use super::super::tests::TestDir;
use crate::{
    DeltaComparison, DeltaPredicate, DeltaScalar, DeltaTable, DeltaTableBuilder,
    delta::kernel::DeltaKernelPredicate,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

fn file_schema(metadata: &ParquetMetaData) -> TestResult<Arc<Schema>> {
    Ok(Arc::new(parquet::arrow::parquet_to_arrow_schema(
        metadata.file_metadata().schema_descr(),
        metadata.file_metadata().key_value_metadata(),
    )?))
}

fn pruned_row_groups(
    metadata: &ParquetMetaData,
    target_schema: &Arc<Schema>,
    file_size: u64,
    byte_range: Option<&std::ops::Range<u64>>,
    predicate: Option<&DeltaKernelPredicate>,
) -> TestResult<Option<Vec<usize>>> {
    let file_schema = file_schema(metadata)?;
    let alignment = super::super::schema_alignment::build_schema_alignment(
        metadata.file_metadata().schema_descr(),
        &file_schema,
        Arc::clone(target_schema),
    )?;
    Ok(super::pruned_row_groups(
        metadata,
        &file_schema,
        &alignment,
        file_size,
        byte_range,
        predicate,
    )?)
}

struct Case {
    name: String,
    source: ArrayRef,
    target: ArrowType,
}

impl Case {
    fn target_schema(&self) -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "v",
            self.target.clone(),
            true,
        )]))
    }
}

fn add_cases(cases: &mut Vec<Case>, source: ArrayRef, targets: &[ArrowType]) {
    for target in targets {
        cases.push(Case {
            name: format!("{:?} -> {target:?}", source.data_type()),
            source: Arc::clone(&source),
            target: target.clone(),
        });
    }
}

fn conversion_cases() -> TestResult<Vec<Case>> {
    use ArrowType::*;
    let mut cases = Vec::new();
    let integers = Int32Array::from(vec![
        Some(-100),
        Some(-99),
        Some(-1),
        Some(0),
        Some(1),
        Some(99),
        Some(100),
        None,
    ]);
    for (source, targets) in [
        (
            Int8,
            vec![Int8, Int16, Int32, Int64, Float64, Decimal128(5, 2)],
        ),
        (Int16, vec![Int16, Int32, Int64, Float64, Decimal128(7, 2)]),
        (
            Int32,
            vec![Int32, Int64, Float64, Decimal128(12, 2), Date32],
        ),
        (
            Int64,
            vec![
                Int64,
                Decimal128(22, 2),
                Timestamp(TimeUnit::Microsecond, None),
                Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            ],
        ),
    ] {
        add_cases(&mut cases, cast(&integers, &source)?, &targets);
    }
    add_cases(
        &mut cases,
        Arc::new(Float32Array::from(vec![
            Some(-100.5),
            Some(-1.25),
            Some(0.0),
            Some(1.25),
            Some(100.5),
            None,
        ])),
        &[Float32, Float64],
    );
    add_cases(
        &mut cases,
        Arc::new(Float64Array::from(vec![
            Some(-100.5),
            Some(-1.25),
            Some(0.0),
            Some(1.25),
            Some(100.5),
            None,
        ])),
        &[Float64],
    );
    add_cases(
        &mut cases,
        Arc::new(Date32Array::from(vec![
            Some(-719_162),
            Some(-100),
            Some(-1),
            Some(0),
            Some(1),
            Some(100),
            Some(2_932_896),
            None,
        ])),
        &[Date32, Timestamp(TimeUnit::Microsecond, None)],
    );
    for (precision, scale, targets) in [
        (
            8,
            2,
            vec![Decimal128(8, 2), Decimal128(12, 2), Decimal128(12, 4)],
        ),
        (18, 4, vec![Decimal128(18, 4), Decimal128(22, 8)]),
        (38, 2, vec![Decimal128(38, 2)]),
        (4, 0, vec![Decimal128(4, 0), Decimal128(38, 34)]),
    ] {
        let values = [-10001_i128, -10000, -1, 0, 1, 10000, 10001];
        let values = values
            .into_iter()
            .map(|x| Some(if precision == 4 { x / 10 } else { x }))
            .chain([None]);
        add_cases(
            &mut cases,
            Arc::new(
                Decimal128Array::from_iter(values).with_precision_and_scale(precision, scale)?,
            ),
            &targets,
        );
    }
    for scale in [0, 18, 38] {
        let limit = 10_i128.pow(38) - 1;
        add_cases(
            &mut cases,
            Arc::new(
                Decimal128Array::from(vec![
                    Some(-limit),
                    Some(-1),
                    Some(0),
                    Some(1),
                    Some(limit),
                    None,
                ])
                .with_precision_and_scale(38, scale)?,
            ),
            &[Decimal128(38, scale)],
        );
    }
    for (source, target) in [
        (Int8, Int64),
        (Int16, Float64),
        (Int32, Float64),
        (Int64, Decimal128(38, 18)),
    ] {
        let values: ArrayRef = match source {
            Int8 => Arc::new(Int8Array::from(vec![
                i8::MIN,
                i8::MIN + 1,
                i8::MAX - 1,
                i8::MAX,
            ])),
            Int16 => Arc::new(Int16Array::from(vec![
                i16::MIN,
                i16::MIN + 1,
                i16::MAX - 1,
                i16::MAX,
            ])),
            Int32 => Arc::new(Int32Array::from(vec![
                i32::MIN,
                i32::MIN + 1,
                i32::MAX - 1,
                i32::MAX,
            ])),
            _ => Arc::new(Int64Array::from(vec![
                i64::MIN,
                i64::MIN + 1,
                i64::MAX - 1,
                i64::MAX,
            ])),
        };
        add_cases(&mut cases, values, &[target]);
    }
    let times = Int64Array::from(vec![
        Some(-1_000_001),
        Some(-1_000_000),
        Some(-999_999),
        Some(-1001),
        Some(-1000),
        Some(-999),
        Some(-1),
        Some(0),
        Some(1),
        Some(999),
        Some(1000),
        Some(1001),
        Some(999_999),
        Some(1_000_000),
        Some(1_000_001),
        None,
    ]);
    for unit in [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ] {
        for timezone in [None, Some("UTC".into())] {
            add_cases(
                &mut cases,
                cast(&times, &Timestamp(unit, timezone))?,
                &[
                    Timestamp(TimeUnit::Microsecond, None),
                    Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                ],
            );
        }
    }
    for unit in [TimeUnit::Millisecond, TimeUnit::Microsecond] {
        for timezone in ["+08:00", "America/New_York"] {
            add_cases(
                &mut cases,
                cast(&times, &Timestamp(unit, Some(timezone.into())))?,
                &[
                    Timestamp(TimeUnit::Microsecond, None),
                    Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                ],
            );
        }
    }
    add_cases(
        &mut cases,
        Arc::new(BooleanArray::from(vec![Some(false), Some(true), None])),
        &[Boolean],
    );
    add_cases(
        &mut cases,
        Arc::new(StringArray::from(vec![
            Some(""),
            Some("a"),
            Some("aa"),
            Some("b"),
            None,
        ])),
        &[Utf8],
    );
    add_cases(
        &mut cases,
        Arc::new(BinaryArray::from(vec![
            Some(b"".as_slice()),
            Some(b"\x00".as_slice()),
            Some(b"\x7f".as_slice()),
            Some(b"\xff".as_slice()),
            None,
        ])),
        &[Binary],
    );
    Ok(cases)
}

fn parquet_file(
    case: &Case,
    group_size: usize,
    statistics: EnabledStatistics,
) -> TestResult<(Bytes, Arc<ParquetMetaData>)> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "v",
        case.source.data_type().clone(),
        true,
    )]));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::clone(&case.source)])?;
    let mut bytes = Vec::new();
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(group_size))
        .set_statistics_enabled(statistics)
        .build();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(properties))?;
    writer.write(&batch)?;
    writer.close()?;
    let bytes = Bytes::from(bytes);
    let metadata = Arc::clone(ParquetRecordBatchReaderBuilder::try_new(bytes.clone())?.metadata());
    Ok((bytes, metadata))
}

const COMPARISONS: [DeltaComparison; 6] = [
    DeltaComparison::Eq,
    DeltaComparison::NotEq,
    DeltaComparison::Lt,
    DeltaComparison::LtEq,
    DeltaComparison::Gt,
    DeltaComparison::GtEq,
];

fn predicate(op: DeltaComparison, scalar: KernelScalar) -> DeltaKernelPredicate {
    let col = Expression::Column(ColumnName::new(["v"]));
    let literal = Expression::Literal(scalar);
    DeltaKernelPredicate::from_test_predicate(match op {
        DeltaComparison::Eq => Predicate::eq(col, literal),
        DeltaComparison::NotEq => Predicate::ne(col, literal),
        DeltaComparison::Lt => Predicate::lt(col, literal),
        DeltaComparison::LtEq => Predicate::le(col, literal),
        DeltaComparison::Gt => Predicate::gt(col, literal),
        DeltaComparison::GtEq => Predicate::ge(col, literal),
    })
}

fn matching_rows(
    values: &ArrayRef,
    literal: ArrayRef,
    op: DeltaComparison,
) -> TestResult<BooleanArray> {
    let literal = Scalar::new(literal);
    Ok(match op {
        DeltaComparison::Eq => cmp::eq(values, &literal)?,
        DeltaComparison::NotEq => cmp::neq(values, &literal)?,
        DeltaComparison::Lt => cmp::lt(values, &literal)?,
        DeltaComparison::LtEq => cmp::lt_eq(values, &literal)?,
        DeltaComparison::Gt => cmp::gt(values, &literal)?,
        DeltaComparison::GtEq => cmp::gt_eq(values, &literal)?,
    })
}

fn check_matrix(group_size: usize, statistics: EnabledStatistics) -> TestResult {
    let mut failures = Vec::new();
    let cases = conversion_cases()?;
    let mut checked = 0;
    for case in &cases {
        let (bytes, metadata) = parquet_file(case, group_size, statistics)?;
        let converted = cast(case.source.as_ref(), &case.target)?;
        for row in 0..converted.len() {
            if converted.is_null(row) {
                continue;
            }
            let literal = extract_primitive_scalar(converted.as_ref(), row)?;
            for op in COMPARISONS {
                let selected = pruned_row_groups(
                    &metadata,
                    &case.target_schema(),
                    bytes.len() as u64,
                    None,
                    Some(&predicate(op, literal.clone())),
                )?
                .unwrap();
                let expected = matching_rows(&converted, converted.slice(row, 1), op)?;
                let expected_groups = (0..converted.len())
                    .filter(|&i| expected.is_valid(i) && expected.value(i))
                    .map(|i| i / group_size)
                    .collect::<std::collections::BTreeSet<_>>();
                let missing = expected_groups.iter().any(|i| !selected.contains(i));
                // Non-null singleton groups have exact bounds. All-null handling is
                // checked separately from the typed min/max conversion contract.
                let unnecessary = group_size == 1
                    && statistics != EnabledStatistics::None
                    && selected
                        .iter()
                        .any(|&i| converted.is_valid(i) && !expected_groups.contains(&i));
                let missing_unknown = statistics == EnabledStatistics::None
                    && selected.len() != metadata.num_row_groups();
                if missing || unnecessary || missing_unknown {
                    failures.push(format!("{} {op:?} {literal:?}: selected={selected:?}, matching={expected_groups:?}", case.name));
                }
                checked += 1;
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{}/{} comparisons failed across {} conversions (group_size={group_size}, stats={statistics:?}):\n{}",
        failures.len(),
        checked,
        cases.len(),
        failures.join("\n")
    );
    eprintln!(
        "typed statistics matrix: {} conversions, {checked} comparisons, group size {group_size}, {statistics:?}",
        cases.len()
    );
    Ok(())
}

#[test]
fn typed_statistics_singleton_groups_preserve_matches_and_prune_nonmatches() -> TestResult {
    check_matrix(1, EnabledStatistics::Chunk)
}

#[test]
fn typed_statistics_multirow_bounds_never_discard_matching_rows() -> TestResult {
    check_matrix(3, EnabledStatistics::Chunk)
}

#[test]
fn typed_statistics_missing_bounds_keep_every_group() -> TestResult {
    check_matrix(3, EnabledStatistics::None)
}

#[test]
fn typed_statistics_composed_predicates_and_nulls_match_arrow() -> TestResult {
    use arrow::compute::kernels::boolean::{and_kleene, is_not_null, is_null, not, or_kleene};
    let mut failures = Vec::new();
    for case in conversion_cases()? {
        let (bytes, metadata) = parquet_file(&case, 1, EnabledStatistics::Chunk)?;
        let values = cast(case.source.as_ref(), &case.target)?;
        let low_row = 1;
        let high_row = values.len() / 2;
        let low = extract_primitive_scalar(values.as_ref(), low_row)?;
        let high = extract_primitive_scalar(values.as_ref(), high_row)?;
        let column = Expression::Column(ColumnName::new(["v"]));
        let lower = Predicate::ge(column.clone(), Expression::Literal(low));
        let upper = Predicate::le(column.clone(), Expression::Literal(high));
        let lower_mask = matching_rows(&values, values.slice(low_row, 1), DeltaComparison::GtEq)?;
        let upper_mask = matching_rows(&values, values.slice(high_row, 1), DeltaComparison::LtEq)?;
        let both = Predicate::and_from([lower.clone(), upper.clone()]);
        let cases = [
            ("and", both.clone(), and_kleene(&lower_mask, &upper_mask)?),
            (
                "or",
                Predicate::or_from([lower, upper]),
                or_kleene(&lower_mask, &upper_mask)?,
            ),
            (
                "not-and",
                Predicate::not(both),
                not(&and_kleene(&lower_mask, &upper_mask)?)?,
            ),
            (
                "is-null",
                Predicate::is_null(column.clone()),
                is_null(values.as_ref())?,
            ),
            (
                "is-not-null",
                Predicate::is_not_null(column),
                is_not_null(values.as_ref())?,
            ),
        ];
        for (name, expression, expected) in cases {
            let predicate = DeltaKernelPredicate::from_test_predicate(expression);
            let actual = pruned_row_groups(
                &metadata,
                &case.target_schema(),
                bytes.len() as u64,
                None,
                Some(&predicate),
            )?
            .unwrap();
            let expected = (0..values.len())
                .filter(|&i| expected.is_valid(i) && expected.value(i))
                .collect::<Vec<_>>();
            if actual != expected {
                failures.push(format!(
                    "{} {name}: got={actual:?}, expected={expected:?}",
                    case.name
                ));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "composed/null predicate failures:\n{}",
        failures.join("\n")
    );
    Ok(())
}

#[test]
fn typed_statistics_byte_ranges_intersect_typed_pruning() -> TestResult {
    let case = Case {
        name: "range ownership".into(),
        source: Arc::new(Int32Array::from(vec![-200, -100, 0, 100, 200])),
        target: ArrowType::Decimal128(12, 2),
    };
    let (bytes, metadata) = parquet_file(&case, 1, EnabledStatistics::Chunk)?;
    let predicate = predicate(
        DeltaComparison::GtEq,
        KernelScalar::decimal(10000_i128, 12, 2)?,
    );
    for group in 0..metadata.num_row_groups() {
        let column = metadata.row_group(group).column(0);
        let offset = column
            .dictionary_page_offset()
            .unwrap_or_else(|| column.data_page_offset()) as u64;
        let actual = pruned_row_groups(
            &metadata,
            &case.target_schema(),
            bytes.len() as u64,
            Some(&(offset..offset + 1)),
            Some(&predicate),
        )?
        .unwrap();
        assert_eq!(
            actual,
            if group >= 3 { vec![group] } else { vec![] },
            "group {group}"
        );
    }
    Ok(())
}

fn metadata_with_stats(
    physical_schema: &str,
    statistics: Statistics,
) -> TestResult<ParquetMetaData> {
    let schema = Arc::new(SchemaDescriptor::new(Arc::new(parse_message_type(
        physical_schema,
    )?)));
    let column = ColumnChunkMetaData::builder(schema.column(0))
        .set_data_page_offset(0)
        .set_num_values(1)
        .set_statistics(statistics)
        .build()?;
    let group = RowGroupMetaData::builder(Arc::clone(&schema))
        .set_num_rows(1)
        .set_column_metadata(vec![column])
        .build()?;
    Ok(ParquetMetaData::new(
        FileMetaData::new(1, 1, None, None, schema, None),
        vec![group],
    ))
}

fn selected(
    metadata: &ParquetMetaData,
    op: DeltaComparison,
    literal: KernelScalar,
) -> TestResult<Vec<usize>> {
    // Keep the file schema here to exercise unsupported literal types in the
    // bounds adapter without asking the data reader to accept an invalid cast.
    Ok(pruned_row_groups(
        metadata,
        &file_schema(metadata)?,
        1,
        None,
        Some(&predicate(op, literal)),
    )?
    .unwrap())
}

#[test]
fn typed_statistics_decimal_physical_encodings_and_sign_extension() -> TestResult {
    let mut failures = Vec::new();
    for raw in [-20_001_i32, -20_000, -1, 0, 1, 20_000, 20_001] {
        let bytes = ByteArray::from(raw.to_be_bytes().to_vec());
        let encodings = [
            (
                "INT32 v (DECIMAL(8,2))",
                Statistics::int32(Some(raw), Some(raw), None, Some(0), false),
            ),
            (
                "INT64 v (DECIMAL(8,2))",
                Statistics::int64(
                    Some(i64::from(raw)),
                    Some(i64::from(raw)),
                    None,
                    Some(0),
                    false,
                ),
            ),
            (
                "BYTE_ARRAY v (DECIMAL(8,2))",
                Statistics::byte_array(
                    Some(bytes.clone()),
                    Some(bytes.clone()),
                    None,
                    Some(0),
                    false,
                ),
            ),
            (
                "FIXED_LEN_BYTE_ARRAY(4) v (DECIMAL(8,2))",
                Statistics::fixed_len_byte_array(
                    Some(FixedLenByteArray::from(bytes.clone())),
                    Some(FixedLenByteArray::from(bytes)),
                    None,
                    Some(0),
                    false,
                ),
            ),
        ];
        for (encoding, stats) in encodings {
            let metadata =
                metadata_with_stats(&format!("message m {{ OPTIONAL {encoding}; }}"), stats)?;
            // Independent integer arithmetic defines the expected widened value.
            let expected = i128::from(raw) * 100;
            for value in [expected - 1, expected, expected + 1] {
                for op in COMPARISONS {
                    let keep = match op {
                        DeltaComparison::Eq => expected == value,
                        DeltaComparison::NotEq => expected != value,
                        DeltaComparison::Lt => expected < value,
                        DeltaComparison::LtEq => expected <= value,
                        DeltaComparison::Gt => expected > value,
                        DeltaComparison::GtEq => expected >= value,
                    };
                    let actual = selected(&metadata, op, KernelScalar::decimal(value, 12, 4)?)?;
                    if actual != if keep { vec![0] } else { vec![] } {
                        failures.push(format!("{encoding}: raw={raw}, {op:?} {value}, selected={actual:?}, keep={keep}"));
                    }
                }
            }
        }
    }
    assert!(
        failures.is_empty(),
        "decimal encoding failures:\n{}",
        failures.join("\n")
    );
    Ok(())
}

#[test]
fn typed_statistics_incomplete_bounds_are_independent() -> TestResult {
    for (min, max) in [(Some(200_i32), None), (None, Some(200)), (None, None)] {
        let metadata = metadata_with_stats(
            "message m { OPTIONAL INT32 v; }",
            Statistics::int32(min, max, None, Some(0), false),
        )?;
        let lower = selected(
            &metadata,
            DeltaComparison::Lt,
            KernelScalar::decimal(19900_i128, 12, 2)?,
        )?;
        let upper = selected(
            &metadata,
            DeltaComparison::Gt,
            KernelScalar::decimal(20100_i128, 12, 2)?,
        )?;
        assert_eq!(
            lower.is_empty(),
            min.is_some(),
            "minimum {min:?}, maximum {max:?}"
        );
        assert_eq!(
            upper.is_empty(),
            max.is_some(),
            "minimum {min:?}, maximum {max:?}"
        );
        assert_eq!(
            selected(
                &metadata,
                DeltaComparison::Eq,
                KernelScalar::decimal(20000_i128, 12, 2)?
            )?,
            vec![0]
        );
    }
    Ok(())
}

#[test]
fn typed_statistics_unsupported_and_overflowing_conversions_are_unknown() -> TestResult {
    let cases = [
        (
            "message m { OPTIONAL INT32 v; }",
            Statistics::int32(Some(1000), Some(1000), None, Some(0), false),
            KernelScalar::Byte(0),
        ),
        (
            "message m { OPTIONAL INT32 v (DATE); }",
            Statistics::int32(Some(1), Some(1), None, Some(0), false),
            KernelScalar::decimal(1_i128, 12, 2)?,
        ),
        (
            "message m { OPTIONAL INT64 v (TIMESTAMP(MILLIS,true)); }",
            Statistics::int64(Some(i64::MAX), Some(i64::MAX), None, Some(0), false),
            KernelScalar::Timestamp(0),
        ),
        (
            "message m { OPTIONAL INT64 v (TIMESTAMP(MILLIS,true)); }",
            Statistics::int64(Some(i64::MIN), Some(i64::MIN), None, Some(0), false),
            KernelScalar::Timestamp(0),
        ),
        (
            "message m { OPTIONAL INT64 v (DECIMAL(18,2)); }",
            Statistics::int64(Some(1), Some(1), None, Some(0), false),
            KernelScalar::decimal(0_i128, 18, 1)?,
        ),
    ];
    let mut failures = Vec::new();
    for (schema, stats, scalar) in cases {
        let metadata = metadata_with_stats(schema, stats)?;
        for op in COMPARISONS {
            if selected(&metadata, op, scalar.clone())? != vec![0] {
                failures.push(format!("{schema} {op:?} {scalar:?}"));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "unusable statistics pruned a row group:\n{}",
        failures.join("\n")
    );
    Ok(())
}

#[test]
fn typed_statistics_malformed_bounds_do_not_panic_or_prune() -> TestResult {
    let mut cases = Vec::new();
    for bytes in [vec![], vec![0; 17], vec![0xff; 17], vec![100]] {
        let bytes = ByteArray::from(bytes);
        cases.push((
            "message m { OPTIONAL BYTE_ARRAY v (DECIMAL(2,0)); }",
            Statistics::byte_array(Some(bytes.clone()), Some(bytes), None, Some(0), false),
            KernelScalar::decimal(0_i128, 12, 2)?,
        ));
    }
    cases.extend([
        (
            "message m { OPTIONAL INT32 v; }",
            Statistics::int64(Some(1), Some(1), None, Some(0), false),
            KernelScalar::Integer(0),
        ),
        (
            "message m { OPTIONAL INT32 v (INT_8); }",
            Statistics::int32(Some(1000), Some(1000), None, Some(0), false),
            KernelScalar::Byte(0),
        ),
        (
            "message m { OPTIONAL INT32 v (DATE); }",
            Statistics::int32(Some(i32::MAX), Some(i32::MAX), None, Some(0), false),
            KernelScalar::TimestampNtz(0),
        ),
        (
            "message m { OPTIONAL INT32 v (DATE); }",
            Statistics::int32(Some(i32::MIN), Some(i32::MIN), None, Some(0), false),
            KernelScalar::TimestampNtz(0),
        ),
        (
            "message m { OPTIONAL BYTE_ARRAY v (UTF8); }",
            Statistics::byte_array(
                Some(ByteArray::from(vec![0xff])),
                Some(ByteArray::from(vec![0xff])),
                None,
                Some(0),
                false,
            ),
            KernelScalar::String("".into()),
        ),
    ]);
    for (schema, stats, literal) in cases {
        let metadata = metadata_with_stats(schema, stats)?;
        for op in COMPARISONS {
            assert_eq!(
                selected(&metadata, op, literal.clone())?,
                vec![0],
                "{schema} {op:?} {literal:?}"
            );
        }
    }
    Ok(())
}

#[test]
fn typed_statistics_null_counts_respect_conversion_and_missing_bounds() -> TestResult {
    for (schema, target, safe_without_bounds) in [
        (
            "message m { OPTIONAL INT32 v; }",
            ArrowType::Decimal128(12, 2),
            true,
        ),
        (
            "message m { OPTIONAL INT32 v (DATE); }",
            ArrowType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        (
            "message m { OPTIONAL INT32 v (DATE); }",
            ArrowType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        (
            "message m { OPTIONAL INT32 v (DATE); }",
            ArrowType::Timestamp(TimeUnit::Second, None),
            true,
        ),
        (
            "message m { OPTIONAL INT32 v (DATE); }",
            ArrowType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        (
            "message m { OPTIONAL INT64 v (TIMESTAMP(NANOS,false)); }",
            ArrowType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        (
            "message m { OPTIONAL INT64 v (TIMESTAMP(MILLIS,false)); }",
            ArrowType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        (
            "message m { OPTIONAL INT64 v (TIMESTAMP(MICROS,true)); }",
            ArrowType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        (
            "message m { OPTIONAL INT64 v (TIMESTAMP(MICROS,false)); }",
            ArrowType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
    ] {
        for nulls in [None, Some(0), Some(1), Some(u64::MAX)] {
            let statistics = if schema.contains("INT32") {
                Statistics::int32(None, None, None, nulls, false)
            } else {
                Statistics::int64(None, None, None, nulls, false)
            };
            let metadata = metadata_with_stats(schema, statistics)?;
            let target_schema = Arc::new(Schema::new(vec![Field::new("v", target.clone(), true)]));
            for is_null in [false, true] {
                let column = Expression::Column(ColumnName::new(["v"]));
                let expression = if is_null {
                    Predicate::is_null(column)
                } else {
                    Predicate::is_not_null(column)
                };
                let predicate = DeltaKernelPredicate::from_test_predicate(expression);
                let selected =
                    pruned_row_groups(&metadata, &target_schema, 1, None, Some(&predicate))?
                        .unwrap();
                let should_prune = if is_null {
                    nulls == Some(0) && safe_without_bounds
                } else {
                    nulls == Some(1)
                };
                assert_eq!(
                    selected.is_empty(),
                    should_prune,
                    "{schema} -> {target:?}, nulls={nulls:?}, is_null={is_null}"
                );
            }
        }
    }
    Ok(())
}

#[test]
fn typed_statistics_nested_leaf_bounds_do_not_describe_the_parent() -> TestResult {
    let metadata = metadata_with_stats(
        "message m { OPTIONAL GROUP v { OPTIONAL INT32 child; } }",
        Statistics::int32(None, None, None, Some(1), false),
    )?;
    for expression in [
        Predicate::is_null(Expression::Column(ColumnName::new(["v"]))),
        Predicate::is_not_null(Expression::Column(ColumnName::new(["v"]))),
    ] {
        let predicate = DeltaKernelPredicate::from_test_predicate(expression);
        assert_eq!(
            pruned_row_groups(
                &metadata,
                &file_schema(&metadata)?,
                1,
                None,
                Some(&predicate)
            )?,
            Some(vec![0])
        );
    }
    Ok(())
}

#[tokio::test]
async fn typed_statistics_widening_preserves_original_row_indexes() -> TestResult {
    use super::super::{
        PhysicalParquetStreamOptions,
        tests::{metrics, reader, task},
    };
    let case = Case {
        name: "original row indexes".into(),
        source: Arc::new(Int32Array::from(vec![-200, -100, 0, 100, 200])),
        target: ArrowType::Decimal128(12, 2),
    };
    let (bytes, _) = parquet_file(&case, 1, EnabledStatistics::Chunk)?;
    let root = TestDir::new("typed-statistics-row-indexes")?;
    fs::write(root.path().join("part.parquet"), &bytes)?;
    let reader = reader(&root, crate::DeltaScanExecutionOptions::new(), metrics())?;
    let task = task("part.parquet", Some(bytes.len() as u64))?;
    let schema = Arc::new(Schema::new(vec![Field::new("v", case.target, true)]));
    let predicate = predicate(
        DeltaComparison::GtEq,
        KernelScalar::decimal(10000_i128, 12, 2)?,
    );
    let mut stream = reader
        .open_physical_parquet_stream(
            &task,
            &schema,
            PhysicalParquetStreamOptions {
                row_group_predicate: Some(&predicate),
                include_original_row_index: true,
                output_batch_size_rows: Some(1),
                ..Default::default()
            },
        )
        .await?;
    let mut indexes = Vec::new();
    let mut values = Vec::new();
    while let Some((batch, original)) = stream.next_batch_with_original_row_indexes().await? {
        indexes.extend_from_slice(original.ok_or("missing original row indexes")?.values());
        values.extend_from_slice(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .values(),
        );
    }
    assert_eq!(indexes, [3, 4]);
    assert_eq!(values, [10000, 20000]);
    Ok(())
}

fn delta_type(data_type: &ArrowType) -> String {
    use ArrowType::*;
    match data_type {
        Int8 => "byte".into(),
        Int16 => "short".into(),
        Int32 => "integer".into(),
        Int64 => "long".into(),
        Float32 => "float".into(),
        Float64 => "double".into(),
        Boolean => "boolean".into(),
        Date32 => "date".into(),
        Utf8 => "string".into(),
        Binary => "binary".into(),
        Decimal128(p, s) => format!("decimal({p},{s})"),
        Timestamp(_, None) => "timestamp_ntz".into(),
        Timestamp(_, Some(_)) => "timestamp".into(),
        _ => unreachable!("matrix contains only supported scalar types"),
    }
}

fn metadata_action(data_type: &str, mapped: bool, from: Option<&str>) -> Value {
    let mut value_metadata = json!({});
    let mut id_metadata = json!({});
    let mut configuration = json!({"delta.enableTypeWidening":"true"});
    if mapped {
        value_metadata =
            json!({"delta.columnMapping.id":2,"delta.columnMapping.physicalName":"phys_v"});
        id_metadata =
            json!({"delta.columnMapping.id":1,"delta.columnMapping.physicalName":"phys_id"});
        configuration["delta.columnMapping.mode"] = json!("name");
        configuration["delta.columnMapping.maxColumnId"] = json!("2");
    }
    if let Some(from) = from {
        value_metadata["delta.typeChanges"] = json!([{"fromType":from,"toType":data_type}]);
    }
    json!({"metaData":{"id":"typed-statistics-regression","format":{"provider":"parquet","options":{}},"schemaString":json!({"type":"struct","fields":[
        {"name":"id","type":"integer","nullable":false,"metadata":id_metadata},
        {"name":"v","type":data_type,"nullable":true,"metadata":value_metadata}
    ]}).to_string(),"partitionColumns":[],"configuration":configuration}})
}

async fn public_table(case: &Case, mapped: bool) -> TestResult<(TestDir, DeltaTable, ArrayRef)> {
    let converted = cast(case.source.as_ref(), &case.target)?;
    public_table_with_converted_values(case, mapped, converted).await
}

async fn public_table_with_converted_values(
    case: &Case,
    mapped: bool,
    converted: ArrayRef,
) -> TestResult<(TestDir, DeltaTable, ArrayRef)> {
    let root = TestDir::new("typed-statistics")?;
    fs::create_dir_all(root.path().join("_delta_log"))?;
    let mut adds = Vec::new();
    for (index, values) in [Arc::clone(&case.source), Arc::clone(&converted)]
        .into_iter()
        .enumerate()
    {
        let path = format!("part-{index}.parquet");
        let mut value_field = Field::new(
            if mapped { "phys_v" } else { "v" },
            values.data_type().clone(),
            true,
        );
        let mut id_field = Field::new(
            if mapped { "phys_id" } else { "id" },
            ArrowType::Int32,
            false,
        );
        if mapped {
            value_field = value_field.with_metadata(
                [(
                    parquet::arrow::PARQUET_FIELD_ID_META_KEY.to_string(),
                    "2".to_string(),
                )]
                .into(),
            );
            id_field = id_field.with_metadata(
                [(
                    parquet::arrow::PARQUET_FIELD_ID_META_KEY.to_string(),
                    "1".to_string(),
                )]
                .into(),
            );
        }
        let ids = Arc::new(Int32Array::from_iter_values(
            (0..values.len()).map(|i| i32::try_from(index * values.len() + i).unwrap()),
        )) as ArrayRef;
        // The old file also uses the opposite physical column order.
        let (fields, columns) = if index == 0 {
            (vec![value_field, id_field], vec![values, ids])
        } else {
            (vec![id_field, value_field], vec![ids, values])
        };
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns)?;
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(3))
            .build();
        let mut writer = ArrowWriter::try_new(
            fs::File::create(root.path().join(&path))?,
            schema,
            Some(props),
        )?;
        writer.write(&batch)?;
        writer.close()?;
        // No add-action stats: every filtering decision being tested reaches the footer.
        adds.push(json!({"add":{"path":path,"partitionValues":{},"size":fs::metadata(root.path().join(&path))?.len(),"modificationTime":0,"dataChange":true}}));
    }
    let source_name = delta_type(case.source.data_type());
    let target_name = delta_type(&case.target);
    // Raw integer dates/timestamps and timestamp unit/timezone conversions are
    // reader compatibility paths, not Delta type-widening transitions.
    let widening = source_name != target_name
        && !matches!(
            (case.source.data_type(), &case.target),
            (ArrowType::Int32, ArrowType::Date32)
                | (ArrowType::Int64, ArrowType::Timestamp(_, _))
                | (ArrowType::Timestamp(_, _), ArrowType::Timestamp(_, _))
        );
    let mut features = vec!["typeWidening", "timestampNtz"];
    if mapped {
        features.push("columnMapping");
    }
    let protocol = json!({"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":features,"writerFeatures":features}});
    fs::write(
        root.path().join("_delta_log/00000000000000000000.json"),
        format!(
            "{protocol}\n{}\n{}\n",
            metadata_action(
                if widening { &source_name } else { &target_name },
                mapped,
                None
            ),
            adds[0]
        ),
    )?;
    fs::write(
        root.path().join("_delta_log/00000000000000000001.json"),
        format!(
            "{}\n{}\n",
            metadata_action(
                &target_name,
                mapped,
                widening.then_some(source_name.as_str())
            ),
            adds[1]
        ),
    )?;
    let table = DeltaTableBuilder::new(root.path().to_string_lossy())
        .load_table()
        .await?;
    Ok((
        root,
        table,
        arrow::compute::concat(&[converted.as_ref(), converted.as_ref()])?,
    ))
}

fn public_scalar(value: KernelScalar) -> DeltaScalar {
    match value {
        KernelScalar::Byte(v) => DeltaScalar::Int8(v),
        KernelScalar::Short(v) => DeltaScalar::Int16(v),
        KernelScalar::Integer(v) => DeltaScalar::Int32(v),
        KernelScalar::Long(v) => DeltaScalar::Int64(v),
        KernelScalar::Float(v) => DeltaScalar::Float32(v),
        KernelScalar::Double(v) => DeltaScalar::Float64(v),
        KernelScalar::Boolean(v) => DeltaScalar::Boolean(v),
        KernelScalar::String(v) => DeltaScalar::Utf8(v),
        KernelScalar::Binary(v) => DeltaScalar::Binary(v),
        KernelScalar::Date(v) => DeltaScalar::Date32(v),
        KernelScalar::Timestamp(v) => DeltaScalar::TimestampMicrosecond {
            value: v,
            timezone: Some("UTC".into()),
        },
        KernelScalar::TimestampNtz(v) => DeltaScalar::TimestampMicrosecond {
            value: v,
            timezone: None,
        },
        KernelScalar::Decimal(v) => DeltaScalar::Decimal128 {
            value: v.bits(),
            precision: v.precision(),
            scale: v.scale() as i8,
        },
        _ => unreachable!("matrix predicates use non-null primitive scalars"),
    }
}

fn ids(batches: &[RecordBatch]) -> Vec<i32> {
    let mut result = batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
        })
        .collect::<Vec<_>>();
    result.sort_unstable();
    result
}

fn expected_ids(mask: &BooleanArray) -> Vec<i32> {
    (0..mask.len())
        .filter(|&i| mask.is_valid(i) && mask.value(i))
        .map(|i| i32::try_from(i).unwrap())
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_statistics_streaming_matches_arrow_after_real_schema_evolution() -> TestResult {
    let mut failures = Vec::new();
    for case in conversion_cases()? {
        for mapped in [false, true] {
            let (_root, table, values) = public_table(&case, mapped).await?;
            // Independently check the unfiltered data before using it as an oracle.
            let full = table
                .scan()
                .with_target_partitions(1)?
                .build()
                .await?
                .into_stream()
                .try_collect::<Vec<_>>()
                .await?;
            assert_eq!(
                full.iter().map(RecordBatch::num_rows).sum::<usize>(),
                values.len(),
                "{} full-scan row count",
                case.name
            );
            assert_eq!(
                ids(&full),
                (0..values.len()).map(|i| i as i32).collect::<Vec<_>>(),
                "{} full-scan row identities",
                case.name
            );
            let mut actual = vec![None; values.len()];
            for batch in full {
                let row_ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                for i in 0..batch.num_rows() {
                    actual[row_ids.value(i) as usize] =
                        Some(extract_primitive_scalar(batch.column(1).as_ref(), i)?);
                }
            }
            for (i, value) in actual.into_iter().enumerate() {
                assert_eq!(
                    value,
                    Some(extract_primitive_scalar(values.as_ref(), i)?),
                    "{} unfiltered row {i}",
                    case.name
                );
            }
            for row in [0, case.source.len() / 2, case.source.len() - 2] {
                if values.is_null(row) {
                    continue;
                }
                for op in COMPARISONS {
                    let scalar = extract_primitive_scalar(values.as_ref(), row)?;
                    let predicate = DeltaPredicate::Compare {
                        column: "v".into(),
                        op,
                        value: public_scalar(scalar.clone()),
                    };
                    let output = table
                        .scan()
                        .with_target_partitions(1)?
                        .with_projection(["id"])
                        .with_predicate(predicate)
                        .build()
                        .await?
                        .into_stream()
                        .try_collect::<Vec<_>>()
                        .await?;
                    let expected = expected_ids(&matching_rows(&values, values.slice(row, 1), op)?);
                    let actual = ids(&output);
                    if actual != expected {
                        failures.push(format!("{} mapped={mapped}, {op:?} {scalar:?}: got={actual:?}, expected={expected:?}", case.name));
                    }
                }
            }
        }
    }
    assert!(
        failures.is_empty(),
        "streaming mismatches:\n{}",
        failures.join("\n")
    );
    Ok(())
}

#[cfg(feature = "datafusion")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_statistics_datafusion_matches_arrow_with_both_view_settings() -> TestResult {
    use crate::datafusion::{DeltaTableProvider, ScanOptions};
    use datafusion::{
        common::ScalarValue,
        prelude::{SessionConfig, SessionContext, col, lit},
    };
    let mut failures = Vec::new();
    for case in conversion_cases()? {
        let (_root, table, values) = public_table(&case, true).await?;
        for use_arrow_view_types in [false, true] {
            let provider = Arc::new(DeltaTableProvider::try_new(
                table.clone(),
                ScanOptions {
                    use_arrow_view_types,
                    ..Default::default()
                },
            )?);
            let context =
                SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
            for row in [0, case.source.len() / 2, case.source.len() - 2] {
                if values.is_null(row) {
                    continue;
                }
                for op in COMPARISONS {
                    let scalar = ScalarValue::try_from_array(values.as_ref(), row)?;
                    let literal = lit(scalar.clone());
                    let expression = match op {
                        DeltaComparison::Eq => col("v").eq(literal),
                        DeltaComparison::NotEq => col("v").not_eq(literal),
                        DeltaComparison::Lt => col("v").lt(literal),
                        DeltaComparison::LtEq => col("v").lt_eq(literal),
                        DeltaComparison::Gt => col("v").gt(literal),
                        DeltaComparison::GtEq => col("v").gt_eq(literal),
                    };
                    let output = context
                        .read_table(provider.clone())?
                        .filter(expression)?
                        .select_columns(&["id"])?
                        .collect()
                        .await?;
                    let expected = expected_ids(&matching_rows(&values, values.slice(row, 1), op)?);
                    let actual = ids(&output);
                    if actual != expected {
                        failures.push(format!("{} views={use_arrow_view_types}, {op:?} {scalar:?}: got={actual:?}, expected={expected:?}", case.name));
                    }
                }
            }
        }
    }
    assert!(
        failures.is_empty(),
        "DataFusion mismatches:\n{}",
        failures.join("\n")
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_statistics_timestamp_cast_overflow_does_not_reuse_file_null_counts() -> TestResult {
    let case = Case {
        name: "overflow creates logical nulls".into(),
        source: Arc::new(TimestampMillisecondArray::from(vec![
            Some(i64::MIN),
            Some(i64::MIN + 1),
            Some(i64::MIN + 2),
            Some(-1),
            Some(0),
            Some(1),
            Some(i64::MAX - 2),
            Some(i64::MAX - 1),
            Some(i64::MAX),
            None,
        ])),
        target: ArrowType::Timestamp(TimeUnit::Microsecond, None),
    };
    let (_root, table, values) = public_table(&case, false).await?;
    for (predicate, expected) in [
        (
            DeltaPredicate::IsNull { column: "v".into() },
            arrow::compute::is_null(values.as_ref())?,
        ),
        (
            DeltaPredicate::IsNotNull { column: "v".into() },
            arrow::compute::is_not_null(values.as_ref())?,
        ),
    ] {
        let output = table
            .scan()
            .with_target_partitions(1)?
            .with_projection(["id"])
            .with_predicate(predicate.clone())
            .build()
            .await?
            .into_stream()
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(ids(&output), expected_ids(&expected), "{predicate:?}");
        #[cfg(feature = "datafusion")]
        {
            use crate::datafusion::{DeltaTableProvider, ScanOptions};
            use datafusion::prelude::{SessionConfig, SessionContext, col};
            for use_arrow_view_types in [false, true] {
                let context =
                    SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
                let provider = Arc::new(DeltaTableProvider::try_new(
                    table.clone(),
                    ScanOptions {
                        use_arrow_view_types,
                        ..Default::default()
                    },
                )?);
                let expression = if matches!(predicate, DeltaPredicate::IsNull { .. }) {
                    col("v").is_null()
                } else {
                    col("v").is_not_null()
                };
                let output = context
                    .read_table(provider)?
                    .filter(expression)?
                    .select_columns(&["id"])?
                    .collect()
                    .await?;
                assert_eq!(
                    ids(&output),
                    expected_ids(&expected),
                    "DataFusion views={use_arrow_view_types}, {predicate:?}"
                );
            }
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_statistics_original_report_examples_keep_the_old_and_new_file_rows() -> TestResult {
    let cases = [
        (
            Case {
                name: "integer 200 becomes decimal 200.00".into(),
                source: Arc::new(Int32Array::from(vec![200])),
                target: ArrowType::Decimal128(12, 2),
            },
            DeltaComparison::Gt,
            KernelScalar::decimal(15000_i128, 12, 2)?,
        ),
        (
            Case {
                name: "decimal 200.00 becomes 200.0000".into(),
                source: Arc::new(
                    Decimal128Array::from(vec![20000_i128]).with_precision_and_scale(10, 2)?,
                ),
                target: ArrowType::Decimal128(12, 4),
            },
            DeltaComparison::Gt,
            KernelScalar::decimal(1500000_i128, 12, 4)?,
        ),
        (
            Case {
                name: "2021 milliseconds after 2020".into(),
                source: Arc::new(
                    TimestampMillisecondArray::from(vec![1_609_459_200_000]).with_timezone("UTC"),
                ),
                target: ArrowType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            },
            DeltaComparison::Gt,
            KernelScalar::Timestamp(1_577_836_800_000_000),
        ),
        (
            Case {
                name: "2021 nanoseconds before 2025".into(),
                source: Arc::new(
                    TimestampNanosecondArray::from(vec![1_609_459_200_000_000_000])
                        .with_timezone("UTC"),
                ),
                target: ArrowType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            },
            DeltaComparison::Lt,
            KernelScalar::Timestamp(1_735_689_600_000_000),
        ),
    ];
    for (case, op, scalar) in cases {
        let (_root, table, _) = public_table(&case, false).await?;
        let output = table
            .scan()
            .with_target_partitions(1)?
            .with_projection(["id"])
            .with_predicate(DeltaPredicate::Compare {
                column: "v".into(),
                op,
                value: public_scalar(scalar),
            })
            .build()
            .await?
            .into_stream()
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(
            ids(&output),
            vec![0, 1],
            "{} must preserve the old file too",
            case.name
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_statistics_field_id_mapping_matches_data_and_prunes() -> TestResult {
    let field = |name: &str, data_type: ArrowType, id: i32| {
        Field::new(name, data_type, true).with_metadata(
            [(
                parquet::arrow::PARQUET_FIELD_ID_META_KEY.to_owned(),
                id.to_string(),
            )]
            .into(),
        )
    };
    let source = Arc::new(Int32Array::from(vec![
        Some(200),
        None,
        Some(0),
        None,
        Some(200),
    ])) as ArrayRef;
    let expected_values = cast(source.as_ref(), &ArrowType::Float64)?;
    let mut failures = Vec::new();
    for mode in ["name", "id"] {
        for reversed in [false, true] {
            for collision in [None, Some("phys_v"), Some("old_v")] {
                let label = format!("mode={mode}, reversed={reversed}, collision={collision:?}");
                let root = TestDir::new("typed-statistics-field-ids")?;
                fs::create_dir_all(root.path().join("_delta_log"))?;
                // An unprojected struct makes root indices differ from leaf indices.
                let nested = StructArray::from(vec![
                    (
                        Arc::new(Field::new("a", ArrowType::Int32, true)),
                        Arc::new(Int32Array::from(vec![0; 5])) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("b", ArrowType::Int32, true)),
                        Arc::new(Int32Array::from(vec![0; 5])) as ArrayRef,
                    ),
                ]);
                let mut fields = vec![
                    Field::new("unprojected", nested.data_type().clone(), true),
                    field("phys_id", ArrowType::Int32, 1),
                    field("old_v", ArrowType::Int32, 2),
                ];
                let mut columns = vec![
                    Arc::new(nested) as ArrayRef,
                    Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4])) as ArrayRef,
                    Arc::clone(&source),
                ];
                if let Some(name) = collision {
                    fields.push(field(name, ArrowType::Int32, 3));
                    columns.push(Arc::new(Int32Array::from(vec![
                        Some(0),
                        Some(300),
                        None,
                        Some(100),
                        Some(0),
                    ])));
                }
                if reversed {
                    fields.reverse();
                    columns.reverse();
                }
                let file_schema = Arc::new(Schema::new(fields));
                let batch = RecordBatch::try_new(Arc::clone(&file_schema), columns)?;
                let mut bytes = Vec::new();
                let props = WriterProperties::builder()
                    .set_max_row_group_row_count(Some(1))
                    .build();
                let mut writer =
                    ArrowWriter::try_new(&mut bytes, Arc::clone(&file_schema), Some(props))?;
                writer.write(&batch)?;
                writer.close()?;
                let bytes = Bytes::from(bytes);
                let builder = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())?;
                let target_schema =
                    Arc::new(Schema::new(vec![field("phys_v", ArrowType::Float64, 2)]));
                let column = Expression::Column(ColumnName::new(["phys_v"]));
                let pruning_predicates = [
                    (
                        Predicate::gt(
                            column.clone(),
                            Expression::Literal(KernelScalar::Double(150.0)),
                        ),
                        vec![0, 4],
                    ),
                    (
                        Predicate::lt(
                            column.clone(),
                            Expression::Literal(KernelScalar::Double(150.0)),
                        ),
                        vec![2],
                    ),
                    (Predicate::is_null(column.clone()), vec![1, 3]),
                    (Predicate::is_not_null(column), vec![0, 2, 4]),
                ];
                for (expression, expected) in pruning_predicates {
                    let predicate = DeltaKernelPredicate::from_test_predicate(expression);
                    let selected = pruned_row_groups(
                        builder.metadata(),
                        &target_schema,
                        bytes.len() as u64,
                        None,
                        Some(&predicate),
                    )?
                    .unwrap();
                    // The converter can only select the first file column with a
                    // given name. An ambiguous match may keep extra groups, but
                    // must never discard a group containing matching values.
                    let ambiguous = reversed && collision == Some("old_v");
                    let missing = expected.iter().any(|group| !selected.contains(group));
                    if missing || (!ambiguous && selected != expected) {
                        failures.push(format!(
                            "{label}: groups={selected:?}, expected={expected:?}"
                        ));
                    }
                }
                fs::write(root.path().join("part.parquet"), &bytes)?;
                let mut metadata = metadata_action("double", true, Some("integer"));
                metadata["metaData"]["configuration"]["delta.columnMapping.mode"] = json!(mode);
                metadata["metaData"]["configuration"]["delta.columnMapping.maxColumnId"] =
                    json!("3");
                let protocol = json!({"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["columnMapping","typeWidening"],"writerFeatures":["columnMapping","typeWidening"]}});
                let add = json!({"add":{"path":"part.parquet","partitionValues":{},"size":bytes.len(),"modificationTime":0,"dataChange":true}});
                fs::write(
                    root.path().join("_delta_log/00000000000000000000.json"),
                    format!("{protocol}\n{metadata}\n{add}\n"),
                )?;
                let table = DeltaTableBuilder::new(root.path().to_string_lossy())
                    .load_table()
                    .await?;
                let full = table
                    .scan()
                    .with_target_partitions(1)?
                    .build()
                    .await?
                    .into_stream()
                    .try_collect::<Vec<_>>()
                    .await?;
                assert_eq!(ids(&full), vec![0, 1, 2, 3, 4], "{label}");
                for batch in &full {
                    let row_ids = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap();
                    for i in 0..batch.num_rows() {
                        assert_eq!(
                            extract_primitive_scalar(batch.column(1).as_ref(), i)?,
                            extract_primitive_scalar(
                                expected_values.as_ref(),
                                row_ids.value(i) as usize
                            )?,
                            "{label}"
                        );
                    }
                }
                let mut predicates = COMPARISONS
                    .into_iter()
                    .map(|op| {
                        Ok((
                            DeltaPredicate::Compare {
                                column: "v".into(),
                                op,
                                value: DeltaScalar::Float64(150.0),
                            },
                            matching_rows(
                                &expected_values,
                                Arc::new(Float64Array::from(vec![150.0])),
                                op,
                            )?,
                        ))
                    })
                    .collect::<TestResult<Vec<_>>>()?;
                predicates.push((
                    DeltaPredicate::IsNull { column: "v".into() },
                    arrow::compute::is_null(expected_values.as_ref())?,
                ));
                predicates.push((
                    DeltaPredicate::IsNotNull { column: "v".into() },
                    arrow::compute::is_not_null(expected_values.as_ref())?,
                ));
                for (predicate, expected) in predicates {
                    let expected = expected_ids(&expected);
                    let out = table
                        .scan()
                        .with_target_partitions(1)?
                        .with_projection(["id"])
                        .with_predicate(predicate.clone())
                        .build()
                        .await?
                        .into_stream()
                        .try_collect::<Vec<_>>()
                        .await?;
                    if ids(&out) != expected {
                        failures.push(format!(
                            "{label}: {predicate:?}: got={:?}, expected={expected:?}",
                            ids(&out)
                        ));
                    }
                    #[cfg(feature = "datafusion")]
                    {
                        use crate::datafusion::{DeltaTableProvider, ScanOptions};
                        use datafusion::prelude::{SessionConfig, SessionContext, col, lit};
                        for use_arrow_view_types in [false, true] {
                            let expression = match &predicate {
                                DeltaPredicate::Compare { op, .. } => match op {
                                    DeltaComparison::Eq => col("v").eq(lit(150.0)),
                                    DeltaComparison::NotEq => col("v").not_eq(lit(150.0)),
                                    DeltaComparison::Lt => col("v").lt(lit(150.0)),
                                    DeltaComparison::LtEq => col("v").lt_eq(lit(150.0)),
                                    DeltaComparison::Gt => col("v").gt(lit(150.0)),
                                    DeltaComparison::GtEq => col("v").gt_eq(lit(150.0)),
                                },
                                DeltaPredicate::IsNull { .. } => col("v").is_null(),
                                _ => col("v").is_not_null(),
                            };
                            let provider = Arc::new(DeltaTableProvider::try_new(
                                table.clone(),
                                ScanOptions {
                                    use_arrow_view_types,
                                    ..Default::default()
                                },
                            )?);
                            let ctx = SessionContext::new_with_config(
                                SessionConfig::new().with_target_partitions(1),
                            );
                            let out = ctx
                                .read_table(provider)?
                                .filter(expression)?
                                .select_columns(&["id"])?
                                .collect()
                                .await?;
                            if ids(&out) != expected {
                                failures.push(format!("{label}, views={use_arrow_view_types}: {predicate:?}: got={:?}, expected={expected:?}", ids(&out)));
                            }
                        }
                    }
                }
            }
        }
    }
    assert!(
        failures.is_empty(),
        "field matching failures:\n{}",
        failures.join("\n")
    );
    Ok(())
}

#[test]
fn typed_statistics_duplicate_names_do_not_hide_cast_nulls() -> TestResult {
    // Both columns have zero physical nulls. The second column's milliseconds
    // overflow when widened to microseconds, so IS NULL must retain its group.
    // Looking up the first column's type would incorrectly make the cast look
    // like an identity conversion and trust the physical null count.
    let schema = Arc::new(SchemaDescriptor::new(Arc::new(parse_message_type(
        "message m {
            OPTIONAL INT64 old_v (TIMESTAMP(MICROS,false)) = 1;
            OPTIONAL INT64 old_v (TIMESTAMP(MILLIS,false)) = 2;
        }",
    )?)));
    let columns = [0, i64::MAX]
        .into_iter()
        .enumerate()
        .map(|(index, value)| {
            ColumnChunkMetaData::builder(schema.column(index))
                .set_num_values(1)
                .set_statistics(Statistics::int64(
                    Some(value),
                    Some(value),
                    None,
                    Some(0),
                    false,
                ))
                .build()
        })
        .collect::<Result<Vec<_>, _>>()?;
    let group = RowGroupMetaData::builder(Arc::clone(&schema))
        .set_num_rows(1)
        .set_column_metadata(columns)
        .build()?;
    let metadata = ParquetMetaData::new(
        FileMetaData::new(1, 1, None, None, schema, None),
        vec![group],
    );
    let field = Field::new("v", ArrowType::Timestamp(TimeUnit::Microsecond, None), true)
        .with_metadata(
            [(
                parquet::arrow::PARQUET_FIELD_ID_META_KEY.to_owned(),
                "2".to_owned(),
            )]
            .into(),
        );
    let target = Arc::new(Schema::new(vec![field]));
    let predicate = DeltaKernelPredicate::from_test_predicate(Predicate::is_null(
        Expression::Column(ColumnName::new(["v"])),
    ));
    assert_eq!(
        pruned_row_groups(&metadata, &target, 1, None, Some(&predicate))?,
        Some(vec![0])
    );
    Ok(())
}

#[test]
fn typed_statistics_name_fallback_and_null_fills_use_the_alignment() -> TestResult {
    let case = Case {
        name: "name fallback with a missing target field".into(),
        source: Arc::new(Int32Array::from(vec![Some(-200), Some(0), Some(200), None])),
        target: ArrowType::Float64,
    };
    let (bytes, metadata) = parquet_file(&case, 1, EnabledStatistics::Chunk)?;
    // The file has no field IDs. The matched field must fall back to its name,
    // while the leading missing field receives NULLs and has no file statistics.
    let target_schema = Arc::new(Schema::new(vec![
        Field::new("added", ArrowType::Float64, true),
        Field::new("v", ArrowType::Float64, true).with_metadata(
            [(
                parquet::arrow::PARQUET_FIELD_ID_META_KEY.to_owned(),
                "42".to_owned(),
            )]
            .into(),
        ),
    ]));
    let v = Expression::Column(ColumnName::new(["v"]));
    let added = Expression::Column(ColumnName::new(["added"]));
    for (expression, expected) in [
        (
            Predicate::gt(v.clone(), Expression::Literal(KernelScalar::Double(150.0))),
            vec![2],
        ),
        (Predicate::is_null(v), vec![3]),
        (Predicate::is_null(added.clone()), vec![0, 1, 2, 3]),
        (Predicate::is_not_null(added), vec![0, 1, 2, 3]),
    ] {
        let predicate = DeltaKernelPredicate::from_test_predicate(expression);
        assert_eq!(
            pruned_row_groups(
                &metadata,
                &target_schema,
                bytes.len() as u64,
                None,
                Some(&predicate)
            )?,
            Some(expected)
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_statistics_date32_overflow_matches_checked_values_in_public_scans() -> TestResult {
    let limit = i32::try_from(i64::MAX / 86_400_000_000)?;
    let days = vec![
        Some(-1),
        Some(120_000_000),
        Some(0),
        Some(limit - 1),
        Some(limit),
        Some(limit + 1),
        Some(-limit - 1),
        Some(-limit),
        Some(-limit + 1),
        Some(i32::MIN),
        Some(i32::MIN + 1),
        Some(i32::MAX),
        None,
        None,
        None,
        Some(-719_162),
        Some(0),
        Some(2_932_896),
    ];
    let converted = Arc::new(TimestampMicrosecondArray::from_iter(days.iter().map(
        |day| day.and_then(|day| i64::try_from(i128::from(day) * 86_400_000_000).ok()),
    ))) as ArrayRef;
    let case = Case {
        name: "checked Date32 overflow".into(),
        source: Arc::new(Date32Array::from(days)),
        target: ArrowType::Timestamp(TimeUnit::Microsecond, None),
    };
    for mapped in [false, true] {
        let (_root, table, values) =
            public_table_with_converted_values(&case, mapped, Arc::clone(&converted)).await?;
        let full = table
            .scan()
            .with_target_partitions(1)?
            .build()
            .await?
            .into_stream()
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(
            ids(&full),
            (0..values.len()).map(|i| i as i32).collect::<Vec<_>>()
        );
        for batch in &full {
            let row_ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                assert_eq!(
                    extract_primitive_scalar(batch.column(1).as_ref(), i)?,
                    extract_primitive_scalar(values.as_ref(), row_ids.value(i) as usize)?,
                    "mapped={mapped}, row={}",
                    row_ids.value(i)
                );
            }
        }
        let mut predicates = Vec::new();
        for value in [-2_000_000_000_000, 0, 2_000_000_000_000] {
            for op in COMPARISONS {
                let mask = matching_rows(
                    &values,
                    Arc::new(TimestampMicrosecondArray::from(vec![value])),
                    op,
                )?;
                predicates.push((
                    DeltaPredicate::Compare {
                        column: "v".into(),
                        op,
                        value: DeltaScalar::TimestampMicrosecond {
                            value,
                            timezone: None,
                        },
                    },
                    mask,
                ));
            }
        }
        predicates.push((
            DeltaPredicate::IsNull { column: "v".into() },
            arrow::compute::is_null(values.as_ref())?,
        ));
        predicates.push((
            DeltaPredicate::IsNotNull { column: "v".into() },
            arrow::compute::is_not_null(values.as_ref())?,
        ));
        for (predicate, mask) in predicates {
            let expected = expected_ids(&mask);
            let out = table
                .scan()
                .with_target_partitions(1)?
                .with_projection(["id"])
                .with_predicate(predicate.clone())
                .build()
                .await?
                .into_stream()
                .try_collect::<Vec<_>>()
                .await?;
            assert_eq!(ids(&out), expected, "mapped={mapped}, {predicate:?}");
            #[cfg(feature = "datafusion")]
            {
                use crate::datafusion::{DeltaTableProvider, ScanOptions};
                use datafusion::{
                    common::ScalarValue,
                    prelude::{SessionConfig, SessionContext, col, lit},
                };
                let expression = match &predicate {
                    DeltaPredicate::Compare {
                        op,
                        value: DeltaScalar::TimestampMicrosecond { value, .. },
                        ..
                    } => {
                        let literal = lit(ScalarValue::TimestampMicrosecond(Some(*value), None));
                        match op {
                            DeltaComparison::Eq => col("v").eq(literal),
                            DeltaComparison::NotEq => col("v").not_eq(literal),
                            DeltaComparison::Lt => col("v").lt(literal),
                            DeltaComparison::LtEq => col("v").lt_eq(literal),
                            DeltaComparison::Gt => col("v").gt(literal),
                            DeltaComparison::GtEq => col("v").gt_eq(literal),
                        }
                    }
                    DeltaPredicate::IsNull { .. } => col("v").is_null(),
                    DeltaPredicate::IsNotNull { .. } => col("v").is_not_null(),
                    _ => unreachable!("this test only uses timestamp comparisons and null checks"),
                };
                for use_arrow_view_types in [false, true] {
                    let provider = Arc::new(DeltaTableProvider::try_new(
                        table.clone(),
                        ScanOptions {
                            use_arrow_view_types,
                            ..Default::default()
                        },
                    )?);
                    let ctx = SessionContext::new_with_config(
                        SessionConfig::new().with_target_partitions(1),
                    );
                    let out = ctx
                        .read_table(provider)?
                        .filter(expression.clone())?
                        .select_columns(&["id"])?
                        .collect()
                        .await?;
                    assert_eq!(
                        ids(&out),
                        expected,
                        "mapped={mapped}, views={use_arrow_view_types}, {predicate:?}"
                    );
                }
            }
        }
    }
    Ok(())
}
