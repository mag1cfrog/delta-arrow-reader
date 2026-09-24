//! NaN pruning regressions for #120. Arrow comparisons on unfiltered values are
//! the oracle; row identities detect missing, duplicated, or substituted rows.
//! Real footers cover NaN signs/payloads, infinities, signed zeros, nulls, and
//! float widening. Partial footers separately exercise each missing statistic.

#![allow(clippy::unwrap_used)]

use std::{collections::BTreeSet, error::Error, fs, sync::Arc};

use arrow::{
    array::{Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array},
    compute::{
        cast, concat,
        kernels::boolean::{and_kleene, is_not_null, is_null, not, or_kleene},
    },
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use bytes::Bytes;
use futures_util::TryStreamExt;
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    file::{
        metadata::ParquetMetaData,
        properties::{EnabledStatistics, WriterProperties},
        statistics::Statistics,
    },
};
use serde_json::json;

use super::{
    super::{
        nan_counts::{
            NanCounts,
            tests::{CountField, footer, with_nan_counts},
        },
        tests::TestDir,
    },
    typed_statistics_tests::{
        COMPARISONS, expected_ids, ids, matching_rows, metadata_action,
        pruned_row_groups_with_nan_counts,
    },
};
use crate::{
    DeltaComparison, DeltaPredicate, DeltaScalar, DeltaTable, DeltaTableBuilder,
    delta::kernel::kernel_pruning_predicate,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

const TYPES: [(DataType, DataType); 3] = [
    (DataType::Float32, DataType::Float32),
    (DataType::Float64, DataType::Float64),
    (DataType::Float32, DataType::Float64),
];

fn groups(source: &DataType) -> Vec<ArrayRef> {
    // Construct NaNs at the source width so casts cannot erase the signaling
    // bit or payload before the file is written.
    macro_rules! groups {
        ($array:ident, $float:ty, $nan:expr, $payload:expr, $signaling:expr) => {{
            let pos = <$float>::from_bits($nan);
            let payload = <$float>::from_bits($payload);
            let signaling = <$float>::from_bits($signaling);
            [
                vec![Some(1.5), Some(1.5)],
                vec![Some(1.5), Some(pos)],
                vec![Some(1.5), Some(-pos)],
                vec![Some(1.5), Some(payload), None],
                vec![Some(1.5), Some(-payload), None],
                vec![Some(1.5), Some(signaling), Some(-signaling)],
                vec![Some(pos), Some(payload), Some(signaling)],
                vec![Some(-pos), Some(-payload), Some(-signaling)],
                vec![Some(pos), Some(-pos), None],
                vec![None, None],
                vec![Some(-0.0), Some(0.0), None],
                vec![Some(-0.0), Some(pos)],
                vec![Some(0.0), Some(-pos)],
                vec![Some(<$float>::NEG_INFINITY), Some(<$float>::INFINITY)],
                vec![Some(<$float>::INFINITY), Some(pos)],
                vec![Some(<$float>::NEG_INFINITY), Some(-pos)],
                vec![Some(-100.0), Some(-1.5), Some(1.5), Some(100.0), None],
                vec![Some(-<$float>::MAX), Some(<$float>::MAX)],
                vec![Some(-<$float>::from_bits(1)), Some(<$float>::from_bits(1))],
            ]
            .into_iter()
            .map(|values| Arc::new($array::from(values)) as ArrayRef)
            .collect()
        }};
    }
    match source {
        DataType::Float32 => groups!(Float32Array, f32, 0x7fc0_0000, 0x7fc1_2345, 0x7f80_0001),
        DataType::Float64 => groups!(
            Float64Array,
            f64,
            0x7ff8_0000_0000_0000,
            0x7ff8_1234_5678_9abc,
            0x7ff0_0000_0000_0001
        ),
        _ => unreachable!("only floating-point sources"),
    }
}

struct Fixture {
    bytes: Bytes,
    metadata: Arc<ParquetMetaData>,
    schema: Arc<Schema>,
    values: ArrayRef,
    row_groups: Vec<usize>,
}

impl Fixture {
    fn new(source: &DataType, target: &DataType, stats: EnabledStatistics) -> TestResult<Self> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("v", source.clone(), true),
        ]));
        let mut bytes = Vec::new();
        let props = WriterProperties::builder()
            .set_statistics_enabled(stats)
            .set_data_page_row_count_limit(2)
            .set_write_batch_size(2)
            .build();
        let mut writer = ArrowWriter::try_new(&mut bytes, schema.clone(), Some(props))?;
        let groups = groups(source);
        let mut row_groups = Vec::new();
        for (ordinal, values) in groups.iter().enumerate() {
            let first = row_groups.len() as i32;
            writer.write(&RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from_iter_values(
                        first..first + values.len() as i32,
                    )),
                    values.clone(),
                ],
            )?)?;
            writer.flush()?;
            row_groups.extend(std::iter::repeat_n(ordinal, values.len()));
        }
        writer.close()?;
        let bytes = Bytes::from(bytes);
        let metadata = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())?
            .metadata()
            .clone();
        assert_eq!(metadata.num_row_groups(), groups.len());
        let values = concat(&groups.iter().map(|a| a.as_ref()).collect::<Vec<_>>())?;
        let values = cast(values.as_ref(), target)?;
        Ok(Self {
            bytes,
            metadata,
            schema: Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("v", target.clone(), true),
            ])),
            values,
            row_groups,
        })
    }

    fn selected(&self, predicate: &DeltaPredicate) -> TestResult<Vec<usize>> {
        Ok(pruned_row_groups_with_nan_counts(
            &self.metadata,
            &NanCounts::decode(footer(&self.bytes), &self.metadata),
            &self.schema,
            self.bytes.len() as u64,
            None,
            kernel_pruning_predicate(predicate).as_ref(),
        )?
        .unwrap_or_else(|| (0..self.metadata.num_row_groups()).collect()))
    }

    fn with_counts(mut self, mode: &str) -> TestResult<Self> {
        if mode == "missing" {
            return Ok(self);
        }
        let mut counts = vec![0_i64; self.metadata.num_row_groups()];
        for (index, group) in self.row_groups.iter().enumerate() {
            let nan = match self.values.data_type() {
                DataType::Float32 => self
                    .values
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .unwrap()
                    .value(index)
                    .is_nan(),
                _ => self
                    .values
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(index)
                    .is_nan(),
            };
            counts[*group] += i64::from(self.values.is_valid(index) && nan);
        }
        let counts = counts
            .into_iter()
            .enumerate()
            .map(|(group, count)| {
                vec![
                    CountField::Missing,
                    if mode == "mixed" && group % 3 == 0 {
                        CountField::Missing
                    } else {
                        CountField::Count(count)
                    },
                ]
            })
            .collect::<Vec<_>>();
        self.bytes = with_nan_counts(&self.bytes, &counts);
        self.metadata = ParquetRecordBatchReaderBuilder::try_new(self.bytes.clone())?
            .metadata()
            .clone();
        Ok(self)
    }

    async fn table(&self) -> TestResult<(TestDir, DeltaTable)> {
        let root = TestDir::new("nan-statistics")?;
        fs::create_dir_all(root.path().join("_delta_log"))?;
        fs::write(root.path().join("part.parquet"), &self.bytes)?;
        let source = match self.metadata.row_group(0).column(1).column_type() {
            parquet::basic::Type::FLOAT => "float",
            _ => "double",
        };
        let target = match self.values.data_type() {
            DataType::Float32 => "float",
            _ => "double",
        };
        let protocol = json!({"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["typeWidening"],"writerFeatures":["typeWidening"]}});
        // Omit add-action statistics to isolate footer pruning.
        let add = json!({"add":{"path":"part.parquet","partitionValues":{},"size":self.bytes.len(),"modificationTime":0,"dataChange":true}});
        fs::write(
            root.path().join("_delta_log/00000000000000000000.json"),
            format!(
                "{protocol}\n{}\n{add}\n",
                metadata_action(source, false, None)
            ),
        )?;
        if source != target {
            fs::write(
                root.path().join("_delta_log/00000000000000000001.json"),
                format!("{}\n", metadata_action(target, false, Some(source))),
            )?;
        }
        let table = DeltaTableBuilder::new(root.path().to_string_lossy())
            .load_table()
            .await?;
        Ok((root, table))
    }
}

fn compare(data_type: &DataType, op: DeltaComparison, value: f64) -> DeltaPredicate {
    DeltaPredicate::Compare {
        column: "v".into(),
        op,
        value: match data_type {
            DataType::Float32 => DeltaScalar::Float32(value as f32),
            _ => DeltaScalar::Float64(value),
        },
    }
}

fn predicates(values: &ArrayRef) -> TestResult<Vec<(DeltaPredicate, BooleanArray)>> {
    let mut result = Vec::new();
    for value in [
        -100.0,
        -1.5,
        -0.0,
        0.0,
        f32::from_bits(1) as f64,
        1.5,
        100.0,
        f32::MAX as f64,
    ] {
        let literal = cast(&Float64Array::from(vec![value]), values.data_type())?;
        for op in COMPARISONS {
            let predicate = compare(values.data_type(), op, value);
            let mask = matching_rows(values, literal.clone(), op)?;
            result.push((
                DeltaPredicate::Not(Box::new(predicate.clone())),
                not(&mask)?,
            ));
            result.push((predicate, mask));
        }
    }
    let lower = compare(values.data_type(), DeltaComparison::Gt, 100.0);
    let upper = compare(values.data_type(), DeltaComparison::Lt, -100.0);
    let low_mask = matching_rows(
        values,
        cast(&Float64Array::from(vec![100.0]), values.data_type())?,
        DeltaComparison::Gt,
    )?;
    let high_mask = matching_rows(
        values,
        cast(&Float64Array::from(vec![-100.0]), values.data_type())?,
        DeltaComparison::Lt,
    )?;
    let null = DeltaPredicate::IsNull { column: "v".into() };
    let nonnull = DeltaPredicate::IsNotNull { column: "v".into() };
    let integer = DeltaPredicate::Compare {
        column: "id".into(),
        op: DeltaComparison::Lt,
        value: DeltaScalar::Int32(3),
    };
    let integer_mask = BooleanArray::from_iter((0..values.len()).map(|i| Some(i < 3)));
    for (predicate, mask) in [
        (null.clone(), is_null(values.as_ref())?),
        (nonnull.clone(), is_not_null(values.as_ref())?),
        (
            DeltaPredicate::And(vec![lower.clone(), upper.clone()]),
            and_kleene(&low_mask, &high_mask)?,
        ),
        (
            DeltaPredicate::Or(vec![lower.clone(), upper.clone()]),
            or_kleene(&low_mask, &high_mask)?,
        ),
        (
            DeltaPredicate::Or(vec![lower.clone(), null]),
            or_kleene(&low_mask, &is_null(values.as_ref())?)?,
        ),
        (
            DeltaPredicate::And(vec![lower.clone(), nonnull]),
            and_kleene(&low_mask, &is_not_null(values.as_ref())?)?,
        ),
        (
            DeltaPredicate::And(vec![lower.clone(), integer.clone()]),
            and_kleene(&low_mask, &integer_mask)?,
        ),
        (
            DeltaPredicate::Or(vec![lower, integer]),
            or_kleene(&low_mask, &integer_mask)?,
        ),
    ] {
        result.push((
            DeltaPredicate::Not(Box::new(predicate.clone())),
            not(&mask)?,
        ));
        result.push((predicate, mask));
    }
    Ok(result)
}

fn partial_metadata(metadata: &ParquetMetaData, mode: &str) -> TestResult<ParquetMetaData> {
    let groups = metadata
        .row_groups()
        .iter()
        .map(|group| {
            let mut columns = group.columns().to_vec();
            let stats = columns[1].statistics().unwrap();
            macro_rules! partial {
                ($stats:expr, $constructor:ident) => {
                    Statistics::$constructor(
                        (mode != "max-only" && mode != "null-only")
                            .then(|| $stats.min_opt().copied())
                            .flatten(),
                        (mode != "min-only" && mode != "null-only")
                            .then(|| $stats.max_opt().copied())
                            .flatten(),
                        None,
                        if mode == "no-null-count" {
                            None
                        } else {
                            $stats.null_count_opt()
                        },
                        false,
                    )
                };
            }
            let stats = match stats {
                Statistics::Float(stats) => partial!(stats, float),
                Statistics::Double(stats) => partial!(stats, double),
                _ => unreachable!("floating column"),
            };
            columns[1] = columns[1]
                .clone()
                .into_builder()
                .set_statistics(stats)
                .build()?;
            group
                .clone()
                .into_builder()
                .set_column_metadata(columns)
                .build()
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ParquetMetaData::new(
        metadata.file_metadata().clone(),
        groups,
    ))
}

#[test]
fn floating_statistics_all_comparisons_and_partial_bounds_preserve_matches() -> TestResult {
    let mut failures = Vec::new();
    let mut checked = 0;
    for (source, target) in TYPES {
        for (stats, count_mode) in [
            EnabledStatistics::Chunk,
            EnabledStatistics::Page,
            EnabledStatistics::None,
        ]
        .into_iter()
        .flat_map(|stats| ["missing", "recorded", "mixed"].map(|mode| (stats, mode)))
        {
            let mut fixture = Fixture::new(&source, &target, stats)?.with_counts(count_mode)?;
            let original = fixture.metadata.clone();
            for mode in [
                "complete",
                "min-only",
                "max-only",
                "null-only",
                "no-null-count",
            ] {
                if mode != "complete" && stats == EnabledStatistics::None {
                    continue;
                }
                fixture.metadata = if mode == "complete" {
                    original.clone()
                } else {
                    Arc::new(partial_metadata(&original, mode)?)
                };
                for (predicate, mask) in predicates(&fixture.values)? {
                    let selected = fixture.selected(&predicate)?;
                    let expected = expected_ids(&mask)
                        .into_iter()
                        .map(|i| fixture.row_groups[i as usize])
                        .collect::<BTreeSet<_>>();
                    if expected.iter().any(|group| !selected.contains(group)) {
                        failures.push(format!("{source:?}->{target:?}, {stats:?}/{mode}, counts={count_mode}, {predicate:?}: selected={selected:?}, matching={expected:?}"));
                    }
                    if stats == EnabledStatistics::None {
                        assert_eq!(
                            selected.len(),
                            original.num_row_groups(),
                            "missing statistics"
                        );
                    }
                    checked += 1;
                }
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{}/{} pruning cases lost matches:\n{}",
        failures.len(),
        checked,
        failures
            .iter()
            .take(20)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    );
    eprintln!("floating statistics: {checked} pruning comparisons");
    Ok(())
}

#[test]
fn floating_statistics_keep_equality_null_and_other_column_pruning() -> TestResult {
    for (source, target) in TYPES {
        let mut fixture = Fixture::new(&source, &target, EnabledStatistics::Chunk)?;
        // The first two groups have indistinguishable counts and bounds, but
        // only the second contains a NaN. Both must remain candidates for >.
        assert_eq!(
            fixture.metadata.row_group(0).column(1).statistics(),
            fixture.metadata.row_group(1).column(1).statistics()
        );
        let selected = fixture.selected(&compare(&target, DeltaComparison::Gt, 100.0))?;
        assert!(selected.contains(&0) && selected.contains(&1));
        let selected = fixture.selected(&compare(&target, DeltaComparison::Eq, 100.0))?;
        assert!(
            !selected.contains(&0) && !selected.contains(&1),
            "finite equality should still prune"
        );
        let original = fixture.metadata.clone();
        for (mode, literal, should_prune) in [
            ("min-only", -100.0, true),
            ("max-only", 100.0, true),
            ("no-null-count", 100.0, true),
            ("null-only", 100.0, false),
        ] {
            fixture.metadata = Arc::new(partial_metadata(&original, mode)?);
            let selected = fixture.selected(&compare(&target, DeltaComparison::Eq, literal))?;
            for group in [0, 1] {
                assert_eq!(!selected.contains(&group), should_prune, "{mode}");
            }
        }
        fixture.metadata = original;
        let equal = compare(&target, DeltaComparison::Eq, 100.0);
        assert_eq!(
            fixture.selected(&DeltaPredicate::Not(Box::new(DeltaPredicate::Not(
                Box::new(equal.clone())
            ))))?,
            fixture.selected(&equal)?
        );
        assert_eq!(
            fixture.selected(&DeltaPredicate::IsNull { column: "v".into() })?,
            vec![3, 4, 8, 9, 10, 16]
        );
        let nonnull = fixture.selected(&DeltaPredicate::IsNotNull { column: "v".into() })?;
        assert_eq!(nonnull, (0..19).filter(|i| *i != 9).collect::<Vec<_>>());
        let impossible_id = DeltaPredicate::Compare {
            column: "id".into(),
            op: DeltaComparison::Gt,
            value: DeltaScalar::Int32(i32::MAX),
        };
        let nan_sensitive = compare(&target, DeltaComparison::NotEq, 1.5);
        assert!(
            fixture
                .selected(&DeltaPredicate::And(vec![
                    nan_sensitive.clone(),
                    impossible_id.clone()
                ]))?
                .is_empty()
        );
        assert_eq!(
            fixture.selected(&DeltaPredicate::Or(vec![
                nan_sensitive.clone(),
                impossible_id
            ]))?,
            fixture.selected(&nan_sensitive)?
        );
    }
    Ok(())
}

#[test]
fn floating_statistics_zero_nan_count_restores_range_and_inequality_pruning() -> TestResult {
    for (source, target) in TYPES {
        let fixture =
            Fixture::new(&source, &target, EnabledStatistics::Page)?.with_counts("recorded")?;
        for (op, value) in [
            (DeltaComparison::Gt, 100.0),
            (DeltaComparison::GtEq, 100.0),
            (DeltaComparison::Lt, -100.0),
            (DeltaComparison::LtEq, -100.0),
            (DeltaComparison::NotEq, 1.5),
        ] {
            let selected = fixture.selected(&compare(&target, op, value))?;
            assert!(
                !selected.contains(&0),
                "NaN-free group must be pruned: {source:?}->{target:?}, {op:?}"
            );
            assert!(
                selected.contains(&1) && selected.contains(&2),
                "groups with either NaN sign must remain candidates"
            );
        }
        let selected = fixture.selected(&DeltaPredicate::Not(Box::new(compare(
            &target,
            DeltaComparison::Eq,
            1.5,
        ))))?;
        assert!(!selected.contains(&0) && selected.contains(&1) && selected.contains(&2));
    }
    Ok(())
}

async fn unfiltered_values(table: &DeltaTable, fixture: &Fixture) -> TestResult<ArrayRef> {
    let batches = table
        .scan()
        .with_target_partitions(1)?
        .build()
        .await?
        .into_stream()
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(
        ids(&batches),
        (0..fixture.values.len() as i32).collect::<Vec<_>>()
    );
    let values = concat(
        &batches
            .iter()
            .map(|b| b.column(1).as_ref())
            .collect::<Vec<_>>(),
    )?;
    // Arrow array equality checks floating-point bits, including NaN payloads.
    assert_eq!(values.to_data(), fixture.values.to_data());
    Ok(values)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn floating_statistics_public_streaming_matches_unfiltered_arrow() -> TestResult {
    let mut failures = Vec::new();
    let mut checked = 0;
    for (source, target) in TYPES {
        for (stats, count_mode) in [
            EnabledStatistics::Chunk,
            EnabledStatistics::Page,
            EnabledStatistics::None,
        ]
        .into_iter()
        .flat_map(|stats| ["missing", "recorded", "mixed"].map(|mode| (stats, mode)))
        {
            let fixture = Fixture::new(&source, &target, stats)?.with_counts(count_mode)?;
            let (_root, table) = fixture.table().await?;
            let values = unfiltered_values(&table, &fixture).await?;
            for (predicate, mask) in predicates(&values)? {
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
                let (actual, expected) = (ids(&output), expected_ids(&mask));
                if actual != expected {
                    failures.push(format!("{source:?}->{target:?}, {stats:?}, counts={count_mode}, {predicate:?}: actual={actual:?}, expected={expected:?}"));
                }
                checked += 1;
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{}/{} streaming scans disagreed with Arrow:\n{}",
        failures.len(),
        checked,
        failures
            .iter()
            .take(20)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    );
    eprintln!("floating statistics: {checked} public streaming scans");
    Ok(())
}

#[cfg(feature = "datafusion")]
fn datafusion_predicate(predicate: &DeltaPredicate) -> datafusion::logical_expr::Expr {
    use datafusion::{
        common::ScalarValue,
        logical_expr::Expr,
        prelude::{col, lit},
    };
    match predicate {
        DeltaPredicate::Compare { column, op, value } => {
            let value = lit(match value {
                DeltaScalar::Float32(v) => ScalarValue::Float32(Some(*v)),
                DeltaScalar::Float64(v) => ScalarValue::Float64(Some(*v)),
                DeltaScalar::Int32(v) => ScalarValue::Int32(Some(*v)),
                _ => unreachable!("test literals"),
            });
            match op {
                DeltaComparison::Eq => col(column).eq(value),
                DeltaComparison::NotEq => col(column).not_eq(value),
                DeltaComparison::Lt => col(column).lt(value),
                DeltaComparison::LtEq => col(column).lt_eq(value),
                DeltaComparison::Gt => col(column).gt(value),
                DeltaComparison::GtEq => col(column).gt_eq(value),
            }
        }
        DeltaPredicate::Not(p) => !datafusion_predicate(p),
        DeltaPredicate::IsNull { column } => col(column).is_null(),
        DeltaPredicate::IsNotNull { column } => col(column).is_not_null(),
        DeltaPredicate::And(predicates) => predicates
            .iter()
            .map(datafusion_predicate)
            .reduce(Expr::and)
            .unwrap(),
        DeltaPredicate::Or(predicates) => predicates
            .iter()
            .map(datafusion_predicate)
            .reduce(Expr::or)
            .unwrap(),
        DeltaPredicate::Constant(value) => lit(*value),
    }
}

#[cfg(feature = "datafusion")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn floating_statistics_public_datafusion_matches_arrow_with_both_view_settings() -> TestResult
{
    use crate::datafusion::{DeltaTableProvider, ScanOptions};
    use datafusion::prelude::{SessionConfig, SessionContext};
    let mut failures = Vec::new();
    let mut checked = 0;
    for (source, target) in TYPES {
        for (stats, count_mode) in [
            EnabledStatistics::Chunk,
            EnabledStatistics::Page,
            EnabledStatistics::None,
        ]
        .into_iter()
        .flat_map(|stats| ["missing", "recorded", "mixed"].map(|mode| (stats, mode)))
        {
            let fixture = Fixture::new(&source, &target, stats)?.with_counts(count_mode)?;
            let (_root, table) = fixture.table().await?;
            let values = unfiltered_values(&table, &fixture).await?;
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
                for (predicate, mask) in predicates(&values)? {
                    let output = context
                        .read_table(provider.clone())?
                        .filter(datafusion_predicate(&predicate))?
                        .select_columns(&["id"])?
                        .collect()
                        .await?;
                    let (actual, expected) = (ids(&output), expected_ids(&mask));
                    if actual != expected {
                        failures.push(format!("{source:?}->{target:?}, {stats:?}, counts={count_mode}, views={use_arrow_view_types}, {predicate:?}: actual={actual:?}, expected={expected:?}"));
                    }
                    checked += 1;
                }
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{}/{} DataFusion scans disagreed with Arrow:\n{}",
        failures.len(),
        checked,
        failures
            .iter()
            .take(20)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    );
    eprintln!("floating statistics: {checked} public DataFusion scans");
    Ok(())
}
