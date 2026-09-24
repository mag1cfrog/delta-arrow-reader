//! Test-only footer editing uses Thrift's encoder, independently of the count
//! reader. It leaves data pages and their offsets unchanged.

#![allow(clippy::unwrap_used)]

use bytes::Bytes;
use parquet::file::{
    FOOTER_SIZE,
    metadata::{FooterTail, ParquetMetaData, ParquetMetaDataReader},
};
use thrift::protocol::{
    TCompactInputProtocol, TCompactOutputProtocol, TFieldIdentifier, TInputProtocol,
    TListIdentifier, TOutputProtocol, TStructIdentifier, TType,
};

use super::NanCounts;

#[derive(Clone, Debug)]
pub(crate) enum CountField {
    Missing,
    Count(i64),
    Encoded(Vec<u8>),
    WrongType,
    Duplicate(i64, i64),
}

#[derive(Debug)]
enum Value {
    Bool(bool),
    I16(i16),
    I32(i32),
    I64(i64),
    Raw(TType, Vec<u8>),
    Double(f64),
    Bytes(Vec<u8>),
    Struct(Vec<(i16, Self)>),
    List(TType, Vec<Self>),
}

impl Value {
    fn read(p: &mut impl TInputProtocol, kind: TType) -> thrift::Result<Self> {
        Ok(match kind {
            TType::Bool => Self::Bool(p.read_bool()?),
            TType::I16 => Self::I16(p.read_i16()?),
            TType::I32 => Self::I32(p.read_i32()?),
            TType::I64 => Self::I64(p.read_i64()?),
            TType::Double => Self::Double(p.read_double()?),
            TType::String => Self::Bytes(p.read_bytes()?),
            TType::Struct => {
                p.read_struct_begin()?;
                let mut fields = Vec::new();
                loop {
                    let field = p.read_field_begin()?;
                    if field.field_type == TType::Stop {
                        break;
                    }
                    fields.push((field.id.unwrap(), Self::read(p, field.field_type)?));
                    p.read_field_end()?;
                }
                p.read_struct_end()?;
                Self::Struct(fields)
            }
            TType::List => {
                let list = p.read_list_begin()?;
                let values = (0..list.size)
                    .map(|_| Self::read(p, list.element_type))
                    .collect::<thrift::Result<Vec<_>>>()?;
                p.read_list_end()?;
                Self::List(list.element_type, values)
            }
            _ => unreachable!("not used in the test Parquet footers: {kind:?}"),
        })
    }

    fn kind(&self) -> TType {
        match self {
            Self::Bool(_) => TType::Bool,
            Self::I16(_) => TType::I16,
            Self::I32(_) => TType::I32,
            Self::I64(_) => TType::I64,
            Self::Raw(kind, _) => *kind,
            Self::Double(_) => TType::Double,
            Self::Bytes(_) => TType::String,
            Self::Struct(_) => TType::Struct,
            Self::List(_, _) => TType::List,
        }
    }

    fn write(&self, p: &mut impl TOutputProtocol) -> thrift::Result<()> {
        match self {
            Self::Bool(v) => p.write_bool(*v),
            Self::I16(v) => p.write_i16(*v),
            Self::I32(v) => p.write_i32(*v),
            Self::I64(v) => p.write_i64(*v),
            Self::Raw(_, bytes) => {
                for byte in bytes {
                    p.write_byte(*byte)?;
                }
                Ok(())
            }
            Self::Double(v) => p.write_double(*v),
            Self::Bytes(v) => p.write_bytes(v),
            Self::Struct(fields) => {
                p.write_struct_begin(&TStructIdentifier::new(""))?;
                for (id, value) in fields {
                    p.write_field_begin(&TFieldIdentifier::new("", value.kind(), *id))?;
                    value.write(p)?;
                    p.write_field_end()?;
                }
                p.write_field_stop()?;
                p.write_struct_end()
            }
            Self::List(kind, values) => {
                p.write_list_begin(&TListIdentifier::new(*kind, values.len() as i32))?;
                for value in values {
                    value.write(p)?;
                }
                p.write_list_end()
            }
        }
    }

    fn fields(&mut self) -> &mut Vec<(i16, Self)> {
        match self {
            Self::Struct(fields) => fields,
            _ => unreachable!("struct"),
        }
    }

    fn field(&mut self, id: i16) -> &mut Self {
        &mut self
            .fields()
            .iter_mut()
            .find(|(field, _)| *field == id)
            .unwrap()
            .1
    }

    fn list(&mut self) -> &mut Vec<Self> {
        match self {
            Self::List(_, values) => values,
            _ => unreachable!("list"),
        }
    }
}

pub(crate) fn footer(bytes: &[u8]) -> &[u8] {
    let end = bytes.len() - FOOTER_SIZE;
    let length = FooterTail::try_from(&bytes[end..])
        .unwrap()
        .metadata_length();
    &bytes[end - length..end]
}

pub(crate) fn with_nan_counts(bytes: &Bytes, counts: &[Vec<CountField>]) -> Bytes {
    rewrite(bytes, |metadata| {
        for (row, counts) in metadata.field(4).list().iter_mut().zip(counts) {
            for (column, count) in row.field(1).list().iter_mut().zip(counts) {
                let fields = column.field(3).fields();
                if !fields.iter().any(|(id, _)| *id == 12) {
                    fields.push((12, Value::Struct(Vec::new())));
                }
                let stats = fields
                    .iter_mut()
                    .find(|(id, _)| *id == 12)
                    .unwrap()
                    .1
                    .fields();
                stats.retain(|(id, _)| *id != 9);
                match count {
                    CountField::Missing => {}
                    CountField::Count(count) => stats.push((9, Value::I64(*count))),
                    CountField::Encoded(bytes) => {
                        stats.push((9, Value::Raw(TType::I64, bytes.clone())))
                    }
                    CountField::WrongType => stats.push((9, Value::Bytes(vec![0xff]))),
                    CountField::Duplicate(a, b) => {
                        stats.push((9, Value::I64(*a)));
                        stats.push((9, Value::I64(*b)));
                    }
                }
            }
        }
    })
}

fn rewrite(bytes: &Bytes, edit: impl FnOnce(&mut Value)) -> Bytes {
    let raw_footer = footer(bytes);
    let mut metadata =
        Value::read(&mut TCompactInputProtocol::new(raw_footer), TType::Struct).unwrap();
    edit(&mut metadata);
    let mut encoded = Vec::new();
    metadata
        .write(&mut TCompactOutputProtocol::new(&mut encoded))
        .unwrap();
    replace_footer(bytes, &encoded)
}

fn metadata(bytes: &Bytes) -> ParquetMetaData {
    ParquetMetaDataReader::new()
        .parse_and_finish(bytes)
        .unwrap()
}

fn sample() -> Bytes {
    use arrow::{
        array::{ArrayRef, Float32Array, Float64Array, Int32Array},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("f", DataType::Float32, true),
        Field::new("d", DataType::Float64, true),
    ]));
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema.clone(), None).unwrap();
    for i in 0..2 {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![i, i])),
            Arc::new(Float32Array::from(vec![
                Some(-1.5),
                if i == 0 { None } else { Some(f32::NAN) },
            ])),
            Arc::new(Float64Array::from(vec![
                Some(1.5),
                if i == 0 { Some(f64::NAN) } else { None },
            ])),
        ];
        writer
            .write(&RecordBatch::try_new(schema.clone(), columns).unwrap())
            .unwrap();
        writer.flush().unwrap();
    }
    writer.close().unwrap();
    Bytes::from(bytes)
}

fn sample_counts() -> [Vec<CountField>; 2] {
    [
        vec![
            CountField::Missing,
            CountField::Count(0),
            CountField::Count(1),
        ],
        vec![
            CountField::Missing,
            CountField::Count(1),
            CountField::Count(0),
        ],
    ]
}

#[test]
fn nan_counts_distinguish_missing_zero_positive_and_invalid_counts() {
    let bytes = sample();
    assert!(
        NanCounts::decode(footer(&bytes), &metadata(&bytes))
            .0
            .is_empty()
    );
    for (field, expected) in [
        (CountField::Missing, None),
        (CountField::Count(0), Some(0)),
        (CountField::Count(1), Some(1)),
        (CountField::Count(-1), None),
        (CountField::Count(i64::MAX), None),
        (CountField::WrongType, None),
        (CountField::Duplicate(0, 1), None),
        (CountField::Duplicate(1, 0), None),
        (CountField::Duplicate(0, 0), None),
    ] {
        let encoded = with_nan_counts(&bytes, &vec![vec![field.clone(); 3]; 2]);
        let native = metadata(&encoded);
        let counts = NanCounts::decode(footer(&encoded), &native);
        for row in 0..2 {
            assert_eq!(counts.get(row, 0), None, "non-floating column");
            assert_eq!(counts.get(row, 1), expected, "{field:?}");
            assert_eq!(counts.get(row, 2), expected, "{field:?}");
        }
    }
    // Count 2 fits num_values but not the non-null count of column f.
    let encoded = with_nan_counts(&bytes, &vec![vec![CountField::Count(2); 3]; 2]);
    let counts = NanCounts::decode(footer(&encoded), &metadata(&encoded));
    assert_eq!(counts.get(0, 1), None);
    assert_eq!(counts.get(0, 2), Some(2));
}

#[test]
fn nan_counts_follow_physical_ordinals_and_skip_binary_unknown_fields() {
    let bytes = with_nan_counts(&sample(), &sample_counts());
    let bytes = rewrite(&bytes, |m| {
        // Fields on both sides of row_groups exercise compact field-ID deltas.
        m.fields()
            .insert(0, (100, Value::Bytes(vec![0xff, 0, 0x80])));
        m.fields().push((
            101,
            Value::List(
                TType::Struct,
                vec![Value::Struct(vec![
                    (1, Value::Bytes(vec![0xfe; 300])),
                    (2, Value::Bool(false)),
                ])],
            ),
        ));
    });
    let native = metadata(&bytes);
    let counts = NanCounts::decode(footer(&bytes), &native);
    assert_eq!(counts.get(0, 1), Some(0));
    assert_eq!(counts.get(0, 2), Some(1));
    assert_eq!(counts.get(1, 1), Some(1));
    assert_eq!(counts.get(1, 2), Some(0));
    assert_eq!(counts.get(2, 1), None);
    assert_eq!(counts.get(0, 3), None);
}

#[test]
fn nan_counts_reject_truncation_and_structural_mismatches() {
    let bytes = with_nan_counts(&sample(), &vec![vec![CountField::Count(0); 3]; 2]);
    let native = metadata(&bytes);
    let raw = footer(&bytes);
    for len in 0..raw.len() {
        assert!(
            NanCounts::decode(&raw[..len], &native).0.is_empty(),
            "prefix {len}"
        );
    }
    for kind in [
        "row-count",
        "column-count",
        "duplicate-row-groups",
        "duplicate-columns",
        "duplicate-statistics",
    ] {
        let bytes = rewrite(&bytes, |m| match kind {
            "row-count" => {
                m.field(4).list().pop();
            }
            "column-count" => {
                m.field(4).list()[0].field(1).list().pop();
            }
            "duplicate-row-groups" => m.fields().push((4, Value::List(TType::Struct, Vec::new()))),
            "duplicate-columns" => m.field(4).list()[0]
                .fields()
                .push((1, Value::List(TType::Struct, Vec::new()))),
            _ => m.field(4).list()[0].field(1).list()[0]
                .field(3)
                .fields()
                .push((12, Value::Struct(Vec::new()))),
        });
        assert!(
            NanCounts::decode(footer(&bytes), &native).0.is_empty(),
            "{kind}"
        );
    }
}

#[tokio::test]
async fn nan_counts_reuse_footer_requests_page_indexes_and_cached_metadata()
-> Result<(), Box<dyn std::error::Error>> {
    use super::super::{
        ParquetMetadataCache, arrow_reader_options,
        tests::{TestDir, metrics, reader, task},
    };
    use crate::DeltaScanExecutionOptions;
    use parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader};
    use std::{fs, sync::Arc};

    let root = TestDir::new("nan-count-footer-io")?;
    let bytes = with_nan_counts(&sample(), &sample_counts());
    fs::write(root.path().join("part.parquet"), &bytes)?;
    let file_size = bytes.len() as u64;
    for hint in [None, Some(9), Some(bytes.len()), Some(bytes.len() + 128)] {
        for has_row_filter in [false, true] {
            let options = arrow_reader_options(false, has_row_filter)?;
            let mut snapshots = Vec::new();
            for compatibility in [false, true] {
                let metrics = metrics();
                let reader = reader(
                    &root,
                    DeltaScanExecutionOptions::new().with_parquet_metadata_size_hint_bytes(hint)?,
                    metrics.clone(),
                )?
                .with_metadata_cache(Arc::new(ParquetMetadataCache::default()));
                let object =
                    reader.resolve_parquet_object(&task("part.parquet", Some(file_size))?)?;
                let mut source = ParquetObjectReader::new(object.store, object.path.clone())
                    .with_file_size(file_size);
                if let Some(hint) = hint {
                    source = source.with_footer_size_hint(hint);
                }
                let parquet = if compatibility {
                    let loaded = reader
                        .load_parquet_metadata(&object.path, file_size, &mut source, &options)
                        .await?;
                    for row in 0..2 {
                        assert_eq!(loaded.nan_counts.get(row, 1), Some(row as u64));
                        assert_eq!(loaded.nan_counts.get(row, 2), Some(1 - row as u64));
                    }
                    let before = metrics.snapshot();
                    let cached = reader
                        .load_parquet_metadata(&object.path, file_size, &mut source, &options)
                        .await?;
                    assert!(Arc::ptr_eq(&loaded, &cached));
                    assert_eq!(
                        metrics.snapshot().parquet_data_file_range_get_operations,
                        before.parquet_data_file_range_get_operations
                    );
                    assert_eq!(
                        metrics.snapshot().parquet_data_file_bytes_received,
                        before.parquet_data_file_bytes_received
                    );
                    loaded.parquet.clone()
                } else {
                    source.get_metadata(Some(&options)).await?
                };
                assert_eq!(parquet.offset_index().is_some(), has_row_filter);
                snapshots.push(metrics.snapshot());
            }
            assert_eq!(
                snapshots[0].parquet_data_file_range_get_operations,
                snapshots[1].parquet_data_file_range_get_operations,
                "hint={hint:?}, indexes={has_row_filter}"
            );
            assert_eq!(
                snapshots[0].parquet_data_file_bytes_received,
                snapshots[1].parquet_data_file_bytes_received,
                "hint={hint:?}, indexes={has_row_filter}"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn nan_counts_reach_whole_file_and_cached_ranged_streams()
-> Result<(), Box<dyn std::error::Error>> {
    use super::super::{
        ParquetMetadataCache, PhysicalParquetStreamOptions,
        tests::{TestDir, metrics, reader, task},
    };
    use crate::{
        DeltaComparison, DeltaPredicate, DeltaScalar, DeltaScanExecutionOptions,
        delta::kernel::kernel_pruning_predicate,
    };
    use std::{fs, sync::Arc};

    let root = TestDir::new("nan-count-streams")?;
    let bytes = with_nan_counts(&sample(), &sample_counts());
    fs::write(root.path().join("part.parquet"), &bytes)?;
    let native = metadata(&bytes);
    let schema = Arc::new(parquet::arrow::parquet_to_arrow_schema(
        native.file_metadata().schema_descr(),
        native.file_metadata().key_value_metadata(),
    )?);
    let size = bytes.len() as u64;
    let second_group = native.row_group(1).column(0);
    let split = second_group
        .dictionary_page_offset()
        .unwrap_or_else(|| second_group.data_page_offset()) as u64;
    assert!(split > 0 && split < size);
    for cached in [false, true] {
        let mut reader = reader(&root, DeltaScanExecutionOptions::new(), metrics())?;
        if cached {
            reader = reader.with_metadata_cache(Arc::new(ParquetMetadataCache::default()));
        }
        for ranged in [false, true] {
            for (column, value, expected) in [
                ("f", DeltaScalar::Float32(100.0), vec![2_i64, 3]),
                ("d", DeltaScalar::Float64(100.0), vec![0_i64, 1]),
            ] {
                let predicate = kernel_pruning_predicate(&DeltaPredicate::Compare {
                    column: column.into(),
                    op: DeltaComparison::Gt,
                    value,
                });
                let ranges = if ranged {
                    vec![Some(0..split), Some(split..size)]
                } else {
                    vec![None]
                };
                let mut indexes = Vec::new();
                for range in ranges {
                    let mut task = task("part.parquet", Some(size))?;
                    task.parquet_byte_range = range;
                    let mut stream = reader
                        .open_physical_parquet_stream(
                            &task,
                            &schema,
                            PhysicalParquetStreamOptions {
                                row_group_predicate: predicate.as_ref(),
                                include_original_row_index: true,
                                ..Default::default()
                            },
                        )
                        .await?;
                    while let Some((_, original)) =
                        stream.next_batch_with_original_row_indexes().await?
                    {
                        indexes.extend_from_slice(original.unwrap().values());
                    }
                }
                // There is no row filter here: assert pruning itself, including
                // preserving original row positions across disjoint file ranges.
                assert_eq!(
                    indexes, expected,
                    "cached={cached}, ranged={ranged}, column={column}"
                );
            }
        }
    }
    Ok(())
}

fn overflowing_count() -> Vec<u8> {
    // 2^64 cannot fit the unsigned intermediate of an i64 ZigZag value.
    let mut bytes = vec![0x80; 9];
    bytes.push(0x02);
    bytes
}

#[test]
fn nan_counts_reject_overflowing_wire_integers() {
    let bytes = sample();
    for last in 2..=255 {
        let mut wire = vec![0x80; 9];
        wire.push(last);
        if last & 0x80 != 0 {
            wire.push(0);
        }
        let mut counts = sample_counts();
        counts[0][2] = CountField::Encoded(wire);
        let bytes = with_nan_counts(&bytes, &counts);
        // Native parquet-rs skips this unknown field, so the side reader must
        // reject its invalid encoding without relying on native validation.
        let native = metadata(&bytes);
        assert!(
            NanCounts::decode(footer(&bytes), &native).0.is_empty(),
            "invalid tenth byte {last:#x} must not establish a NaN-free column"
        );
    }
}

#[tokio::test]
async fn overflowing_count_preserves_public_scan_rows() -> Result<(), Box<dyn std::error::Error>> {
    let mut counts = sample_counts();
    counts[0][2] = CountField::Encoded(overflowing_count());
    assert_public_scan_preserves_nan(with_nan_counts(&sample(), &counts)).await
}

async fn assert_public_scan_preserves_nan(bytes: Bytes) -> Result<(), Box<dyn std::error::Error>> {
    use super::super::tests::TestDir;
    use crate::{DeltaComparison, DeltaPredicate, DeltaScalar, DeltaTableBuilder};
    use arrow::{array::Float64Array, compute::kernels::cmp};
    use futures_util::TryStreamExt;
    use serde_json::json;
    use std::fs;
    let root = TestDir::new("invalid-nan-metadata")?;
    fs::create_dir_all(root.path().join("_delta_log"))?;
    fs::write(root.path().join("part.parquet"), &bytes)?;
    let schema = json!({"type":"struct","fields":[
        {"name":"id","type":"integer","nullable":false,"metadata":{}},
        {"name":"f","type":"float","nullable":true,"metadata":{}},
        {"name":"d","type":"double","nullable":true,"metadata":{}}
    ]});
    let protocol = json!({"protocol":{"minReaderVersion":1,"minWriterVersion":2}});
    let meta = json!({"metaData":{"id":"invalid-nan-metadata","format":{"provider":"parquet","options":{}},"schemaString":schema.to_string(),"partitionColumns":[],"configuration":{}}});
    let add = json!({"add":{"path":"part.parquet","partitionValues":{},"size":bytes.len(),"modificationTime":0,"dataChange":true}});
    fs::write(
        root.path().join("_delta_log/00000000000000000000.json"),
        format!("{protocol}\n{meta}\n{add}\n"),
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
    let expected: usize = full
        .iter()
        .map(|batch| {
            cmp::gt(&batch.column(2).as_ref(), &Float64Array::new_scalar(100.0))
                .unwrap()
                .true_count()
        })
        .sum();
    assert_eq!(expected, 1, "Arrow oracle must see the positive NaN match");
    let result = table
        .scan()
        .with_target_partitions(1)?
        .with_predicate(DeltaPredicate::Compare {
            column: "d".into(),
            op: DeltaComparison::Gt,
            value: DeltaScalar::Float64(100.0),
        })
        .build()
        .await?
        .into_stream()
        .try_collect::<Vec<_>>()
        .await?;
    let actual: usize = result.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(
        actual, expected,
        "invalid optional metadata must preserve the NaN match"
    );
    Ok(())
}

#[test]
fn checked_counts_preserve_the_full_i64_domain_and_reject_truncation() {
    let mut values = vec![0, -1, i64::MIN, i64::MAX];
    for bit in 0..63 {
        let value = 1_i64 << bit;
        values.extend([value, -value, value - 1, -value + 1]);
    }
    for value in values {
        let mut bytes = Vec::new();
        TCompactOutputProtocol::new(&mut bytes)
            .write_i64(value)
            .unwrap();
        assert_eq!(super::read_i64(&mut bytes.as_slice()).unwrap(), value);
        for end in 0..bytes.len() {
            assert!(super::read_i64(&mut &bytes[..end]).is_err());
        }
    }
    // Overlong but representable zero remains zero; only overflow is rejected.
    for length in 1..=10 {
        let mut bytes = vec![0x80; length - 1];
        bytes.push(0);
        assert_eq!(super::read_i64(&mut bytes.as_slice()).unwrap(), 0);
    }
}

fn replace_footer(bytes: &Bytes, raw: &[u8]) -> Bytes {
    let old = footer(bytes);
    let mut result = bytes[..bytes.len() - FOOTER_SIZE - old.len()].to_vec();
    result.extend_from_slice(raw);
    result.extend_from_slice(&(raw.len() as u32).to_le_bytes());
    result.extend_from_slice(b"PAR1");
    Bytes::from(result)
}

fn with_overflowing_field_id() -> Bytes {
    let bytes = with_nan_counts(&sample(), &sample_counts());
    let bytes = rewrite(&bytes, |m| {
        m.fields()
            .push((i16::MAX, Value::List(TType::Bool, vec![Value::Bool(true)])));
    });
    let mut raw = footer(&bytes).to_vec();
    assert_eq!(raw.pop(), Some(0));
    // parquet-rs 58 skips no bytes for the unknown boolean list, then reads its
    // true byte as a field header. A decoder that consumes the boolean reaches
    // this delta-one field after ID 32767, so native success is not sufficient.
    raw.extend_from_slice(&[0x16, 0x00, 0x00]);
    replace_footer(&bytes, &raw)
}

#[tokio::test]
async fn native_metadata_success_does_not_allow_field_id_overflow()
-> Result<(), Box<dyn std::error::Error>> {
    let bytes = with_overflowing_field_id();
    let native = metadata(&bytes);
    assert!(NanCounts::decode(footer(&bytes), &native).0.is_empty());
    assert_public_scan_preserves_nan(bytes).await
}

#[test]
fn field_headers_check_delta_and_explicit_id_boundaries() {
    for prior in [i16::MIN, -1, 0, 1, i16::MAX - 15, i16::MAX - 1, i16::MAX] {
        for delta in 1..=15_u8 {
            let bytes = [(delta << 4) | 6];
            let mut last = prior;
            let result = super::read_field_header(&mut bytes.as_slice(), &mut last);
            // Widened arithmetic is independent of the reader's i16 arithmetic.
            let expected = i32::from(prior) + i32::from(delta);
            if expected > i32::from(i16::MAX) {
                assert!(result.is_err(), "prior={prior}, delta={delta}");
            } else {
                assert_eq!(result.unwrap(), TType::I64);
                assert_eq!(i32::from(last), expected);
            }
        }
    }
    for id in [i64::MIN, -32769, -32768, -1, 0, 1, 32767, 32768, i64::MAX] {
        let mut bytes = vec![6]; // Explicit field ID, I64 type.
        TCompactOutputProtocol::new(&mut bytes)
            .write_i64(id)
            .unwrap();
        let mut last = i16::MAX;
        let result = super::read_field_header(&mut bytes.as_slice(), &mut last);
        if (-32768..=32767).contains(&id) {
            assert_eq!(result.unwrap(), TType::I64);
            assert_eq!(i64::from(last), id);
        } else {
            assert!(result.is_err(), "id={id}");
        }
    }
}

#[test]
fn nan_counts_reject_overflow_in_unknown_nested_structs() {
    let bytes = with_nan_counts(&sample(), &sample_counts());
    let native = metadata(&bytes);
    let mut nested = vec![6]; // I64 field with explicit ID 32767, value zero.
    let mut encoder = TCompactOutputProtocol::new(&mut nested);
    encoder.write_i16(i16::MAX).unwrap();
    encoder.write_i64(0).unwrap();
    nested.extend_from_slice(&[0x16, 0, 0]); // Next field's ID would be 32768.
    for before in [false, true] {
        let bytes = rewrite(&bytes, |m| {
            let index = if before { 0 } else { m.fields().len() };
            m.fields()
                .insert(index, (100, Value::Raw(TType::Struct, nested.clone())));
        });
        // Also exercise the skip path, both before and after counts are collected.
        assert!(NanCounts::decode(footer(&bytes), &native).0.is_empty());
    }
}

#[test]
fn unknown_compact_values_preserve_counts() {
    let bytes = with_nan_counts(&sample(), &sample_counts());
    let native = metadata(&bytes);
    let values = [
        Value::Bool(false),
        Value::Bool(true),
        Value::I16(i16::MIN),
        Value::I32(i32::MAX),
        Value::I64(i64::MIN),
        Value::Double(f64::NAN),
        Value::Raw(TType::I08, vec![0xff]),
        Value::Bytes(vec![0xff; 256]),
        Value::List(TType::Bool, Vec::new()),
        Value::List(TType::I64, (0..16).map(Value::I64).collect()),
        Value::Raw(TType::List, vec![0]), // Legacy empty-list header.
        Value::Struct(vec![(1, Value::Bool(false)), (16, Value::I64(10))]),
    ];
    for value in values {
        let bytes = rewrite(&bytes, |m| {
            m.fields().insert(0, (100, value));
        });
        let counts = NanCounts::decode(footer(&bytes), &native);
        for row in 0..2 {
            assert_eq!(counts.get(row, 1), Some(row as u64));
            assert_eq!(counts.get(row, 2), Some(1 - row as u64));
        }
    }
}

#[test]
fn nan_counts_reject_invalid_unknown_fields_and_trailing_bytes() {
    let bytes = with_nan_counts(&sample(), &sample_counts());
    let native = metadata(&bytes);
    let mut wide_i16 = Vec::new();
    TCompactOutputProtocol::new(&mut wide_i16)
        .write_i32(32768)
        .unwrap();
    let mut wide_i32 = Vec::new();
    TCompactOutputProtocol::new(&mut wide_i32)
        .write_i64(i64::from(i32::MAX) + 1)
        .unwrap();
    let mut deep_struct = vec![0x1c; 65];
    deep_struct.extend(vec![0; 66]);
    for value in [
        Value::Raw(TType::I16, wide_i16),
        Value::Raw(TType::I32, wide_i32),
        Value::Raw(TType::I64, overflowing_count()),
        Value::Raw(TType::String, vec![0xff, 0xff, 0xff, 0xff, 0x10]),
        Value::Raw(TType::List, vec![0xf6, 0xff, 0xff, 0xff, 0xff, 0x10]),
        Value::Raw(TType::List, vec![0x11, 3]), // Invalid collection boolean.
        Value::Raw(TType::List, vec![0x10]),    // Nonempty list of STOP.
        Value::Raw(TType::Struct, vec![0x10]),  // STOP with a field-ID delta.
        Value::Raw(TType::Struct, deep_struct),
    ] {
        let bytes = rewrite(&bytes, |m| {
            m.fields().push((100, value));
        });
        assert!(NanCounts::decode(footer(&bytes), &native).0.is_empty());
    }
    let mut raw = footer(&bytes).to_vec();
    raw.push(0);
    assert!(NanCounts::decode(&raw, &native).0.is_empty());
}

#[test]
fn nan_counts_follow_ids_after_nested_leaves_and_name_conflicts()
-> Result<(), Box<dyn std::error::Error>> {
    use super::super::{
        row_group_pruning::pruned_row_groups, schema_alignment::build_schema_alignment,
    };
    use crate::{
        DeltaComparison, DeltaPredicate, DeltaScalar, delta::kernel::kernel_pruning_predicate,
    };
    use arrow::{
        array::{Array, ArrayRef, Float64Array, StructArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use parquet::arrow::{
        ArrowWriter, PARQUET_FIELD_ID_META_KEY, arrow_reader::ParquetRecordBatchReaderBuilder,
    };
    use std::sync::Arc;
    let field = |name: &str, id: i32| {
        Field::new(name, DataType::Float64, true)
            .with_metadata([(PARQUET_FIELD_ID_META_KEY.to_owned(), id.to_string())].into())
    };
    let nested = StructArray::from(vec![
        (
            Arc::new(field("a", 10)),
            Arc::new(Float64Array::from(vec![0.0, 0.0])) as ArrayRef,
        ),
        (
            Arc::new(field("b", 11)),
            Arc::new(Float64Array::from(vec![0.0, 0.0])) as ArrayRef,
        ),
    ]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("nested", nested.data_type().clone(), true),
        field("with_nan", 1),
        field("finite", 2),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(nested),
            Arc::new(Float64Array::from(vec![1.5, f64::NAN])),
            Arc::new(Float64Array::from(vec![1.5, 1.5])),
        ],
    )?;
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    let bytes = with_nan_counts(
        &Bytes::from(bytes),
        &[vec![
            CountField::Count(0),
            CountField::Count(0),
            CountField::Count(1),
            CountField::Count(0),
        ]],
    );
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())?;
    let metadata = builder.metadata();
    let counts = NanCounts::decode(footer(&bytes), metadata);
    // Logical names deliberately conflict with the other physical column.
    let target = Arc::new(Schema::new(vec![field("with_nan", 2), field("finite", 1)]));
    let alignment = build_schema_alignment(builder.parquet_schema(), builder.schema(), target)?;
    for (column, expected) in [("with_nan", vec![]), ("finite", vec![0])] {
        let predicate = kernel_pruning_predicate(&DeltaPredicate::Compare {
            column: column.into(),
            op: DeltaComparison::Gt,
            value: DeltaScalar::Float64(100.0),
        });
        assert_eq!(
            pruned_row_groups(
                metadata,
                &counts,
                builder.schema(),
                &alignment,
                bytes.len() as u64,
                None,
                predicate.as_ref()
            )?,
            Some(expected)
        );
    }
    Ok(())
}

#[test]
fn malformed_footer_mutations_do_not_panic() {
    let bytes = with_nan_counts(&sample(), &sample_counts());
    let native = metadata(&bytes);
    let raw = footer(&bytes);
    let mut mutated = raw.to_vec();
    for index in 0..raw.len() {
        for value in [0, 0x7f, 0x80, 0xff] {
            mutated[index] = value;
            // A mutation may still encode valid metadata. Regardless of whether
            // counts survive, malformed input must terminate without a panic.
            let _ = NanCounts::decode(&mutated, &native);
        }
        mutated[index] = raw[index];
    }
}

#[test]
fn nan_counts_reject_boolean_collections_that_native_skips_differently() {
    let bytes = with_nan_counts(&sample(), &sample_counts());
    let bytes = rewrite(&bytes, |m| {
        m.fields()
            .push((100, Value::List(TType::Bool, vec![Value::Bool(true)])));
        m.fields().push((101, Value::I64(0)));
    });
    // This footer is well-formed, but parquet-rs 58 leaves the collection's true
    // byte unconsumed and treats it as a struct field. Do not trust two different
    // interpretations of the same footer just because both decoders succeed.
    let native = metadata(&bytes);
    assert!(NanCounts::decode(footer(&bytes), &native).0.is_empty());
}
