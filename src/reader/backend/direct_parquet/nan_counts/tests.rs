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
    WrongType,
    Duplicate(i64, i64),
}

#[derive(Debug)]
enum Value {
    Bool(bool),
    I16(i16),
    I32(i32),
    I64(i64),
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
    let mut bytes = bytes[..bytes.len() - FOOTER_SIZE - raw_footer.len()].to_vec();
    bytes.extend_from_slice(&encoded);
    bytes.extend_from_slice(&(encoded.len() as u32).to_le_bytes());
    bytes.extend_from_slice(b"PAR1");
    Bytes::from(bytes)
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
