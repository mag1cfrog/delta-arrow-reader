//! Compatibility reader for Statistics.nan_count (Parquet Thrift field 9).
//!
//! parquet-rs 58 discards this field. Read it from the same validated footer,
//! indexed by row-group ordinal and physical leaf ordinal, never by field name.
//! Unknown or malformed counts cannot establish that a column is NaN-free.
//!
//! ponytail: Replace this module and footer capture with Statistics::nan_count_opt()
//! after upgrading to Parquet 60 and verifying the malformed-encoding regressions.
//! Keep the same Some(0) pruning rule; an API upgrade alone does not validate counts.

use std::collections::HashMap;

use parquet::{basic::Type, file::metadata::ParquetMetaData};
use thrift::{
    ProtocolErrorKind, new_protocol_error,
    protocol::{TCompactInputProtocol, TInputProtocol, TType},
};

#[cfg(test)]
pub(super) mod tests;

#[derive(Debug, Default)]
pub(super) struct NanCounts(HashMap<(usize, usize), u64>);

impl NanCounts {
    pub(super) fn get(&self, row_group: usize, column: usize) -> Option<u64> {
        self.0.get(&(row_group, column)).copied()
    }

    /// Called only after parquet-rs has successfully decoded this footer.
    pub(super) fn decode(footer: &[u8], metadata: &ParquetMetaData) -> Self {
        if !metadata
            .file_metadata()
            .schema_descr()
            .columns()
            .iter()
            .any(|column| matches!(column.physical_type(), Type::FLOAT | Type::DOUBLE))
        {
            return Self::default();
        }
        // Any structural disagreement discards the entire side table: trusting
        // a count associated with the wrong column could silently lose rows.
        Self::read(footer, metadata).unwrap_or_default()
    }

    fn read(footer: &[u8], metadata: &ParquetMetaData) -> thrift::Result<Self> {
        let mut protocol = TCompactInputProtocol::new(footer);
        let mut counts = HashMap::new();
        // FileMetaData.row_groups -> RowGroup.columns -> ColumnChunk.meta_data
        // -> ColumnMetaData.statistics -> Statistics.nan_count.
        read_field(&mut protocol, 4, TType::List, |p| {
            read_struct_list(p, metadata.num_row_groups(), |p, row_index| {
                let row = metadata.row_group(row_index);
                read_field(p, 1, TType::List, |p| {
                    read_struct_list(p, row.num_columns(), |p, column_index| {
                        let count = read_field(p, 3, TType::Struct, |p| {
                            Ok(read_field(p, 12, TType::Struct, |p| {
                                read_field(p, 9, TType::I64, read_i64)
                            })?
                            .flatten())
                        })?
                        .flatten();
                        let column = row.column(column_index);
                        if matches!(column.column_type(), Type::FLOAT | Type::DOUBLE)
                            && let Some(count) = count.and_then(|count| u64::try_from(count).ok())
                            && let Ok(values) = u64::try_from(column.num_values())
                            && count <= values
                            && column
                                .statistics()
                                .and_then(|s| s.null_count_opt())
                                .is_none_or(|nulls| nulls <= values && count <= values - nulls)
                        {
                            counts.insert((row_index, column_index), count);
                        }
                        Ok(())
                    })
                })?;
                Ok(())
            })
        })?;
        Ok(Self(counts))
    }
}

type Protocol<'a> = TCompactInputProtocol<&'a [u8]>;

fn invalid_metadata() -> thrift::Error {
    new_protocol_error(ProtocolErrorKind::InvalidData, "invalid NaN-count metadata")
}

/// Reject over-wide encodings before decoding ZigZag. thrift 0.17 can truncate
/// an overflowing varint to zero, falsely establishing that a column has no NaNs.
fn read_varint(p: &mut Protocol<'_>) -> thrift::Result<u64> {
    let mut value = 0_u64;
    for shift in (0..70).step_by(7) {
        let byte = p.read_byte()?;
        if shift == 63 && byte > 1 {
            return Err(invalid_metadata());
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(invalid_metadata())
}

fn read_i64(p: &mut Protocol<'_>) -> thrift::Result<i64> {
    let value = read_varint(p)?;
    Ok((value >> 1) as i64 ^ -((value & 1) as i64))
}

/// Visit one optional field in a struct, checking its type and uniqueness.
fn read_field<T>(
    p: &mut Protocol<'_>,
    id: i16,
    field_type: TType,
    mut read: impl FnMut(&mut Protocol<'_>) -> thrift::Result<T>,
) -> thrift::Result<Option<T>> {
    p.read_struct_begin()?;
    let mut value = None;
    loop {
        let field = p.read_field_begin()?;
        if field.field_type == TType::Stop {
            break;
        }
        if field.id == Some(id) {
            if field.field_type != field_type || value.is_some() {
                return Err(invalid_metadata());
            }
            value = Some(read(p)?);
        } else {
            skip_value(p, field.field_type, 64)?;
        }
        p.read_field_end()?;
    }
    p.read_struct_end()?;
    Ok(value)
}

fn read_struct_list(
    p: &mut Protocol<'_>,
    expected: usize,
    mut read: impl FnMut(&mut Protocol<'_>, usize) -> thrift::Result<()>,
) -> thrift::Result<()> {
    let list = p.read_list_begin()?;
    if list.element_type != TType::Struct || usize::try_from(list.size).ok() != Some(expected) {
        return Err(invalid_metadata());
    }
    for index in 0..expected {
        read(p, index)?;
    }
    p.read_list_end()
}

/// Thrift's default skip decodes binary fields as UTF-8 strings. Parquet bounds
/// and Arrow schemas are arbitrary bytes, so skip them without decoding or
/// allocating from untrusted lengths. Every loop consumes input or returns an
/// error, and recursive unknown fields have a fixed depth limit.
fn skip_value(p: &mut Protocol<'_>, kind: TType, depth: usize) -> thrift::Result<()> {
    if depth == 0 {
        return Err(invalid_metadata());
    }
    match kind {
        TType::String => {
            // Compact binary lengths are unsigned varints (unlike read_i32).
            let mut length = 0_u32;
            for shift in (0..35).step_by(7) {
                let byte = p.read_byte()?;
                if shift == 28 && byte > 0x0f {
                    return Err(invalid_metadata());
                }
                length |= u32::from(byte & 0x7f) << shift;
                if byte & 0x80 == 0 {
                    for _ in 0..length {
                        p.read_byte()?;
                    }
                    return Ok(());
                }
            }
            Err(invalid_metadata())
        }
        TType::Struct => {
            p.read_struct_begin()?;
            loop {
                let field = p.read_field_begin()?;
                if field.field_type == TType::Stop {
                    break;
                }
                skip_value(p, field.field_type, depth - 1)?;
                p.read_field_end()?;
            }
            p.read_struct_end()
        }
        TType::List | TType::Set => {
            let (element_type, size) = if kind == TType::List {
                let list = p.read_list_begin()?;
                (list.element_type, list.size)
            } else {
                let set = p.read_set_begin()?;
                (set.element_type, set.size)
            };
            let size = usize::try_from(size).map_err(|_| invalid_metadata())?;
            for _ in 0..size {
                skip_value(p, element_type, depth - 1)?;
            }
            if kind == TType::List {
                p.read_list_end()
            } else {
                p.read_set_end()
            }
        }
        TType::Map => {
            let map = p.read_map_begin()?;
            let size = usize::try_from(map.size).map_err(|_| invalid_metadata())?;
            if size > 0 {
                let key = map.key_type.ok_or_else(invalid_metadata)?;
                let value = map.value_type.ok_or_else(invalid_metadata)?;
                for _ in 0..size {
                    skip_value(p, key, depth - 1)?;
                    skip_value(p, value, depth - 1)?;
                }
            }
            p.read_map_end()
        }
        // All remaining valid types are fixed-width or bounded varints.
        _ => p.skip(kind),
    }
}
