//! Compatibility reader for Statistics.nan_count (Parquet Thrift field 9).
//!
//! parquet-rs 58 discards this field. Read it from the same footer bytes,
//! indexed by row-group ordinal and physical leaf ordinal, never by field name.
//! Native metadata decoding does not validate unknown fields for us. All wire
//! integers, field IDs, and lengths below are checked before they are trusted.
//! Unknown or malformed counts cannot establish that a column is NaN-free.
//!
//! ponytail: Replace this module and footer capture with Statistics::nan_count_opt()
//! after upgrading to Parquet 60 and verifying the malformed-encoding regressions.
//! Keep the same Some(0) pruning rule; an API upgrade alone does not validate counts.

use std::collections::HashMap;

use parquet::{basic::Type, file::metadata::ParquetMetaData};
use thrift::protocol::TType;

use super::compact_thrift::{invalid_metadata, read_field, read_i64, read_struct_list};

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
        let mut protocol = footer;
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
        if !protocol.is_empty() {
            return Err(invalid_metadata());
        }
        Ok(Self(counts))
    }
}
