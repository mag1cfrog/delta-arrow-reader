//! Native parquet row-group pruning for physical scan predicates.
//!
//! This module uses Delta Kernel's public data-skipping evaluator trait, but
//! owns the parquet footer stats adapter because Delta Kernel's built-in
//! row-group adapter is crate-private. The safety rule is conservative: if a
//! row group's stats are missing or cannot be converted to the expected Delta
//! scalar type, keep the row group.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::ops::Range;

use chrono::{DateTime, Days};
use delta_kernel::kernel_predicates::{
    DataSkippingPredicateEvaluator, KernelPredicateEvaluator, KernelPredicateEvaluatorDefaults,
};
use parquet::file::statistics::Statistics;
use parquet::schema::types::ColumnDescPtr;
use parquet::{
    errors::{ParquetError, Result as ParquetResult},
    file::metadata::{ParquetMetaData, RowGroupMetaData},
};

use delta_kernel::{
    expressions::{ColumnName, DecimalData, Scalar},
    schema::{DataType, PrimitiveType},
};

use crate::kernel::DeltaKernelPredicate;

/// Computes the row groups selected by a byte range and footer statistics.
///
/// A row group belongs to the half-open byte range containing its first column
/// chunk's dictionary-page offset, or its data-page offset when no dictionary
/// page exists. A row group can therefore belong to at most one non-overlapping
/// range, and covering ranges assign every row group exactly once rather than
/// splitting rows at an arbitrary byte boundary. When a predicate is present,
/// the result is the intersection of range ownership and conservative
/// footer-statistics pruning.
///
/// `None` means there is no byte range or physical predicate to use for pruning.
/// `Some(Vec::new())` means every row group was proven impossible and the
/// parquet reader should return no rows.
#[allow(dead_code)]
pub(crate) fn native_async_pruned_row_groups(
    metadata: &ParquetMetaData,
    file_size: u64,
    byte_range: Option<&Range<u64>>,
    predicate: Option<&DeltaKernelPredicate>,
) -> ParquetResult<Option<Vec<usize>>> {
    if byte_range.is_none() && predicate.is_none() {
        return Ok(None);
    }

    if byte_range.is_some_and(|range| range.start >= range.end || range.end > file_size) {
        return Err(ParquetError::General(
            "parquet scan byte range is outside the file".to_owned(),
        ));
    }

    let mut selected = Vec::new();
    for (ordinal, row_group) in metadata.row_groups().iter().enumerate() {
        let in_range = match byte_range {
            None => true,
            Some(range) => {
                let column = row_group.columns().first().ok_or_else(|| {
                    ParquetError::General(
                        "parquet row group has no first-column metadata".to_owned(),
                    )
                })?;
                let offset = column
                    .dictionary_page_offset()
                    .unwrap_or_else(|| column.data_page_offset());
                let offset = u64::try_from(offset).map_err(|_| {
                    ParquetError::General("parquet row-group offset is negative".to_owned())
                })?;
                if offset >= file_size {
                    return Err(ParquetError::General(
                        "parquet row-group offset is outside the file".to_owned(),
                    ));
                }
                range.contains(&offset)
            }
        };
        let may_match = predicate.is_none_or(|predicate| {
            NativeAsyncRowGroupStats::new(row_group).may_contain_matching_rows(predicate.as_ref())
        });
        if in_range && may_match {
            selected.push(ordinal);
        }
    }
    Ok(Some(selected))
}

struct NativeAsyncRowGroupStats<'a> {
    row_group: &'a RowGroupMetaData,
    field_indices: HashMap<ColumnName, usize>,
}

impl<'a> NativeAsyncRowGroupStats<'a> {
    fn new(row_group: &'a RowGroupMetaData) -> Self {
        Self {
            row_group,
            field_indices: row_group_field_indices(row_group.schema_descr().columns()),
        }
    }

    fn may_contain_matching_rows(&self, predicate: &delta_kernel::PredicateRef) -> bool {
        self.eval_sql_where(predicate) != Some(false)
    }

    fn stats(&self, column: &ColumnName) -> Option<Option<&Statistics>> {
        self.field_indices
            .get(column)
            .map(|index| self.row_group.column(*index).statistics())
    }

    fn min_stat(&self, column: &ColumnName, data_type: &DataType) -> Option<Scalar> {
        stat_min_scalar(data_type, self.stats(column)??)
    }

    fn max_stat(&self, column: &ColumnName, data_type: &DataType) -> Option<Scalar> {
        stat_max_scalar(data_type, self.stats(column)??)
    }

    fn null_count_stat(&self, column: &ColumnName) -> Option<i64> {
        self.stats(column)??
            .null_count_opt()
            .map(|value| value as i64)
    }

    fn row_count_stat(&self) -> i64 {
        self.row_group.num_rows()
    }
}

impl DataSkippingPredicateEvaluator for NativeAsyncRowGroupStats<'_> {
    type Output = bool;
    type ColumnStat = Scalar;

    fn get_min_stat(&self, col: &ColumnName, data_type: &DataType) -> Option<Scalar> {
        self.min_stat(col, data_type)
    }

    fn get_max_stat(&self, col: &ColumnName, data_type: &DataType) -> Option<Scalar> {
        self.max_stat(col, data_type)
    }

    fn get_nullcount_stat(&self, col: &ColumnName) -> Option<Scalar> {
        self.null_count_stat(col).map(Scalar::from)
    }

    fn get_rowcount_stat(&self) -> Option<Scalar> {
        Some(Scalar::from(self.row_count_stat()))
    }

    fn eval_partial_cmp(
        &self,
        ord: Ordering,
        col: Scalar,
        val: &Scalar,
        inverted: bool,
    ) -> Option<bool> {
        KernelPredicateEvaluatorDefaults::partial_cmp_scalars(ord, &col, val, inverted)
    }

    fn eval_pred_scalar(&self, val: &Scalar, inverted: bool) -> Option<bool> {
        KernelPredicateEvaluatorDefaults::eval_pred_scalar(val, inverted)
    }

    fn eval_pred_scalar_is_null(&self, val: &Scalar, inverted: bool) -> Option<bool> {
        KernelPredicateEvaluatorDefaults::eval_pred_scalar_is_null(val, inverted)
    }

    fn eval_pred_is_null(&self, col: &ColumnName, inverted: bool) -> Option<bool> {
        let safe_to_skip = match inverted {
            true => self.get_rowcount_stat()?,
            false => Scalar::from(0_i64),
        };
        Some(self.get_nullcount_stat(col)? != safe_to_skip)
    }

    fn eval_pred_binary_scalars(
        &self,
        op: delta_kernel::expressions::BinaryPredicateOp,
        left: &Scalar,
        right: &Scalar,
        inverted: bool,
    ) -> Option<bool> {
        KernelPredicateEvaluatorDefaults::eval_pred_binary_scalars(op, left, right, inverted)
    }

    fn eval_pred_opaque(
        &self,
        op: &delta_kernel::expressions::OpaquePredicateOpRef,
        exprs: &[delta_kernel::Expression],
        inverted: bool,
    ) -> Option<bool> {
        op.eval_as_data_skipping_predicate(self, exprs, inverted)
    }

    fn finish_eval_pred_junction(
        &self,
        op: delta_kernel::expressions::JunctionPredicateOp,
        preds: &mut dyn Iterator<Item = Option<bool>>,
        inverted: bool,
    ) -> Option<bool> {
        KernelPredicateEvaluatorDefaults::finish_eval_pred_junction(op, preds, inverted)
    }
}

fn row_group_field_indices(columns: &[ColumnDescPtr]) -> HashMap<ColumnName, usize> {
    columns
        .iter()
        .enumerate()
        .filter_map(|(index, column)| {
            let name = column.path().parts().first()?.as_str();
            Some((ColumnName::new([name]), index))
        })
        .collect()
}

fn stat_min_scalar(data_type: &DataType, stats: &Statistics) -> Option<Scalar> {
    use PrimitiveType::*;

    match (data_type.as_primitive_opt()?, stats) {
        (String, Statistics::ByteArray(values)) => values.min_opt()?.as_utf8().ok().map(Into::into),
        (String, Statistics::FixedLenByteArray(values)) => {
            values.min_opt()?.as_utf8().ok().map(Into::into)
        }
        (Long, Statistics::Int64(values)) => values.min_opt().map(Into::into),
        (Long, Statistics::Int32(values)) => values.min_opt().map(|value| (*value as i64).into()),
        (Integer, Statistics::Int32(values)) => values.min_opt().map(Into::into),
        (Short, Statistics::Int32(values)) => values.min_opt().map(|value| (*value as i16).into()),
        (Byte, Statistics::Int32(values)) => values.min_opt().map(|value| (*value as i8).into()),
        (Float, Statistics::Float(values)) => values.min_opt().map(Into::into),
        (Double, Statistics::Double(values)) => values.min_opt().map(Into::into),
        (Double, Statistics::Float(values)) => values.min_opt().map(|value| (*value as f64).into()),
        (Boolean, Statistics::Boolean(values)) => values.min_opt().map(Into::into),
        (Binary, Statistics::ByteArray(values)) => {
            values.min_opt().map(|value| value.data().into())
        }
        (Binary, Statistics::FixedLenByteArray(values)) => {
            values.min_opt().map(|value| value.data().into())
        }
        (Date, Statistics::Int32(values)) => values.min_opt().map(|value| Scalar::Date(*value)),
        (Timestamp, Statistics::Int64(values)) => {
            values.min_opt().map(|value| Scalar::Timestamp(*value))
        }
        (TimestampNtz, Statistics::Int64(values)) => {
            values.min_opt().map(|value| Scalar::TimestampNtz(*value))
        }
        (TimestampNtz, Statistics::Int32(values)) => timestamp_ntz_from_days(values.min_opt()),
        (Decimal(decimal_type), Statistics::Int32(values)) => values
            .min_opt()
            .and_then(|value| DecimalData::try_new(*value, *decimal_type).ok())
            .map(Into::into),
        (Decimal(decimal_type), Statistics::Int64(values)) => values
            .min_opt()
            .and_then(|value| DecimalData::try_new(*value, *decimal_type).ok())
            .map(Into::into),
        (Decimal(decimal_type), Statistics::FixedLenByteArray(values)) => values
            .min_opt()
            .and_then(|value| decimal_scalar_from_bytes(value.data(), *decimal_type)),
        _ => None,
    }
}

fn stat_max_scalar(data_type: &DataType, stats: &Statistics) -> Option<Scalar> {
    use PrimitiveType::*;

    match (data_type.as_primitive_opt()?, stats) {
        (String, Statistics::ByteArray(values)) => values.max_opt()?.as_utf8().ok().map(Into::into),
        (String, Statistics::FixedLenByteArray(values)) => {
            values.max_opt()?.as_utf8().ok().map(Into::into)
        }
        (Long, Statistics::Int64(values)) => values.max_opt().map(Into::into),
        (Long, Statistics::Int32(values)) => values.max_opt().map(|value| (*value as i64).into()),
        (Integer, Statistics::Int32(values)) => values.max_opt().map(Into::into),
        (Short, Statistics::Int32(values)) => values.max_opt().map(|value| (*value as i16).into()),
        (Byte, Statistics::Int32(values)) => values.max_opt().map(|value| (*value as i8).into()),
        (Float, Statistics::Float(values)) => values.max_opt().map(Into::into),
        (Double, Statistics::Double(values)) => values.max_opt().map(Into::into),
        (Double, Statistics::Float(values)) => values.max_opt().map(|value| (*value as f64).into()),
        (Boolean, Statistics::Boolean(values)) => values.max_opt().map(Into::into),
        (Binary, Statistics::ByteArray(values)) => {
            values.max_opt().map(|value| value.data().into())
        }
        (Binary, Statistics::FixedLenByteArray(values)) => {
            values.max_opt().map(|value| value.data().into())
        }
        (Date, Statistics::Int32(values)) => values.max_opt().map(|value| Scalar::Date(*value)),
        (Timestamp, Statistics::Int64(values)) => {
            values.max_opt().map(|value| Scalar::Timestamp(*value))
        }
        (TimestampNtz, Statistics::Int64(values)) => {
            values.max_opt().map(|value| Scalar::TimestampNtz(*value))
        }
        (TimestampNtz, Statistics::Int32(values)) => timestamp_ntz_from_days(values.max_opt()),
        (Decimal(decimal_type), Statistics::Int32(values)) => values
            .max_opt()
            .and_then(|value| DecimalData::try_new(*value, *decimal_type).ok())
            .map(Into::into),
        (Decimal(decimal_type), Statistics::Int64(values)) => values
            .max_opt()
            .and_then(|value| DecimalData::try_new(*value, *decimal_type).ok())
            .map(Into::into),
        (Decimal(decimal_type), Statistics::FixedLenByteArray(values)) => values
            .max_opt()
            .and_then(|value| decimal_scalar_from_bytes(value.data(), *decimal_type)),
        _ => None,
    }
}

fn timestamp_ntz_from_days(days: Option<&i32>) -> Option<Scalar> {
    let days = u64::try_from(*days?).ok()?;
    let timestamp = DateTime::UNIX_EPOCH.checked_add_days(Days::new(days))?;
    let duration = timestamp.signed_duration_since(DateTime::UNIX_EPOCH);
    Some(Scalar::TimestampNtz(duration.num_microseconds()?))
}

fn decimal_scalar_from_bytes(
    bytes: &[u8],
    data_type: delta_kernel::schema::DecimalType,
) -> Option<Scalar> {
    if bytes.len() > 16 {
        return None;
    }

    // Parquet fixed-length decimal stats are stored as big-endian two's
    // complement bytes. Convert to little-endian i128 bytes and preserve the
    // sign when the encoded value is narrower than 16 bytes.
    let pad = if bytes.first().is_some_and(|byte| byte & 0x80 != 0) {
        0xff
    } else {
        0x00
    };
    let mut bytes = Vec::from(bytes);
    bytes.reverse();
    bytes.resize(16, pad);
    let bytes: [u8; 16] = bytes.try_into().ok()?;
    DecimalData::try_new(i128::from_le_bytes(bytes), data_type)
        .ok()
        .map(Into::into)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use parquet::{
        basic::Type as PhysicalType,
        file::metadata::{ColumnChunkMetaData, FileMetaData, RowGroupMetaData},
        schema::types::{SchemaDescriptor, Type as SchemaType},
    };

    use super::*;

    fn metadata_with_row_group_offsets(
        offsets: &[(i64, Option<i64>)],
    ) -> Result<ParquetMetaData, parquet::errors::ParquetError> {
        let schema = SchemaType::group_type_builder("schema")
            .with_fields(vec![Arc::new(
                SchemaType::primitive_type_builder("value", PhysicalType::INT32).build()?,
            )])
            .build()?;
        let schema = Arc::new(SchemaDescriptor::new(Arc::new(schema)));
        let row_groups = offsets
            .iter()
            .enumerate()
            .map(|(ordinal, (data_offset, dictionary_offset))| {
                let column = ColumnChunkMetaData::builder(schema.columns()[0].clone())
                    .set_data_page_offset(*data_offset)
                    .set_dictionary_page_offset(*dictionary_offset)
                    .build()?;
                RowGroupMetaData::builder(Arc::clone(&schema))
                    .set_num_rows(1)
                    .set_ordinal(ordinal as i16)
                    .set_column_metadata(vec![column])
                    .build()
            })
            .collect::<Result<Vec<_>, _>>()?;
        let file = FileMetaData::new(1, row_groups.len() as i64, None, None, schema, None);
        Ok(ParquetMetaData::new(file, row_groups))
    }

    fn metadata_without_columns() -> Result<ParquetMetaData, parquet::errors::ParquetError> {
        let schema = SchemaType::group_type_builder("schema").build()?;
        let schema = Arc::new(SchemaDescriptor::new(Arc::new(schema)));
        let row_group = RowGroupMetaData::builder(Arc::clone(&schema))
            .set_num_rows(1)
            .set_ordinal(0)
            .set_column_metadata(vec![])
            .build()?;
        let file = FileMetaData::new(1, 1, None, None, schema, None);
        Ok(ParquetMetaData::new(file, vec![row_group]))
    }

    #[test]
    fn byte_range_selects_each_row_group_exactly_once() -> Result<(), Box<dyn std::error::Error>> {
        let metadata = metadata_with_row_group_offsets(&[
            (10, None),
            (30, Some(25)),
            (50, None),
            (70, Some(65)),
        ])?;

        assert_eq!(
            native_async_pruned_row_groups(&metadata, 80, Some(&(0..25)), None)?,
            Some(vec![0])
        );
        assert_eq!(
            native_async_pruned_row_groups(&metadata, 80, Some(&(25..50)), None)?,
            Some(vec![1])
        );
        assert_eq!(
            native_async_pruned_row_groups(&metadata, 80, Some(&(50..65)), None)?,
            Some(vec![2])
        );
        assert_eq!(
            native_async_pruned_row_groups(&metadata, 80, Some(&(65..80)), None)?,
            Some(vec![3])
        );

        Ok(())
    }

    #[test]
    fn byte_range_uses_dictionary_offset_and_half_open_bounds()
    -> Result<(), Box<dyn std::error::Error>> {
        let metadata = metadata_with_row_group_offsets(&[(30, Some(20)), (40, None)])?;

        assert_eq!(
            native_async_pruned_row_groups(&metadata, 41, Some(&(20..40)), None)?,
            Some(vec![0])
        );
        assert_eq!(
            native_async_pruned_row_groups(&metadata, 41, Some(&(40..41)), None)?,
            Some(vec![1])
        );

        Ok(())
    }

    #[test]
    fn regression_byte_range_rejects_malformed_row_group_coordinates()
    -> Result<(), Box<dyn std::error::Error>> {
        assert!(
            native_async_pruned_row_groups(
                &metadata_without_columns()?,
                100,
                Some(&(0..100)),
                None,
            )
            .is_err()
        );
        for metadata in [
            metadata_with_row_group_offsets(&[(-1, None)])?,
            metadata_with_row_group_offsets(&[(10, Some(-1))])?,
            metadata_with_row_group_offsets(&[(100, None)])?,
            metadata_with_row_group_offsets(&[(101, None)])?,
        ] {
            assert!(native_async_pruned_row_groups(&metadata, 100, Some(&(0..100)), None).is_err());
        }

        let metadata = metadata_with_row_group_offsets(&[(0, None), (99, None)])?;
        assert_eq!(
            native_async_pruned_row_groups(&metadata, 100, Some(&(0..100)), None)?,
            Some(vec![0, 1])
        );
        for (start, end) in [(0, 0), (0, 101), (100, 99)] {
            let range = start..end;
            assert!(native_async_pruned_row_groups(&metadata, 100, Some(&range), None).is_err());
        }

        Ok(())
    }

    #[test]
    fn decimal_scalar_from_fixed_len_bytes_sign_extends_negative_values()
    -> Result<(), Box<dyn std::error::Error>> {
        let decimal_type = delta_kernel::schema::DecimalType::try_new(10, 2)?;
        let negative_one = match decimal_scalar_from_bytes(&[0xff], decimal_type) {
            Some(Scalar::Decimal(value)) => value,
            other => return Err(format!("expected decimal scalar, got {other:?}").into()),
        };

        assert_eq!(negative_one.bits(), -1);

        Ok(())
    }

    #[test]
    fn decimal_scalar_from_fixed_len_bytes_preserves_positive_values()
    -> Result<(), Box<dyn std::error::Error>> {
        let decimal_type = delta_kernel::schema::DecimalType::try_new(10, 2)?;
        let positive_one = match decimal_scalar_from_bytes(&[0x01], decimal_type) {
            Some(Scalar::Decimal(value)) => value,
            other => return Err(format!("expected decimal scalar, got {other:?}").into()),
        };

        assert_eq!(positive_one.bits(), 1);

        Ok(())
    }
}
