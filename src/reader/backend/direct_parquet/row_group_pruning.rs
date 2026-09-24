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

use arrow::{
    array::ArrayRef,
    datatypes::{DataType as ArrowDataType, Field, Schema, TimeUnit},
};
use delta_kernel::engine::arrow_conversion::{TryFromKernel, scalar::extract_primitive_scalar};
use delta_kernel::kernel_predicates::{
    DataSkippingPredicateEvaluator, KernelPredicateEvaluator, KernelPredicateEvaluatorDefaults,
};
use parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use parquet::file::statistics::Statistics;
use parquet::schema::types::SchemaDescriptor;
use parquet::{
    errors::{ParquetError, Result as ParquetResult},
    file::metadata::{ParquetMetaData, RowGroupMetaData},
};

use delta_kernel::{
    expressions::{ColumnName, Scalar},
    schema::DataType,
};

use super::schema_alignment::{ParquetSchemaAlignment, cast_leaf_array, leaf_cast_plan};
use crate::delta::kernel::DeltaKernelPredicate;

#[cfg(test)]
#[path = "row_group_pruning/typed_statistics_tests.rs"]
mod typed_statistics_tests;

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
pub(super) fn pruned_row_groups(
    metadata: &ParquetMetaData,
    file_schema: &Schema,
    schema_alignment: &ParquetSchemaAlignment,
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

    let field_indices = if predicate.is_some() {
        row_group_field_indices(metadata.file_metadata().schema_descr(), schema_alignment)
    } else {
        HashMap::new()
    };
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
            RowGroupStats {
                row_group,
                file_schema,
                field_indices: &field_indices,
            }
            .may_contain_matching_rows(predicate.as_ref())
        });
        if in_range && may_match {
            selected.push(ordinal);
        }
    }
    Ok(Some(selected))
}

struct RowGroupStats<'a> {
    row_group: &'a RowGroupMetaData,
    file_schema: &'a Schema,
    field_indices: &'a HashMap<ColumnName, (usize, &'a Field)>,
}

impl RowGroupStats<'_> {
    fn may_contain_matching_rows(&self, predicate: &delta_kernel::PredicateRef) -> bool {
        self.eval_sql_where(predicate) != Some(false)
    }

    fn stats(&self, column: &ColumnName) -> Option<&Statistics> {
        let (index, _) = self.field_indices.get(column)?;
        self.row_group.column(*index).statistics()
    }

    fn min_stat(&self, column: &ColumnName, data_type: &DataType) -> Option<Scalar> {
        let target = ArrowDataType::try_from_kernel(data_type).ok()?;
        extract_primitive_scalar(self.bound(column, &target, true)?.as_ref(), 0).ok()
    }

    fn max_stat(&self, column: &ColumnName, data_type: &DataType) -> Option<Scalar> {
        let target = ArrowDataType::try_from_kernel(data_type).ok()?;
        extract_primitive_scalar(self.bound(column, &target, false)?.as_ref(), 0).ok()
    }

    /// Decode in the file's type, then apply the same cast as the data reader.
    /// Passing the table type to StatisticsConverter would relabel raw decimal
    /// scales/timestamp units instead of converting their values.
    fn bound(
        &self,
        column: &ColumnName,
        target: &ArrowDataType,
        minimum: bool,
    ) -> Option<ArrayRef> {
        let &(index, _) = self.field_indices.get(column)?;
        let descriptor = self.row_group.schema_descr().column(index);
        let statistics = self.stats(column)?;
        if statistics.physical_type() != descriptor.physical_type() {
            return None;
        }
        let converter = StatisticsConverter::try_new(
            descriptor.name(),
            self.file_schema,
            self.row_group.schema_descr(),
        )
        .ok()?;
        // The converter matches by name, which can select a different field ID
        // when file columns share a name. Trust only the data reader's match.
        if converter.parquet_column_index() != Some(index) {
            return None;
        }
        let source = converter.arrow_field().data_type();
        if !compatible_statistics_type(source, target) {
            return None;
        }
        let bytes = if minimum {
            statistics.min_bytes_opt()?
        } else {
            statistics.max_bytes_opt()?
        };
        // parquet-rs sign-extends decimal bytes into a fixed-size integer. Reject
        // invalid widths before invoking that decoder (which assumes valid input).
        if matches!(source, ArrowDataType::Decimal128(_, _))
            && (bytes.is_empty() || bytes.len() > 16)
        {
            return None;
        }
        let values = if minimum {
            converter.row_group_mins([self.row_group]).ok()?
        } else {
            converter.row_group_maxes([self.row_group]).ok()?
        };
        if values.is_null(0) {
            return None;
        }
        if matches!(source, ArrowDataType::Decimal128(_, _)) {
            // Validate the source precision too; widening must not legitimize a
            // malformed source bound merely because it fits the destination.
            extract_primitive_scalar(values.as_ref(), 0).ok()?;
        }
        let values = cast_leaf_array(values.as_ref(), target).ok()?;
        values.is_valid(0).then_some(values)
    }

    fn null_count_stat(&self, column: &ColumnName) -> Option<i64> {
        let count = i64::try_from(self.stats(column)?.null_count_opt()?).ok()?;
        let &(index, target_field) = self.field_indices.get(column)?;
        let descriptor = self.row_group.schema_descr().column(index);
        if self.stats(column)?.physical_type() != descriptor.physical_type() {
            return None;
        }
        let converter = StatisticsConverter::try_new(
            descriptor.name(),
            self.file_schema,
            self.row_group.schema_descr(),
        )
        .ok()?;
        // The converter matches by name, which can select a different field ID
        // when file columns share a name. Trust only the data reader's match.
        if converter.parquet_column_index() != Some(index) {
            return None;
        }
        let source = converter.arrow_field().data_type();
        let target = target_field.data_type();
        if !compatible_statistics_type(source, target) {
            return None;
        }
        if count != self.row_count_stat() && temporal_cast_can_introduce_nulls(source, target) {
            // Temporal casts can overflow to NULL. File null counts describe
            // logical values only if both bounds survive the monotonic cast.
            self.bound(column, target, true)?;
            self.bound(column, target, false)?;
        }
        Some(count)
    }

    fn row_count_stat(&self) -> i64 {
        self.row_group.num_rows()
    }
}

impl DataSkippingPredicateEvaluator for RowGroupStats<'_> {
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

fn row_group_field_indices<'a>(
    schema: &SchemaDescriptor,
    alignment: &'a ParquetSchemaAlignment,
) -> HashMap<ColumnName, (usize, &'a Field)> {
    alignment
        .matched_root_fields()
        .filter_map(|(target_field, root_index)| {
            // A nested leaf's bounds or null count cannot describe its parent.
            if target_field.data_type().is_nested() {
                return None;
            }
            let index = schema
                .columns()
                .iter()
                .enumerate()
                .find_map(|(index, column)| {
                    (schema.get_column_root_idx(index) == root_index
                        && column.path().parts().len() == 1)
                        .then_some(index)
                })?;
            Some((
                ColumnName::new([target_field.name()]),
                (index, target_field),
            ))
        })
        .collect()
}

// View arrays differ only in representation. Other conversions must follow
// the same supported widening rules as physical data decoding.
fn compatible_statistics_type(source: &ArrowDataType, target: &ArrowDataType) -> bool {
    matches!(
        (source, target),
        (ArrowDataType::Utf8View, ArrowDataType::Utf8)
            | (ArrowDataType::BinaryView, ArrowDataType::Binary)
    ) || leaf_cast_plan(target, source).is_ok()
}

fn temporal_cast_can_introduce_nulls(source: &ArrowDataType, target: &ArrowDataType) -> bool {
    use ArrowDataType::{Date32, Timestamp};
    use TimeUnit::{Microsecond, Millisecond, Nanosecond, Second};
    matches!(
        (source, target),
        (Date32, Timestamp(Microsecond | Nanosecond, _))
        | (Timestamp(Second | Millisecond, _), Timestamp(Microsecond, _))
        // Localizing a naive microsecond timestamp also needs a representable
        // calendar date. Nanoseconds already fit in that calendar range.
        | (Timestamp(Microsecond, None), Timestamp(Microsecond, Some(_)))
    )
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

    fn pruned_row_groups(
        metadata: &ParquetMetaData,
        file_size: u64,
        byte_range: Option<&Range<u64>>,
        predicate: Option<&DeltaKernelPredicate>,
    ) -> ParquetResult<Option<Vec<usize>>> {
        let schema = Arc::new(parquet::arrow::parquet_to_arrow_schema(
            metadata.file_metadata().schema_descr(),
            metadata.file_metadata().key_value_metadata(),
        )?);
        let alignment = super::super::schema_alignment::build_schema_alignment(
            metadata.file_metadata().schema_descr(),
            &schema,
            Arc::clone(&schema),
        )
        .map_err(|error| ParquetError::General(error.to_string()))?;
        super::pruned_row_groups(
            metadata, &schema, &alignment, file_size, byte_range, predicate,
        )
    }

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
            pruned_row_groups(&metadata, 80, Some(&(0..25)), None)?,
            Some(vec![0])
        );
        assert_eq!(
            pruned_row_groups(&metadata, 80, Some(&(25..50)), None)?,
            Some(vec![1])
        );
        assert_eq!(
            pruned_row_groups(&metadata, 80, Some(&(50..65)), None)?,
            Some(vec![2])
        );
        assert_eq!(
            pruned_row_groups(&metadata, 80, Some(&(65..80)), None)?,
            Some(vec![3])
        );

        Ok(())
    }

    #[test]
    fn byte_range_uses_dictionary_offset_and_half_open_bounds()
    -> Result<(), Box<dyn std::error::Error>> {
        let metadata = metadata_with_row_group_offsets(&[(30, Some(20)), (40, None)])?;

        assert_eq!(
            pruned_row_groups(&metadata, 41, Some(&(20..40)), None)?,
            Some(vec![0])
        );
        assert_eq!(
            pruned_row_groups(&metadata, 41, Some(&(40..41)), None)?,
            Some(vec![1])
        );

        Ok(())
    }

    #[test]
    fn regression_byte_range_rejects_malformed_row_group_coordinates()
    -> Result<(), Box<dyn std::error::Error>> {
        assert!(
            pruned_row_groups(&metadata_without_columns()?, 100, Some(&(0..100)), None,).is_err()
        );
        for metadata in [
            metadata_with_row_group_offsets(&[(-1, None)])?,
            metadata_with_row_group_offsets(&[(10, Some(-1))])?,
            metadata_with_row_group_offsets(&[(100, None)])?,
            metadata_with_row_group_offsets(&[(101, None)])?,
        ] {
            assert!(pruned_row_groups(&metadata, 100, Some(&(0..100)), None).is_err());
        }

        let metadata = metadata_with_row_group_offsets(&[(0, None), (99, None)])?;
        assert_eq!(
            pruned_row_groups(&metadata, 100, Some(&(0..100)), None)?,
            Some(vec![0, 1])
        );
        for (start, end) in [(0, 0), (0, 101), (100, 99)] {
            let range = start..end;
            assert!(pruned_row_groups(&metadata, 100, Some(&range), None).is_err());
        }

        Ok(())
    }
}
