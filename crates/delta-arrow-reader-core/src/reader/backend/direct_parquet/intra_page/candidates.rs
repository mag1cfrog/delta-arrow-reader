//! Selects pages whose metadata and matched rows allow partial reading.
//!
//! This checks format eligibility and safety limits without fetching page bytes.
//! The budget planners then decide whether those candidates are worth probing.

use std::ops::Range;

use parquet::basic::{Compression, ConvertedType, Encoding, LogicalType, Type};

use super::super::cost_model::range_bytes;
use super::{
    IntraPageReader, MAX_PAGES, MAX_REQUESTS,
    page::{self, Page},
};
use crate::reader::options::MAX_INTRA_PAGE_READ_BYTES;

impl IntraPageReader {
    pub(super) fn candidate_pages(&self, ranges: &[Range<u64>]) -> Option<(Vec<Page>, Vec<u64>)> {
        if ranges.len() > MAX_REQUESTS
            || ranges
                .iter()
                .any(|r| r.start > r.end || r.end > self.file_size)
            || range_bytes(ranges) > MAX_INTRA_PAGE_READ_BYTES
        {
            return None;
        }
        let state = self
            .selection
            .as_ref()?
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let group_index = state.current_group?;
        let group_rows = state.row_groups.get(group_index)?;
        if state.limit_exceeded
            || state.rows.is_empty()
            || state.rows.len() as u64 >= (group_rows.end - group_rows.start).div_ceil(2)
        {
            return None;
        }
        let offset_indexes = self.metadata.offset_index()?.get(group_index)?;
        let row_group = self.metadata.row_group(group_index);
        let predicate_columns = state.predicate_columns.as_ref()?;
        let mut pages = Vec::new();
        for (column, chunk) in row_group.columns().iter().enumerate() {
            let descriptor = chunk.column_descr();
            let value_width = match descriptor.physical_type() {
                Type::INT32 | Type::FLOAT => Some(4),
                Type::INT64 | Type::DOUBLE => Some(8),
                Type::FIXED_LEN_BYTE_ARRAY => Some(descriptor.type_length() as usize),
                Type::BYTE_ARRAY
                    if matches!(
                        descriptor.logical_type_ref(),
                        None | Some(LogicalType::String)
                    ) && matches!(
                        descriptor.converted_type(),
                        ConvertedType::NONE | ConvertedType::UTF8
                    ) && chunk.compression() == Compression::UNCOMPRESSED
                        && chunk
                            .encodings()
                            .any(|encoding| encoding == Encoding::DELTA_LENGTH_BYTE_ARRAY) =>
                {
                    None
                }
                _ => continue,
            };
            if predicate_columns.leaf_included(column)
                || descriptor.path().parts().len() != 1
                || !matches!(descriptor.max_def_level(), 0 | 1)
                || descriptor.max_rep_level() != 0
                || chunk.dictionary_page_offset().is_some()
                || !matches!(
                    chunk.compression(),
                    Compression::UNCOMPRESSED | Compression::ZSTD(_)
                )
            {
                continue;
            }
            let group_row_count = group_rows.end - group_rows.start;
            let non_null_rows = chunk
                .statistics()
                .and_then(|stats| stats.null_count_opt())
                .and_then(|nulls| group_row_count.checked_sub(nulls))
                .unwrap_or(group_row_count);
            let locations = &offset_indexes.get(column)?.page_locations;
            for (i, location) in locations.iter().enumerate() {
                let offset = u64::try_from(location.offset).ok()?;
                let length = u64::try_from(location.compressed_page_size).ok()?;
                let range = offset..offset.checked_add(length)?;
                if !ranges.contains(&range) {
                    continue;
                }
                let first_row = u64::try_from(location.first_row_index).ok()?;
                let end_row = locations
                    .get(i + 1)
                    .map_or(Some(group_rows.end - group_rows.start), |p| {
                        u64::try_from(p.first_row_index).ok()
                    })?;
                let row_count = end_row.checked_sub(first_row)?;
                if range.end > self.file_size
                    || length == 0
                    || length > page::MAX_PAGE_BYTES as u64
                    || row_count == 0
                    || row_count > page::MAX_PAGE_ROWS as u64
                    || end_row > group_rows.end - group_rows.start
                {
                    return None;
                }
                // A raw Zstd frame cannot be smaller than its required PLAIN values.
                // These pages need the ordinary decoder, so do not probe them.
                if matches!(chunk.compression(), Compression::ZSTD(_))
                    && descriptor.max_def_level() == 0
                    && value_width.is_some_and(|width| length < row_count * width as u64)
                {
                    continue;
                }
                if value_width.is_none() {
                    let first = group_rows.start + first_row;
                    let selected = state.rows.partition_point(|row| *row < first + row_count)
                        - state.rows.partition_point(|row| *row < first);
                    if length < 1024 * 1024
                        || length < row_count * 64
                        || selected == 0
                        || selected as u64 > row_count / 8
                    {
                        continue;
                    }
                }
                pages.push(Page {
                    range,
                    first_file_row: group_rows.start + first_row,
                    row_count: row_count as usize,
                    max_value_count: row_count.min(non_null_rows) as usize,
                    compression: chunk.compression(),
                    value_width,
                    nullable: descriptor.max_def_level() == 1,
                });
                if pages.len() > MAX_PAGES {
                    return None;
                }
            }
        }
        (!pages.is_empty()).then(|| (pages, state.rows.clone()))
    }
}
