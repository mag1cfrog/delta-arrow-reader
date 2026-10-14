//! Experimental partial reads through parquet-rs's public reader and predicate APIs.
//!
//! The predicate records real file row numbers. Only non-predicate columns are
//! eligible, after filtering. The normal decoder still owns row selection and DV
//! positions; unread value payloads are zero-filled only where it will skip them.

mod delta_length;
mod page;
mod probe_read;

use std::{
    ops::Range,
    sync::{Arc, Mutex},
    time::Duration,
};

use arrow::{
    array::{Array, BooleanArray, Int64Array},
    error::ArrowError,
};
use bytes::Bytes;
use futures_util::future::BoxFuture;
use object_store::path::Path;
use parquet::{
    arrow::{
        ProjectionMask,
        arrow_reader::ArrowReaderOptions,
        async_reader::{AsyncFileReader, ParquetObjectReader},
    },
    basic::{Compression, ConvertedType, Encoding, LogicalType, Type},
    errors::{ParquetError, Result},
    file::metadata::ParquetMetaData,
};
use tokio::sync::Semaphore;

use super::{
    cost_model::{
        TransportEstimate, bandwidth_delay_bytes, dependent_read_cost_bytes, duration_cost_bytes,
        partial_read_cost_bytes, range_bytes, range_cost_bytes,
    },
    metered_object_store::MeteredParquetObjectStore,
    range_planning::{
        DECISION_MARGIN_PERCENT, RANGE_READ_PERMITS, choose_bounded_range_plan, choose_range_plan,
    },
};
use crate::reader::options::{
    MAX_CONCURRENT_PARQUET_RANGE_READS, MAX_INTRA_PAGE_READ_BYTES, MAX_SHARED_RANGE_READS,
};
use page::{Page, ProbeError, RangeCache, selected_value_ranges};

// Hard safety ceilings, not byte or concurrency targets for individual reads.
const MAX_REQUESTS: usize = 32_768;
const MAX_PAGES: usize = 4_096;
const MAX_SELECTED_ROWS: usize = 8_192;
const MAX_PROBE_ROUNDS: usize = 20;

pub(super) type SharedSelection = Arc<Mutex<SelectedRows>>;

#[derive(Default)]
pub(super) struct SelectedRows {
    // Absolute file row ranges. Predicate matches belong to the current group.
    row_groups: Vec<Range<u64>>,
    current_group: Option<usize>,
    rows: Vec<u64>,
    limit_exceeded: bool,
    pub(super) predicate_columns: Option<ProjectionMask>,
}

impl SelectedRows {
    pub(super) fn record_matches(
        &mut self,
        row_numbers: &Int64Array,
        selected: &BooleanArray,
    ) -> std::result::Result<(), ArrowError> {
        if row_numbers.len() != selected.len() || row_numbers.null_count() != 0 {
            return Err(ArrowError::ComputeError(
                "invalid intra-page row selection".into(),
            ));
        }
        for (index, keep) in row_numbers.values().iter().zip(selected.iter()) {
            let row = u64::try_from(*index)
                .map_err(|_| ArrowError::ComputeError("negative Parquet row number".into()))?;
            let group = self.row_groups.partition_point(|range| range.end <= row);
            if !self
                .row_groups
                .get(group)
                .is_some_and(|range| range.contains(&row))
            {
                return Err(ArrowError::ComputeError(
                    "Parquet row number outside file".into(),
                ));
            }
            if self.current_group != Some(group) {
                self.current_group = Some(group);
                self.rows.clear();
                self.limit_exceeded = false;
            }
            if keep == Some(true) && !self.limit_exceeded {
                if self.rows.len() == MAX_SELECTED_ROWS {
                    self.rows.clear();
                    self.limit_exceeded = true;
                } else {
                    self.rows.push(row);
                }
            }
        }
        Ok(())
    }
}

pub(super) struct IntraPageReader {
    inner: ParquetObjectReader,
    metadata: Arc<ParquetMetaData>,
    selection: Option<SharedSelection>,
    file_size: u64,
    store: Arc<MeteredParquetObjectStore>,
    path: Path,
}

impl IntraPageReader {
    pub(super) fn new(
        inner: ParquetObjectReader,
        metadata: Arc<ParquetMetaData>,
        selection: Option<SharedSelection>,
        file_size: u64,
        store: Arc<MeteredParquetObjectStore>,
        path: Path,
    ) -> Result<Self> {
        if let Some(selection) = &selection {
            for column in metadata.file_metadata().schema_descr().columns() {
                if column.physical_type() == Type::FIXED_LEN_BYTE_ARRAY && column.type_length() <= 0
                {
                    return Err(invalid_data("non-positive fixed-length value width"));
                }
            }
            let mut first = 0_u64;
            let mut state = selection
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for group in metadata.row_groups() {
                let count = u64::try_from(group.num_rows())
                    .map_err(|_| invalid_data("negative row count"))?;
                let end = first
                    .checked_add(count)
                    .ok_or_else(|| invalid_data("row count overflow"))?;
                state.row_groups.push(first..end);
                first = end;
            }
        }
        Ok(Self {
            inner,
            metadata,
            selection,
            file_size,
            store,
            path,
        })
    }

    fn candidate_pages(&self, ranges: &[Range<u64>]) -> Option<(Vec<Page>, Vec<u64>)> {
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

    async fn fetch_ranges(
        &self,
        requested_ranges: &[Range<u64>],
        ranges: Vec<Range<u64>>,
        cache: &mut RangeCache,
        budget: &Semaphore,
    ) -> Result<Option<Duration>> {
        let (data, elapsed) = self
            .store
            .read_partial_ranges(&self.path, requested_ranges, &ranges, budget)
            .await
            .map_err(|error| ParquetError::External(Box::new(error)))?;
        cache.entries.extend(ranges.into_iter().zip(data));
        Ok(elapsed)
    }

    async fn read_ranges(&mut self, ranges: Vec<Range<u64>>) -> Result<Vec<Bytes>> {
        let result = self.try_partial_read(&ranges).await?;
        match result {
            Some(bytes) => Ok(bytes),
            None => self.inner.get_byte_ranges(ranges).await,
        }
    }

    async fn try_partial_read(&self, ranges: &[Range<u64>]) -> Result<Option<Vec<Bytes>>> {
        if self.selection.is_none() {
            return Ok(None);
        }
        let Some((pages, selected_rows)) = self.candidate_pages(ranges) else {
            tracing::debug!(target: "delta_arrow_reader::diagnostics::intra_page",
                eligible = false, fallback_reason = "selection_or_layout_ineligible",
                "Partial-page read skipped");
            return Ok(None);
        };
        let Some(estimate) = self
            .store
            .transport_estimate()
            .filter(|estimate| estimate.shared_throughput_bytes_per_second > 0)
        else {
            tracing::debug!(target: "delta_arrow_reader::diagnostics::intra_page",
                eligible = true, fallback_reason = "insufficient_transport_evidence",
                "Partial-page read skipped");
            return Ok(None);
        };
        let (fixed, variable): (Vec<_>, Vec<_>) = pages
            .into_iter()
            .partition(|page| page.value_width.is_some());
        // Preserve an existing profitable fixed-width path before trying length discovery.
        if !fixed.is_empty()
            && let Some(bytes) = self
                .read_partial_pages(
                    ranges,
                    &fixed,
                    &selected_rows,
                    estimate,
                    &RANGE_READ_PERMITS,
                )
                .await?
        {
            return Ok(Some(bytes));
        }
        if variable.is_empty() {
            Ok(None)
        } else {
            probe_read::read(
                self,
                ranges,
                &variable,
                &selected_rows,
                estimate,
                &RANGE_READ_PERMITS,
            )
            .await
        }
    }

    async fn read_partial_pages(
        &self,
        ranges: &[Range<u64>],
        pages: &[Page],
        selected_rows: &[u64],
        estimate: TransportEstimate,
        budget: &Semaphore,
    ) -> Result<Option<Vec<Bytes>>> {
        let ordinary_plan = choose_range_plan(ranges, Some(estimate));
        let ordinary_cost = range_cost_bytes(
            &ordinary_plan.physical_ranges,
            estimate,
            MAX_CONCURRENT_PARQUET_RANGE_READS,
        );
        // Unsupported output columns still require complete reads. Charge their
        // known bytes before probing any candidate pages.
        let ordinary_ranges: Vec<_> = ranges
            .iter()
            .filter(|range| !pages.iter().any(|page| page.range == **range))
            .cloned()
            .collect();
        let ordinary_bytes = range_bytes(&ordinary_ranges);
        // Never spend more bytes on probes and data than the original page request.
        let byte_budget = range_bytes(ranges);
        let trace = |phase,
                     reason,
                     cost: u128,
                     bytes: u128,
                     requests: usize,
                     rounds: usize,
                     concurrency: usize| {
            tracing::debug!(target: "delta_arrow_reader::diagnostics::intra_page",
                eligible = true, phase, fallback_reason = reason,
                ordinary_predicted_cost_bytes = ordinary_cost,
                partial_predicted_cost_bytes = cost,
                planned_bytes = bytes, planned_requests = requests, probe_rounds = rounds,
                concurrency_limit = concurrency,
                "Partial-page read decision");
        };
        let Some(mut request_overhead_bytes) = self.store.request_overhead_bytes() else {
            // Latency and bandwidth alone do not price thousands of small requests.
            tracing::debug!(target: "delta_arrow_reader::diagnostics::intra_page",
                eligible = true, fallback_reason = "insufficient_request_capacity_evidence",
                "Partial-page read skipped");
            return Ok(None);
        };
        let Some(mut values) = estimated_value_ranges(pages, selected_rows) else {
            return Ok(None);
        };
        values.extend_from_slice(&ordinary_ranges);
        // Compare both plans in shared-capacity units. Splitting bandwidth
        // per file while keeping all request slots would underprice small reads.
        let concurrency = MAX_SHARED_RANGE_READS;
        let Some(plan) = choose_bounded_range_plan(
            &values,
            estimate,
            concurrency,
            byte_budget,
            request_overhead_bytes,
        ) else {
            return Ok(None);
        };
        let prefixes: Vec<_> = pages.iter().map(Page::prefix_range).collect();
        // Charge dependent rounds separately; raw-block headers are found one at a time.
        let mut extra_rounds = [(0_u128, 0_usize); MAX_PROBE_ROUNDS - 1];
        for page in pages {
            let Some(value_width) = page.value_width else {
                return Ok(None);
            };
            let (rounds, size) = match page.compression {
                Compression::ZSTD(_) => {
                    let bytes_after_prefix =
                        (page.range.end - page.range.start).saturating_sub(page::PAGE_PREFIX_BYTES);
                    let minimum_blocks =
                        bytes_after_prefix.div_ceil(page::MAX_ZSTD_BLOCK_BYTES as u64 + 3);
                    // The prefix may already hold the first block's header.
                    (minimum_blocks.saturating_sub(1) as usize, 3)
                }
                Compression::UNCOMPRESSED if page.nullable => {
                    // A large nullable page can still have a tiny RLE stream.
                    let structure_bytes = (page.range.end - page.range.start)
                        .saturating_sub(page.max_value_count as u64 * value_width as u64);
                    if structure_bytes <= page::PAGE_PREFIX_BYTES {
                        continue;
                    }
                    (1, u128::from(structure_bytes))
                }
                _ => continue,
            };
            if rounds >= MAX_PROBE_ROUNDS {
                tracing::debug!(target: "delta_arrow_reader::diagnostics::intra_page",
                    eligible = true, fallback_reason = "probe_round_limit",
                    minimum_probe_rounds = rounds + 1, "Partial-page read skipped");
                return Ok(None);
            }
            for (bytes, requests) in &mut extra_rounds[..rounds] {
                *bytes += size;
                *requests += 1;
            }
        }
        let extra_probe_bytes = extra_rounds.iter().map(|(bytes, _)| bytes).sum::<u128>();
        let extra_probes = extra_rounds
            .iter()
            .map(|(_, requests)| requests)
            .sum::<usize>();
        let cost = dependent_read_cost_bytes(
            [
                (range_bytes(&plan), plan.len()),
                (range_bytes(&prefixes), prefixes.len()),
            ]
            .into_iter()
            .chain(extra_rounds),
            estimate,
            concurrency,
            request_overhead_bytes,
        );
        if !partial_read_has_clear_savings(cost, ordinary_cost) {
            trace(
                "preflight",
                "profile_predicts_no_savings",
                cost,
                range_bytes(&plan) + range_bytes(&prefixes) + extra_probe_bytes,
                plan.len() + prefixes.len() + extra_probes,
                0,
                concurrency,
            );
            return Ok(None);
        }
        let mut cache = RangeCache::default();
        let mut request_count = 0;
        let mut total_cost = 0_u128;
        let mut probing = true;
        let mut next_ranges = pages.iter().map(Page::prefix_range).collect::<Vec<_>>();
        // Up to MAX_PROBE_ROUNDS dependent probe rounds, followed by one data round.
        for round in 0..=MAX_PROBE_ROUNDS {
            let phase = if probing { "probe" } else { "data" };
            if probing && round == MAX_PROBE_ROUNDS {
                trace(
                    phase,
                    "probe_round_limit",
                    total_cost,
                    cache.byte_len() as u128,
                    request_count,
                    round,
                    0,
                );
                return Ok(None);
            }
            let probe_rounds = round + usize::from(probing);
            let concurrency = MAX_SHARED_RANGE_READS;
            let Some(read_ranges) = choose_bounded_range_plan(
                &next_ranges,
                estimate,
                concurrency,
                byte_budget.saturating_sub(cache.byte_len() as u128),
                request_overhead_bytes,
            ) else {
                trace(
                    phase,
                    "byte_budget",
                    total_cost,
                    cache.byte_len() as u128,
                    request_count,
                    round,
                    concurrency,
                );
                return Ok(None);
            };
            let round_requests = read_ranges.len();
            request_count += round_requests;
            let round_cost = partial_read_cost_bytes(
                range_bytes(&read_ranges),
                round_requests,
                estimate,
                concurrency,
                request_overhead_bytes,
            );
            // Probes must leave room for at least one dependent data wave.
            // Charge each round separately, even when it has just one request.
            let predicted_cost = total_cost
                .saturating_add(round_cost)
                .saturating_add(if probing {
                    ordinary_bytes.saturating_add(bandwidth_delay_bytes(estimate))
                } else {
                    0
                });
            let reason = if request_count > MAX_REQUESTS {
                "request_budget"
            } else if !partial_read_has_clear_savings(predicted_cost, ordinary_cost) {
                "uncertain_savings"
            } else {
                "none"
            };
            trace(
                phase,
                reason,
                predicted_cost,
                cache.byte_len() as u128 + range_bytes(&read_ranges),
                request_count,
                probe_rounds,
                concurrency,
            );
            if reason != "none" {
                return Ok(None);
            }
            let elapsed = self
                .fetch_ranges(&next_ranges, read_ranges, &mut cache, budget)
                .await?;
            let observed_cost = elapsed.map(|elapsed| {
                duration_cost_bytes(elapsed, estimate.shared_throughput_bytes_per_second)
            });
            // Waiting behind another plan is not extra work performed by this one.
            total_cost = total_cost.saturating_add(observed_cost.unwrap_or(0).max(round_cost));
            request_overhead_bytes =
                request_overhead_bytes.max(self.store.request_overhead_bytes().unwrap_or(0));
            tracing::debug!(target: "delta_arrow_reader::diagnostics::intra_page",
                phase, observed_cost_bytes = observed_cost, request_overhead_bytes,
                "Partial-page read round completed");
            if !probing {
                return Ok(Some(
                    ranges
                        .iter()
                        .map(|range| cache.read_with_zero_fill(range))
                        .collect(),
                ));
            }
            let mut probe_ranges = Vec::new();
            let mut data_ranges = Vec::new();
            for page in pages {
                if cache.get(&page.range).is_some() {
                    continue;
                }
                match selected_value_ranges(&cache, page, selected_rows) {
                    Ok(ranges) => {
                        data_ranges.extend(ranges.into_iter().filter(|r| cache.get(r).is_none()))
                    }
                    Err(ProbeError::NeedBytes(range)) => probe_ranges.push(range),
                    Err(ProbeError::Unsupported) => probe_ranges.push(page.range.clone()),
                    Err(ProbeError::Invalid(reason)) => return Err(invalid_data(reason)),
                }
                if data_ranges.len() > MAX_REQUESTS {
                    trace(
                        "data",
                        "request_budget",
                        total_cost,
                        cache.byte_len() as u128,
                        request_count,
                        probe_rounds,
                        0,
                    );
                    return Ok(None);
                }
            }
            probing = !probe_ranges.is_empty();
            next_ranges = if probing {
                probe_ranges
            } else {
                data_ranges.extend_from_slice(&ordinary_ranges);
                data_ranges
            };
        }
        Ok(None)
    }
}

impl AsyncFileReader for IntraPageReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
        self.inner.get_bytes(range)
    }
    fn get_byte_ranges(&mut self, ranges: Vec<Range<u64>>) -> BoxFuture<'_, Result<Vec<Bytes>>> {
        Box::pin(self.read_ranges(ranges))
    }
    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, Result<Arc<ParquetMetaData>>> {
        self.inner.get_metadata(options)
    }
}

fn invalid_data(reason: &str) -> ParquetError {
    ParquetError::General(format!("invalid intra-page data: {reason}"))
}

/// Approximate value positions for cost comparison only; these ranges never perform I/O.
fn estimated_value_ranges(pages: &[Page], selected_rows: &[u64]) -> Option<Vec<Range<u64>>> {
    let mut ranges = Vec::new();
    for page in pages {
        // Variable lengths need their own discovery budget, not uniform row offsets.
        let value_width = page.value_width?;
        let first = selected_rows.partition_point(|row| *row < page.first_file_row);
        let end = page.first_file_row.checked_add(page.row_count as u64)?;
        for row in selected_rows[first..].iter().take_while(|row| **row < end) {
            // ponytail: assume evenly spread values; exact nullable/compressed offsets need probes.
            let offset = u128::from(row - page.first_file_row)
                * u128::from(page.range.end - page.range.start)
                / page.row_count as u128;
            let start = page.range.start + u64::try_from(offset).ok()?;
            ranges.push(start..start.saturating_add(value_width as u64).min(page.range.end));
            if ranges.len() > MAX_REQUESTS {
                return None;
            }
        }
    }
    Some(ranges)
}

// Prefer ordinary reads unless the gain exceeds the planner's uncertainty margin.
fn partial_read_has_clear_savings(partial_cost: u128, ordinary_cost: u128) -> bool {
    partial_cost.saturating_mul(100) < ordinary_cost.saturating_mul(100 - DECISION_MARGIN_PERCENT)
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::{
        column::page::PageReader, file::serialized_reader::SerializedPageReader,
        schema::types::ColumnDescPtr,
    };

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

    // Use Parquet's independent Thrift writer only to construct format controls.
    fn encode_page(
        body: &[u8],
        decoded_size: usize,
        rows: usize,
        crc: Option<i32>,
    ) -> TestResult<Bytes> {
        encode_page_with_encoding(
            body,
            decoded_size,
            rows,
            crc,
            parquet::basic::Encoding::PLAIN,
        )
    }

    #[allow(deprecated)]
    pub(super) fn encode_page_with_encoding(
        body: &[u8],
        decoded_size: usize,
        rows: usize,
        crc: Option<i32>,
        encoding: parquet::basic::Encoding,
    ) -> TestResult<Bytes> {
        use parquet::{
            format::{DataPageHeader, Encoding as WireEncoding, PageHeader, PageType},
            thrift::TSerializable,
        };
        let header = PageHeader {
            type_: PageType::DATA_PAGE,
            uncompressed_page_size: decoded_size.try_into()?,
            compressed_page_size: body.len().try_into()?,
            crc,
            data_page_header: Some(DataPageHeader::new(
                rows.try_into()?,
                WireEncoding(encoding as i32),
                WireEncoding::RLE,
                WireEncoding::RLE,
                None,
            )),
            index_page_header: None,
            dictionary_page_header: None,
            data_page_header_v2: None,
        };
        let mut bytes = Vec::new();
        header.write_to_out_protocol(&mut thrift::protocol::TCompactOutputProtocol::new(
            &mut bytes,
        ))?;
        bytes.extend_from_slice(body);
        Ok(bytes.into())
    }

    fn standard_page_reader(
        bytes: Bytes,
        rows: usize,
        codec: Compression,
        schema: &str,
    ) -> TestResult<(ColumnDescPtr, SerializedPageReader<Bytes>)> {
        use parquet::{
            file::metadata::ColumnChunkMetaData,
            schema::{parser::parse_message_type, types::SchemaDescriptor},
        };
        let schema = SchemaDescriptor::new(Arc::new(parse_message_type(schema)?));
        let metadata = ColumnChunkMetaData::builder(schema.column(0))
            .set_compression(codec)
            .set_num_values(rows as i64)
            .set_total_compressed_size(bytes.len() as i64)
            .set_data_page_offset(0)
            .build()?;
        let reader = SerializedPageReader::new(Arc::new(bytes), &metadata, rows, None)?;
        Ok((schema.column(0), reader))
    }

    fn raw_zstd_frame(body: &[u8], block_size: usize) -> Vec<u8> {
        let mut frame = vec![0x28, 0xb5, 0x2f, 0xfd, 0xa0];
        frame.extend_from_slice(&(body.len() as u32).to_le_bytes());
        let chunks = body.chunks(block_size);
        let count = chunks.len();
        for (i, chunk) in chunks.enumerate() {
            let header = ((chunk.len() as u32) << 3) | u32::from(i + 1 == count);
            frame.extend_from_slice(&header.to_le_bytes()[..3]);
            frame.extend_from_slice(chunk);
        }
        frame
    }

    fn decompress_page(bytes: Bytes, rows: usize, codec: Compression) -> TestResult<Bytes> {
        Ok(
            standard_page_reader(bytes, rows, codec, "message test { optional int64 value; }")?
                .1
                .get_next_page()?
                .ok_or("missing page")?
                .buffer()
                .clone(),
        )
    }

    #[test]
    fn intra_page_rejects_non_positive_schema_width() -> TestResult {
        use super::super::{metered_object_store::MultiRangeReadStrategy, tests::metrics};
        use object_store::memory::InMemory;
        use parquet::{
            file::metadata::FileMetaData,
            schema::{parser::parse_message_type, types::SchemaDescriptor},
        };
        // Parquet rejects negative widths, but its schema parser accepts zero.
        assert!(
            parse_message_type("message test { required fixed_len_byte_array(-1) value; }")
                .is_err()
        );
        let schema = Arc::new(SchemaDescriptor::new(Arc::new(parse_message_type(
            "message test { required fixed_len_byte_array(0) value; }",
        )?)));
        let metadata = Arc::new(ParquetMetaData::new(
            FileMetaData::new(1, 0, None, None, schema, None),
            vec![],
        ));
        let store = Arc::new(MeteredParquetObjectStore::new(
            Arc::new(InMemory::new()),
            metrics(),
            MultiRangeReadStrategy::ChooseAutomatically,
        ));
        let path = Path::from("invalid.parquet");
        let error = IntraPageReader::new(
            ParquetObjectReader::new(store.clone(), path.clone()),
            metadata,
            Some(Arc::new(Mutex::new(SelectedRows::default()))),
            0,
            store,
            path,
        )
        .err()
        .ok_or("zero-width values accepted")?;
        assert!(
            error
                .to_string()
                .contains("non-positive fixed-length value width")
        );
        Ok(())
    }

    #[test]
    fn intra_page_fixed_width_values_cross_raw_zstd_blocks() -> TestResult {
        use parquet::{column::reader::ColumnReaderImpl, data_type::FixedLenByteArrayType};
        for width in [3, 9, 16] {
            let rows = 50_000;
            let body: Vec<u8> = (0..rows * width).map(|i| (i % 251) as u8).collect();
            let frame = raw_zstd_frame(&body, 131_069);
            let full = encode_page(&frame, body.len(), rows, None)?;
            let page = Page {
                range: 0..full.len() as u64,
                first_file_row: 0,
                row_count: rows,
                max_value_count: rows,
                value_width: Some(width),
                nullable: false,
                compression: Compression::ZSTD(Default::default()),
            };
            let selected = [0, 131_069 / width as u64, rows as u64 - 1];
            let mut cache = RangeCache {
                entries: vec![(0..4096, full.slice(..4096))],
            };
            let ranges = loop {
                match selected_value_ranges(&cache, &page, &selected) {
                    Ok(ranges) => break ranges,
                    Err(ProbeError::NeedBytes(range)) => {
                        assert!(cache.entries.len() < MAX_PROBE_ROUNDS);
                        cache.entries.push((
                            range.clone(),
                            full.slice(range.start as usize..range.end as usize),
                        ));
                    }
                    Err(error) => {
                        return Err(format!("unexpected fixed-width probe: {error:?}").into());
                    }
                }
            };
            assert_eq!(
                ranges.len(),
                4,
                "one {width}-byte value crosses a block boundary"
            );
            for range in ranges {
                cache.entries.push((
                    range.clone(),
                    full.slice(range.start as usize..range.end as usize),
                ));
            }
            assert!(cache.byte_len() < full.len() / 2);
            let (desc, reader) = standard_page_reader(
                cache.read_with_zero_fill(&page.range),
                rows,
                page.compression,
                &format!("message test {{ required fixed_len_byte_array({width}) value; }}"),
            )?;
            let mut column = ColumnReaderImpl::<FixedLenByteArrayType>::new(desc, Box::new(reader));
            let mut values = Vec::new();
            assert_eq!(
                column.read_records(rows, None, None, &mut values)?,
                (rows, rows, rows)
            );
            for row in selected {
                let start = row as usize * width;
                assert_eq!(values[row as usize].data(), &body[start..start + width]);
            }
        }
        Ok(())
    }

    #[test]
    fn intra_page_padded_definition_levels_fall_back() -> TestResult {
        use parquet::{column::reader::ColumnReaderImpl, data_type::Int64Type};
        // Some writers pad the end of the levels stream with zero bytes. The
        // ordinary decoder accepts these, so leave this layout to that decoder.
        let mut body = vec![7, 0, 0, 0, 0xa0, 0x1f, 1, 0, 0, 0, 0];
        let expected: Vec<i64> = (0..2_000).collect();
        for value in &expected {
            body.extend_from_slice(&value.to_le_bytes());
        }
        let bytes = encode_page(&body, body.len(), expected.len(), None)?;
        let page = Page {
            range: 0..bytes.len() as u64,
            first_file_row: 0,
            row_count: expected.len(),
            max_value_count: expected.len(),
            value_width: Some(8),
            nullable: true,
            compression: Compression::UNCOMPRESSED,
        };
        let (desc, reader) = standard_page_reader(
            bytes.clone(),
            page.row_count,
            page.compression,
            "message test { optional int64 value; }",
        )?;
        let mut column = ColumnReaderImpl::<Int64Type>::new(desc, Box::new(reader));
        let (mut levels, mut values) = (Vec::new(), Vec::new());
        assert_eq!(
            column.read_records(page.row_count, Some(&mut levels), None, &mut values)?,
            (page.row_count, page.row_count, page.row_count)
        );
        assert_eq!(values, expected);
        assert_eq!(levels, vec![1; page.row_count]);
        let result = selected_value_ranges(
            &RangeCache {
                entries: vec![(page.range.clone(), bytes)],
            },
            &page,
            &[0, 1999],
        );
        assert!(matches!(result, Err(ProbeError::Unsupported)), "{result:?}");
        Ok(())
    }

    #[test]
    fn intra_page_valid_zstd_frame_sequences_fall_back() -> TestResult {
        // Zstd permits multiple frames in one compressed page, including skippable frames.
        let mut body = vec![3, 0, 0, 0, 0xa0, 0x1f, 1];
        for value in 0_i64..2_000 {
            body.extend_from_slice(&value.to_le_bytes());
        }
        let frame = |data: &[u8]| {
            let mut frame = vec![0x28, 0xb5, 0x2f, 0xfd, 0xa0];
            frame.extend_from_slice(&(data.len() as u32).to_le_bytes());
            frame.extend_from_slice(&((data.len() as u32 * 8) | 1).to_le_bytes()[..3]);
            frame.extend_from_slice(data);
            frame
        };
        let skip = [0x50, 0x2a, 0x4d, 0x18, 1, 0, 0, 0, 42];
        for compressed in [
            [frame(&body[..100]), frame(&body[100..])].concat(),
            [skip.to_vec(), frame(&body)].concat(),
            [frame(&body), skip.to_vec()].concat(),
            [frame(&body), frame(&[])].concat(),
        ] {
            let bytes = encode_page(&compressed, body.len(), 2_000, None)?;
            let page = Page {
                range: 0..bytes.len() as u64,
                first_file_row: 0,
                row_count: 2_000,
                max_value_count: 2_000,
                value_width: Some(8),
                nullable: true,
                compression: Compression::ZSTD(Default::default()),
            };
            assert_eq!(
                decompress_page(bytes.clone(), page.row_count, page.compression)?.as_ref(),
                body
            );
            let result = selected_value_ranges(
                &RangeCache {
                    entries: vec![(page.range.clone(), bytes)],
                },
                &page,
                &[0, 1999],
            );
            assert!(matches!(result, Err(ProbeError::Unsupported)), "{result:?}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn intra_page_raw_zstd_block_boundaries_and_checksums() -> TestResult {
        // 20,000 non-null rows; deliberately split a value across two raw blocks.
        let mut body = vec![4, 0, 0, 0, 0xc0, 0xb8, 2, 1];
        for value in 0_i64..20_000 {
            body.extend_from_slice(&value.to_le_bytes());
        }
        let frame = raw_zstd_frame(&body, 131_069);
        let full = encode_page(&frame, body.len(), 20_000, None)?;
        let page = Page {
            range: 0..full.len() as u64,
            first_file_row: 50,
            row_count: 20_000,
            max_value_count: 20_000,
            value_width: Some(8),
            nullable: true,
            compression: Compression::ZSTD(Default::default()),
        };
        let mut cache = RangeCache {
            entries: vec![(0..4096, full.slice(..4096))],
        };
        let selected = [50, 50 + 16_382, 50 + 19_999];
        let ranges = loop {
            match selected_value_ranges(&cache, &page, &selected) {
                Ok(ranges) => break ranges,
                Err(ProbeError::NeedBytes(range)) => {
                    assert!(cache.entries.len() < super::MAX_PROBE_ROUNDS);
                    let bytes = full.slice(range.start as usize..range.end as usize);
                    cache.entries.push((range, bytes));
                }
                Err(error) => return Err(format!("unexpected page probe: {error:?}").into()),
            }
        };
        let values: Vec<_> = ranges
            .iter()
            .flat_map(|r| full[r.start as usize..r.end as usize].iter().copied())
            .collect();
        assert_eq!(
            values,
            [0_i64, 16_382, 19_999]
                .into_iter()
                .flat_map(i64::to_le_bytes)
                .collect::<Vec<_>>()
        );
        assert_eq!(ranges.len(), 4, "one value crosses a raw block boundary");
        for range in ranges {
            cache.entries.push((
                range.clone(),
                full.slice(range.start as usize..range.end as usize),
            ));
        }
        assert!(cache.byte_len() < full.len() / 2);

        // Decode the reconstructed frame with the standard codec, including zero-filled skips.
        let reconstructed = cache.read_with_zero_fill(&page.range);
        let decoded = decompress_page(reconstructed, page.row_count, page.compression)?;
        for row in [0, 16_382, 19_999] {
            assert_eq!(
                decoded[8 + row * 8..8 + (row + 1) * 8],
                body[8 + row * 8..8 + (row + 1) * 8]
            );
        }
        // The second raw-block header requires a dependent request. Charging
        // probes as one parallel wave would wrongly approve the high-latency case.
        use super::super::{
            metered_object_store::MultiRangeReadStrategy,
            tests::{metrics, parquet_bytes},
        };
        use object_store::{ObjectStoreExt, memory::InMemory};
        use parquet::file::metadata::ParquetMetaDataReader;
        let memory = Arc::new(InMemory::new());
        let path = Path::from("data.parquet");
        memory.put(&path, full.clone().into()).await?;
        let metrics = metrics();
        let store = Arc::new(MeteredParquetObjectStore::new(
            memory.clone(),
            metrics.clone(),
            MultiRangeReadStrategy::ChooseAutomatically,
        ));
        let metadata = Arc::new(
            ParquetMetaDataReader::new().parse_and_finish(&Bytes::from(parquet_bytes()?))?,
        );
        let mut reader = IntraPageReader::new(
            ParquetObjectReader::new(store.clone(), path.clone()).with_file_size(full.len() as u64),
            metadata,
            None,
            full.len() as u64,
            store,
            path.clone(),
        )?;
        let budget = Semaphore::new(2);
        let estimate = TransportEstimate {
            request_latency: std::time::Duration::from_millis(80),
            shared_throughput_bytes_per_second: 1_000_000,
        };
        assert!(
            reader
                .read_partial_pages(
                    std::slice::from_ref(&page.range),
                    std::slice::from_ref(&page),
                    &selected,
                    estimate,
                    &budget
                )
                .await?
                .is_none()
        );
        assert_eq!(
            metrics.snapshot().parquet_data_file_bytes_received,
            Some(0),
            "missing request capacity must not trigger speculative probes"
        );
        // With a measured profile, the required second block probe can be
        // predicted from page size before either probe is issued.
        reader.store.seed_transport_estimate(estimate);
        reader.store.seed_request_overhead(Duration::ZERO);
        assert!(
            reader
                .read_partial_pages(
                    std::slice::from_ref(&page.range),
                    std::slice::from_ref(&page),
                    &selected,
                    estimate,
                    &budget,
                )
                .await?
                .is_none()
        );
        assert_eq!(
            metrics.snapshot().parquet_data_file_bytes_received,
            Some(0),
            "known block probes must be charged before I/O"
        );
        let estimate = TransportEstimate {
            request_latency: std::time::Duration::from_millis(1),
            ..estimate
        };
        let result = reader
            .read_partial_pages(
                std::slice::from_ref(&page.range),
                std::slice::from_ref(&page),
                &selected,
                estimate,
                &budget,
            )
            .await?
            .ok_or("profitable partial read rejected")?;
        let decoded = decompress_page(result[0].clone(), page.row_count, page.compression)?;
        for row in [0, 16_382, 19_999] {
            assert_eq!(
                decoded[8 + row * 8..8 + (row + 1) * 8],
                body[8 + row * 8..8 + (row + 1) * 8]
            );
        }
        assert_eq!(budget.available_permits(), 2);

        let snapshot = metrics.snapshot();
        assert_eq!(
            snapshot.parquet_data_file_physical_range_bytes_planned,
            snapshot.parquet_data_file_bytes_received
        );
        assert_eq!(
            snapshot.parquet_data_file_physical_range_requests_planned,
            snapshot.parquet_data_file_range_get_operations
        );

        // Most bytes belong to columns that cannot use partial reads. Their
        // known cost should rule out speculative probes before any I/O starts.
        let mixed_bytes = full.repeat(21);
        reader.file_size = mixed_bytes.len() as u64;
        memory.put(&path, mixed_bytes.into()).await?;
        let received_before = metrics.snapshot().parquet_data_file_bytes_received;
        assert!(
            reader
                .read_partial_pages(
                    &[page.range.clone(), page.range.end..reader.file_size],
                    std::slice::from_ref(&page),
                    &selected,
                    estimate,
                    &budget,
                )
                .await?
                .is_none()
        );
        assert_eq!(
            metrics.snapshot().parquet_data_file_bytes_received,
            received_before,
            "known full-read bytes must be charged before probing"
        );

        for (frame_checksum, page_crc) in [(true, None), (false, Some(1))] {
            let mut frame = frame.clone();
            if frame_checksum {
                frame[4] |= 4;
                frame.extend_from_slice(&[0; 4]);
            }
            let bytes = encode_page(&frame, body.len(), 20_000, page_crc)?;
            let page = Page {
                range: 0..bytes.len() as u64,
                ..page
            };
            let cache = RangeCache {
                entries: vec![(page.range.clone(), bytes)],
            };
            assert!(matches!(
                selected_value_ranges(&cache, &page, &selected),
                Err(ProbeError::Unsupported)
            ));
        }
        // Many raw blocks require many dependent probes, or exceed the probe cap.
        for (rows, latency_ms, profitable) in [
            (262_144, 200, false),
            (262_144, 1, true),
            (500_000, 1, false),
        ] {
            let body = vec![7; rows * 9];
            let frame = raw_zstd_frame(&body, page::MAX_ZSTD_BLOCK_BYTES);
            let bytes = encode_page(&frame, body.len(), rows, None)?;
            reader.file_size = bytes.len() as u64;
            memory.put(&path, bytes.into()).await?;
            let page = Page {
                range: 0..reader.file_size,
                first_file_row: 0,
                row_count: rows,
                max_value_count: rows,
                value_width: Some(9),
                nullable: false,
                compression: Compression::ZSTD(Default::default()),
            };
            let estimate = TransportEstimate {
                request_latency: Duration::from_millis(latency_ms),
                ..estimate
            };
            reader.store.seed_transport_estimate(estimate);
            reader.store.seed_request_overhead(Duration::ZERO);
            let before = metrics.snapshot().parquet_data_file_bytes_received.unwrap();
            let result = reader
                .read_partial_pages(
                    std::slice::from_ref(&page.range),
                    std::slice::from_ref(&page),
                    &[0, rows as u64 - 1],
                    estimate,
                    &budget,
                )
                .await?;
            assert_eq!(result.is_some(), profitable);
            let received = metrics.snapshot().parquet_data_file_bytes_received.unwrap() - before;
            if profitable {
                assert!(received < reader.file_size / 2);
                let decoded = decompress_page(result.unwrap()[0].clone(), rows, page.compression)?;
                assert_eq!(&decoded[..9], &body[..9]);
                assert_eq!(&decoded[body.len() - 9..], &body[body.len() - 9..]);
            } else {
                assert_eq!(received, 0, "known probe rounds must be charged before I/O");
            }
        }
        Ok(())
    }

    #[test]
    fn intra_page_malformed_levels_and_block_sizes_return_errors() -> TestResult {
        // Invalid RLE counts, value, varint, bit-packed length and trailing levels.
        for levels in [
            vec![],
            vec![0],
            vec![4, 1],
            vec![2, 2],
            vec![0x80; 5],
            vec![3],
            vec![2, 1, 2],
        ] {
            let mut body = (levels.len() as u32).to_le_bytes().to_vec();
            body.extend_from_slice(&levels);
            body.extend_from_slice(&0_i64.to_le_bytes());
            let bytes = encode_page(&body, body.len(), 1, None)?;
            let page = Page {
                range: 0..bytes.len() as u64,
                first_file_row: 0,
                row_count: 1,
                max_value_count: 1,
                value_width: Some(8),
                nullable: true,
                compression: Compression::UNCOMPRESSED,
            };
            assert!(matches!(
                selected_value_ranges(
                    &RangeCache {
                        entries: vec![(page.range.clone(), bytes)]
                    },
                    &page,
                    &[0]
                ),
                Err(ProbeError::Invalid(_))
            ));
        }
        // A raw block declares bytes past the end of its enclosing page.
        let frame = [0x28, 0xb5, 0x2f, 0xfd, 0x20, 14, 0x79, 0, 0];
        let bytes = encode_page(&frame, 14, 1, None)?;
        let page = Page {
            range: 0..bytes.len() as u64,
            first_file_row: 0,
            row_count: 1,
            max_value_count: 1,
            value_width: Some(8),
            nullable: true,
            compression: Compression::ZSTD(Default::default()),
        };
        assert!(matches!(
            selected_value_ranges(
                &RangeCache {
                    entries: vec![(page.range.clone(), bytes)]
                },
                &page,
                &[0]
            ),
            Err(ProbeError::Invalid(_))
        ));
        Ok(())
    }

    #[test]
    fn intra_page_fixed_width_lengths_and_row_boundaries() -> TestResult {
        for value_width in [1, 3, 4, 8, 9, 16, 31] {
            for nullable in [false, true] {
                // Three present values, optionally preceded by one RLE level run.
                let mut body = if nullable {
                    vec![2, 0, 0, 0, 6, 1]
                } else {
                    vec![]
                };
                body.extend(vec![17; 3 * value_width]);
                for size in [body.len() - 1, body.len(), body.len() + 1] {
                    let mut body = body.clone();
                    body.resize(size, 17);
                    let bytes = encode_page(&body, size, 3, None)?;
                    let page = Page {
                        range: 0..bytes.len() as u64,
                        first_file_row: 10,
                        row_count: 3,
                        max_value_count: 3,
                        value_width: Some(value_width),
                        nullable,
                        compression: Compression::UNCOMPRESSED,
                    };
                    let cache = RangeCache {
                        entries: vec![(page.range.clone(), bytes)],
                    };
                    let result = selected_value_ranges(&cache, &page, &[9, 12, 13]);
                    if size == 3 * value_width + if nullable { 6 } else { 0 } {
                        assert_eq!(
                            result.unwrap(),
                            vec![page.range.end - value_width as u64..page.range.end]
                        );
                        assert!(
                            selected_value_ranges(&cache, &page, &[])
                                .unwrap()
                                .is_empty()
                        );
                        for invalid_page in [
                            Page {
                                value_width: Some(usize::MAX),
                                range: page.range.clone(),
                                ..page
                            },
                            Page {
                                first_file_row: u64::MAX,
                                ..page
                            },
                        ] {
                            assert!(matches!(
                                selected_value_ranges(&cache, &invalid_page, &[10]),
                                Err(ProbeError::Invalid(_))
                            ));
                        }
                    } else {
                        assert!(matches!(result, Err(ProbeError::Invalid(_))));
                    }
                }
            }
        }
        Ok(())
    }

    #[test]
    fn intra_page_required_compressed_pages_skip_probes() -> TestResult {
        use super::super::{metered_object_store::MultiRangeReadStrategy, tests::metrics};
        use arrow::{
            array::{ArrayRef, Int32Array},
            datatypes::{Field, Schema},
            record_batch::RecordBatch,
        };
        use object_store::memory::InMemory;
        use parquet::{
            arrow::ArrowWriter,
            file::{
                metadata::{PageIndexPolicy, ParquetMetaDataReader},
                properties::WriterProperties,
            },
        };

        for values in [
            Arc::new(Int32Array::from_iter_values(0..16_384)) as ArrayRef,
            Arc::new(Int64Array::from_iter_values(0..16_384)),
        ] {
            for nullable in [false, true] {
                let schema = Arc::new(Schema::new(vec![Field::new(
                    "value",
                    values.data_type().clone(),
                    nullable,
                )]));
                let batch = RecordBatch::try_new(schema.clone(), vec![values.clone()])?;
                let properties = WriterProperties::builder()
                    .set_dictionary_enabled(false)
                    .set_compression(Compression::ZSTD(Default::default()))
                    .build();
                let mut writer = ArrowWriter::try_new(Vec::new(), schema, Some(properties))?;
                writer.write(&batch)?;
                let bytes = Bytes::from(writer.into_inner()?);
                let metadata = Arc::new(
                    ParquetMetaDataReader::new()
                        .with_offset_index_policy(PageIndexPolicy::Required)
                        .parse_and_finish(&bytes)?,
                );
                let locations = &metadata.offset_index().unwrap()[0][0].page_locations;
                assert_eq!(locations.len(), 1);
                let page = &locations[0];
                let range =
                    page.offset as u64..(page.offset + i64::from(page.compressed_page_size)) as u64;
                let store = Arc::new(MeteredParquetObjectStore::new(
                    Arc::new(InMemory::new()),
                    metrics(),
                    MultiRangeReadStrategy::ChooseAutomatically,
                ));
                let path = Path::from("compressed.parquet");
                let selection = Arc::new(Mutex::new(SelectedRows {
                    current_group: Some(0),
                    rows: vec![1],
                    predicate_columns: Some(ProjectionMask::roots(
                        metadata.file_metadata().schema_descr(),
                        [],
                    )),
                    ..Default::default()
                }));
                let reader = IntraPageReader::new(
                    ParquetObjectReader::new(store.clone(), path.clone()),
                    metadata,
                    Some(selection),
                    bytes.len() as u64,
                    store,
                    path,
                )?;
                // Required values cannot fit in these small pages as raw blocks.
                // Nullable pages still need inspection to discover their value count.
                assert_eq!(
                    reader
                        .candidate_pages(std::slice::from_ref(&range))
                        .is_some(),
                    nullable
                );
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn intra_page_nullable_rle_pages_keep_profitable_reads() -> TestResult {
        use super::super::{metered_object_store::MultiRangeReadStrategy, tests::metrics};
        use arrow::{
            datatypes::{DataType, Field, Schema},
            record_batch::RecordBatch,
        };
        use object_store::{ObjectStoreExt, memory::InMemory};
        use parquet::{
            arrow::ArrowWriter,
            file::{
                metadata::{PageIndexPolicy, ParquetMetaDataReader},
                properties::{EnabledStatistics, WriterProperties},
            },
        };

        let page_rows = 1_000_000;
        for page_count in [1, 2] {
            for null_rows in [0, 200_000] {
                for statistics in [EnabledStatistics::Page, EnabledStatistics::None] {
                    let rows = page_rows * page_count;
                    let schema = Arc::new(Schema::new(vec![Field::new(
                        "value",
                        DataType::Int64,
                        true,
                    )]));
                    let batch = RecordBatch::try_new(
                        schema.clone(),
                        vec![Arc::new(Int64Array::from_iter(
                            (0..rows).map(|i| (i % page_rows >= null_rows).then_some(i as i64)),
                        ))],
                    )?;
                    let properties = WriterProperties::builder()
                        .set_compression(Compression::UNCOMPRESSED)
                        .set_dictionary_enabled(false)
                        .set_statistics_enabled(statistics)
                        .set_offset_index_disabled(false)
                        .set_max_row_group_row_count(Some(rows))
                        .set_data_page_row_count_limit(page_rows)
                        .set_data_page_size_limit(page::MAX_PAGE_BYTES)
                        .set_write_batch_size(page_rows)
                        .build();
                    let mut writer = ArrowWriter::try_new(Vec::new(), schema, Some(properties))?;
                    writer.write(&batch)?;
                    let bytes = Bytes::from(writer.into_inner()?);
                    let metadata = Arc::new(
                        ParquetMetaDataReader::new()
                            .with_offset_index_policy(PageIndexPolicy::Required)
                            .parse_and_finish(&bytes)?,
                    );
                    let locations = &metadata.offset_index().unwrap()[0][0].page_locations;
                    assert_eq!(locations.len(), page_count);
                    let location = locations.last().unwrap();
                    let range = location.offset as u64
                        ..(location.offset + i64::from(location.compressed_page_size)) as u64;
                    assert!(range.end - range.start <= page::MAX_PAGE_BYTES as u64);
                    let memory = Arc::new(InMemory::new());
                    let path = Path::from("nullable-rle.parquet");
                    memory.put(&path, bytes.clone().into()).await?;
                    let metrics = metrics();
                    let store = Arc::new(MeteredParquetObjectStore::new(
                        memory,
                        metrics.clone(),
                        MultiRangeReadStrategy::ChooseAutomatically,
                    ));
                    store.seed_transport_estimate(TransportEstimate {
                        request_latency: Duration::from_millis(200),
                        shared_throughput_bytes_per_second: 18_750_000,
                    });
                    store.seed_request_overhead(Duration::from_millis(1));
                    let selection = Arc::new(Mutex::new(SelectedRows {
                        current_group: Some(0),
                        rows: vec![(rows - 1) as u64],
                        predicate_columns: Some(ProjectionMask::roots(
                            metadata.file_metadata().schema_descr(),
                            [],
                        )),
                        ..Default::default()
                    }));
                    let inner = ParquetObjectReader::new(store.clone(), path.clone())
                        .with_file_size(bytes.len() as u64);
                    let mut reader = IntraPageReader::new(
                        inner,
                        metadata,
                        Some(selection),
                        bytes.len() as u64,
                        store,
                        path,
                    )?;
                    let out = reader.read_ranges(vec![range]).await?;
                    assert_eq!(
                        &out[0][out[0].len() - 8..],
                        &(rows as i64 - 1).to_le_bytes()
                    );
                    // A long RLE run fits in the prefix, with or without null statistics.
                    assert_eq!(
                        metrics.snapshot().parquet_data_file_bytes_received,
                        Some(4_104),
                        "{page_count} pages, {null_rows} nulls per page, {statistics:?}"
                    );
                }
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn mixed_batches_preserve_profitable_fixed_width_reads() -> TestResult {
        use super::super::{
            metered_object_store::MultiRangeReadStrategy, network_profile::RangeReadCalibration,
            tests::metrics,
        };
        use arrow::{
            array::{Decimal128Array, StringArray},
            datatypes::{DataType, Field, Schema},
            record_batch::RecordBatch,
        };
        use object_store::{ObjectStoreExt, memory::InMemory};
        use parquet::{
            arrow::ArrowWriter,
            basic::Encoding,
            file::{
                metadata::{PageIndexPolicy, ParquetMetaDataReader},
                properties::WriterProperties,
            },
        };

        let rows = 16_384;
        let schema = Arc::new(Schema::new(vec![
            Field::new("fixed", DataType::Decimal128(38, 0), false),
            Field::new("text", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(
                    Decimal128Array::from_iter_values(0..rows as i128)
                        .with_precision_and_scale(38, 0)?,
                ),
                Arc::new(StringArray::from_iter_values(
                    (0..rows).map(|_| "x".repeat(64)),
                )),
            ],
        )?;
        let properties = WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .set_column_encoding("text".into(), Encoding::DELTA_LENGTH_BYTE_ARRAY)
            .set_offset_index_disabled(false)
            .set_data_page_row_count_limit(rows)
            .set_data_page_size_limit(page::MAX_PAGE_BYTES)
            .set_write_batch_size(rows)
            .build();
        let mut writer = ArrowWriter::try_new(Vec::new(), schema, Some(properties))?;
        writer.write(&batch)?;
        let bytes = Bytes::from(writer.into_inner()?);
        let metadata = Arc::new(
            ParquetMetaDataReader::new()
                .with_offset_index_policy(PageIndexPolicy::Required)
                .parse_and_finish(&bytes)?,
        );
        let ranges: Vec<_> = metadata.offset_index().unwrap()[0]
            .iter()
            .map(|index| {
                assert_eq!(index.page_locations.len(), 1);
                let page = &index.page_locations[0];
                page.offset as u64..(page.offset + i64::from(page.compressed_page_size)) as u64
            })
            .collect();
        for partial_latency in [
            None,
            Some(Duration::from_secs(10)),
            Some(Duration::from_millis(1)),
        ] {
            let memory = Arc::new(InMemory::new());
            let path = Path::from("mixed.parquet");
            memory.put(&path, bytes.clone().into()).await?;
            let metrics = metrics();
            let store = Arc::new(MeteredParquetObjectStore::new(
                memory,
                metrics.clone(),
                MultiRangeReadStrategy::ChooseAutomatically,
            ));
            store.seed_transport_estimate(TransportEstimate {
                request_latency: Duration::from_millis(1),
                shared_throughput_bytes_per_second: 100_000,
            });
            store.seed_request_overhead(Duration::ZERO);
            if let Some(partial_latency) = partial_latency {
                store.seed_range_read_calibration(RangeReadCalibration {
                    bytes_per_second: 100_000,
                    ordinary_request_latencies: vec![Duration::from_millis(1)].into(),
                    partial_request_latencies: vec![partial_latency].into(),
                    partial_request_durations: vec![partial_latency].into(),
                    small_reads: None,
                });
            }
            let selection = Arc::new(Mutex::new(SelectedRows {
                current_group: Some(0),
                rows: vec![(rows - 1) as u64],
                predicate_columns: Some(ProjectionMask::roots(
                    metadata.file_metadata().schema_descr(),
                    [],
                )),
                ..Default::default()
            }));
            let mut reader = IntraPageReader::new(
                ParquetObjectReader::new(store.clone(), path.clone()),
                metadata.clone(),
                Some(selection),
                bytes.len() as u64,
                store,
                path,
            )?;
            let (pages, _) = reader.candidate_pages(&ranges).unwrap();
            assert_eq!(
                pages
                    .iter()
                    .filter(|page| page.value_width.is_none())
                    .count(),
                1
            );
            let output = reader.read_ranges(ranges.clone()).await?;
            let fixed = &ranges[0];
            assert_eq!(
                &output[0][output[0].len() - 16..],
                &bytes[fixed.end as usize - 16..fixed.end as usize]
            );
            let text = &ranges[1];
            assert_eq!(
                output[1],
                bytes.slice(text.start as usize..text.end as usize)
            );
            let received = metrics.snapshot().parquet_data_file_bytes_received.unwrap();
            assert!(
                u128::from(received) * 10 < range_bytes(&ranges) * 9,
                "fixed-width savings were lost: partial_latency={partial_latency:?}, received={received}"
            );
        }
        Ok(())
    }

    fn assert_same_bits(
        actual: &arrow::record_batch::RecordBatch,
        expected: &arrow::record_batch::RecordBatch,
    ) {
        use arrow::array::{Float32Array, Float64Array};
        assert_eq!(actual.schema(), expected.schema());
        assert_eq!(actual.num_rows(), expected.num_rows());
        for (actual, expected) in actual.columns().iter().zip(expected.columns()) {
            // Arrow's numeric equality does not distinguish signed zero or NaN payloads.
            if let Some(actual) = actual.as_any().downcast_ref::<Float32Array>() {
                let expected = expected.as_any().downcast_ref::<Float32Array>().unwrap();
                assert!(
                    actual
                        .iter()
                        .map(|v| v.map(f32::to_bits))
                        .eq(expected.iter().map(|v| v.map(f32::to_bits)))
                );
            } else if let Some(actual) = actual.as_any().downcast_ref::<Float64Array>() {
                let expected = expected.as_any().downcast_ref::<Float64Array>().unwrap();
                assert!(
                    actual
                        .iter()
                        .map(|v| v.map(f64::to_bits))
                        .eq(expected.iter().map(|v| v.map(f64::to_bits)))
                );
            } else {
                assert_eq!(actual.to_data(), expected.to_data());
            }
        }
    }

    #[tokio::test]
    async fn variable_length_queries_preserve_values_nulls_and_physical_rows() -> TestResult {
        use super::super::{
            arrow_reader_options, metered_object_store::MultiRangeReadStrategy,
            network_profile::RangeReadCalibration, tests::metrics,
        };
        use arrow::{
            array::{BinaryArray, StringArray},
            compute::concat_batches,
            datatypes::{DataType, Field, Schema},
            record_batch::RecordBatch,
        };
        use futures_util::TryStreamExt;
        use object_store::{ObjectStoreExt, memory::InMemory};
        use parquet::{
            arrow::{
                ArrowWriter,
                arrow_reader::{ArrowPredicateFn, ArrowReaderMetadata, RowFilter, RowSelection},
                async_reader::ParquetRecordBatchStreamBuilder,
            },
            basic::Encoding,
            file::properties::{EnabledStatistics, WriterProperties, WriterVersion},
        };

        let rows = 16_384;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("text", DataType::Utf8, true),
            Field::new("binary", DataType::Binary, false),
            Field::new("ordinary", DataType::Boolean, false),
        ]));
        let strings: Vec<_> = (0..rows)
            .map(|row| {
                (row % 5 != 0).then(|| {
                    if row % 7 == 0 {
                        String::new()
                    } else {
                        format!("{row:05}-{}", "a".repeat(512 + row % 256))
                    }
                })
            })
            .collect();
        let binary: Vec<_> = (0..rows)
            .map(|row| vec![(row % 256) as u8; if row % 7 == 0 { 0 } else { 512 + row % 256 }])
            .collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(0..rows as i64)),
                Arc::new(StringArray::from_iter(strings.iter().map(|s| s.as_deref()))),
                Arc::new(BinaryArray::from_iter_values(binary.iter())),
                Arc::new(BooleanArray::from_iter(
                    (0..rows).map(|row| Some(row % 3 == 0)),
                )),
            ],
        )?;
        let economical = TransportEstimate {
            request_latency: Duration::from_millis(10),
            shared_throughput_bytes_per_second: 100_000,
        };
        // A physical row selection represents deleted rows before the predicate runs.
        let live_rows = RowSelection::from_consecutive_ranges(
            (0..rows)
                .filter(|row| row % 13 != 0)
                .map(|row| row..row + 1),
            rows,
        );
        for encoding in [Encoding::DELTA_LENGTH_BYTE_ARRAY, Encoding::PLAIN] {
            let properties = WriterProperties::builder()
                .set_writer_version(WriterVersion::PARQUET_1_0)
                .set_compression(Compression::UNCOMPRESSED)
                .set_dictionary_enabled(false)
                .set_column_encoding("text".into(), encoding)
                .set_column_encoding("binary".into(), encoding)
                .set_statistics_enabled(EnabledStatistics::Page)
                .set_max_row_group_row_count(Some(8_192))
                .set_data_page_size_limit(8 * 1024 * 1024)
                .set_data_page_row_count_limit(4_096)
                .set_write_batch_size(1_024)
                .build();
            let mut writer = ArrowWriter::try_new(Vec::new(), schema.clone(), Some(properties))?;
            writer.write(&batch)?;
            let bytes = Bytes::from(writer.into_inner()?);
            let memory = Arc::new(InMemory::new());
            let path = Path::from("variable.parquet");
            memory.put(&path, bytes.clone().into()).await?;
            for views in [false, true] {
                for dense in [false, true] {
                    let mut baseline = None;
                    // Missing calibration and fast transport must reject before any probe.
                    for mode in ["disabled", "missing_profile", "fast", "partial"] {
                        let metrics = metrics();
                        let store = Arc::new(MeteredParquetObjectStore::new(
                            memory.clone(),
                            metrics.clone(),
                            MultiRangeReadStrategy::ChooseAutomatically,
                        ));
                        if mode != "missing_profile" {
                            store.seed_range_read_calibration(RangeReadCalibration {
                                bytes_per_second: if mode == "fast" {
                                    1_000_000_000
                                } else {
                                    100_000
                                },
                                ordinary_request_latencies: vec![Duration::from_millis(10)].into(),
                                small_reads: None,
                                partial_request_latencies: vec![Duration::from_millis(100)].into(),
                                partial_request_durations: vec![Duration::from_millis(100)].into(),
                            });
                        }
                        store.seed_request_overhead(Duration::ZERO);
                        store.seed_transport_estimate(economical);
                        let mut inner = ParquetObjectReader::new(store.clone(), path.clone())
                            .with_file_size(bytes.len() as u64);
                        let options = arrow_reader_options(true, true)?;
                        let options = if views {
                            options.with_schema(Arc::new(Schema::new(vec![
                                Field::new("id", DataType::Int64, false),
                                Field::new("text", DataType::Utf8View, true),
                                Field::new("binary", DataType::BinaryView, false),
                                Field::new("ordinary", DataType::Boolean, false),
                            ])))
                        } else {
                            options
                        };
                        let metadata = ArrowReaderMetadata::load_async(&mut inner, options).await?;
                        let projection = ProjectionMask::roots(
                            metadata.metadata().file_metadata().schema_descr(),
                            [0],
                        );
                        let selection = (mode != "disabled").then(|| {
                            Arc::new(Mutex::new(SelectedRows {
                                predicate_columns: Some(projection.clone()),
                                ..Default::default()
                            }))
                        });
                        let reader = IntraPageReader::new(
                            inner,
                            metadata.metadata().clone(),
                            selection.clone(),
                            bytes.len() as u64,
                            store.clone(),
                            path.clone(),
                        )?;
                        let predicate = ArrowPredicateFn::new(projection, move |batch| {
                            // Fixture timings cannot stand in for real network measurements.
                            store.seed_transport_estimate(economical);
                            store.seed_request_overhead(Duration::ZERO);
                            let ids = batch
                                .column(0)
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap();
                            let mask = BooleanArray::from_iter(
                                ids.values()
                                    .iter()
                                    .map(|id| Some(id % if dense { 2 } else { 997 } == 0)),
                            );
                            if let Some(selection) = &selection {
                                let physical = batch
                                    .columns()
                                    .last()
                                    .unwrap()
                                    .as_any()
                                    .downcast_ref::<Int64Array>()
                                    .unwrap();
                                selection.lock().unwrap().record_matches(physical, &mask)?;
                            }
                            Ok(mask)
                        });
                        let builder =
                            ParquetRecordBatchStreamBuilder::new_with_metadata(reader, metadata)
                                .with_row_selection(live_rows.clone())
                                .with_row_filter(RowFilter::new(vec![Box::new(predicate)]));
                        let output_schema = builder.schema().clone();
                        let output = concat_batches(
                            &output_schema,
                            &builder.build()?.try_collect::<Vec<_>>().await?,
                        )?;
                        let received = metrics.snapshot().parquet_data_file_bytes_received.unwrap();
                        if let Some((expected, ordinary_bytes)) = &baseline {
                            assert_eq!(
                                &output, expected,
                                "mode={mode}, views={views}, dense={dense}, encoding={encoding:?}"
                            );
                            if mode == "partial"
                                && !dense
                                && encoding == Encoding::DELTA_LENGTH_BYTE_ARRAY
                            {
                                assert!(
                                    received * 4 < *ordinary_bytes * 3,
                                    "partial I/O did not save bytes: {received} vs {ordinary_bytes}"
                                );
                            } else {
                                // Unsupported PLAIN chunks are rejected using their metadata.
                                assert_eq!(
                                    received, *ordinary_bytes,
                                    "mode={mode}, dense={dense}, encoding={encoding:?}"
                                );
                            }
                        } else {
                            let ids = output
                                .column(0)
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap();
                            assert_eq!(
                                ids.values().as_ref(),
                                &(0..rows as i64)
                                    .filter(
                                        |id| id % 13 != 0 && id % if dense { 2 } else { 997 } == 0
                                    )
                                    .collect::<Vec<_>>()
                            );
                            baseline = Some((output, received));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn intra_page_cost_selection_preserves_values_and_rejects_unprofitable_reads()
    -> TestResult {
        use super::super::{
            arrow_reader_options, metered_object_store::MultiRangeReadStrategy, tests::metrics,
        };
        use arrow::{
            array::{
                ArrayRef, Date32Array, Decimal128Array, FixedSizeBinaryArray, Float32Array,
                Float64Array, Int32Array, TimestampMicrosecondArray, make_array, new_null_array,
            },
            buffer::NullBuffer,
            compute::concat_batches,
            datatypes::{DataType, Field, Schema},
            record_batch::RecordBatch,
        };
        use futures_util::TryStreamExt;
        use object_store::{ObjectStoreExt, memory::InMemory};
        use parquet::{
            arrow::{
                ArrowWriter,
                arrow_reader::{ArrowPredicateFn, ArrowReaderMetadata, RowFilter},
                async_reader::ParquetRecordBatchStreamBuilder,
            },
            file::properties::{EnabledStatistics, WriterProperties},
        };
        use std::time::Duration;

        // Multiple row groups, nullable random payloads, all-null pages, and a
        // predicate column excluded from reconstruction exercise the real decoder.
        let rows = 65_539;
        let mut random = 19_u64;
        let bits: Vec<_> = (0..rows)
            .map(|_| {
                random ^= random << 13;
                random ^= random >> 7;
                random ^= random << 17;
                random
            })
            .collect();
        // Include NaN payloads, infinities and signed zero. Compare their bits below.
        let f32_special = [
            0x7fc00001, 0x7fa00002, 0x7f800000, 0xff800000, 0x80000000, 0,
        ];
        let f64_special = [
            0x7ff8000000000001,
            0x7ff0000000000002,
            0x7ff0000000000000,
            0xfff0000000000000,
            0x8000000000000000,
            0,
        ];
        let mut floats: Vec<_> = bits.iter().map(|v| f32::from_bits(*v as u32)).collect();
        let mut doubles: Vec<_> = bits.iter().map(|v| f64::from_bits(*v)).collect();
        for (i, value) in f32_special.into_iter().enumerate() {
            floats[i * 8_191] = f32::from_bits(value);
        }
        for (i, value) in f64_special.into_iter().enumerate() {
            doubles[i * 8_191] = f64::from_bits(value);
        }
        let mut payloads = vec![
            Arc::new(Int32Array::from_iter_values(bits.iter().map(|v| *v as i32))) as ArrayRef,
            Arc::new(Int64Array::from_iter_values(bits.iter().map(|v| *v as i64))),
            Arc::new(Float32Array::from(floats)),
            Arc::new(Float64Array::from(doubles)),
            Arc::new(Date32Array::from_iter_values(
                bits.iter().map(|v| (*v % 100_000) as i32),
            )),
            Arc::new(TimestampMicrosecondArray::from_iter_values(
                bits.iter().map(|v| (*v % 1_000_000_000_000_000) as i64),
            )),
            Arc::new(
                Decimal128Array::from_iter_values(
                    bits.iter().map(|v| (*v % 1_000_000_000) as i128),
                )
                .with_precision_and_scale(9, 2)?,
            ),
            Arc::new(
                Decimal128Array::from_iter_values(
                    bits.iter()
                        .map(|v| (*v % 1_000_000_000_000_000_000) as i128),
                )
                .with_precision_and_scale(18, 2)?,
            ),
        ];
        for precision in [20, 38] {
            payloads.push(Arc::new(
                Decimal128Array::from_iter_values(bits.iter().enumerate().map(|(i, value)| {
                    let signed = ((*value as i128) << 64) | bits[(i + rows / 2) % rows] as i128;
                    signed % 10_i128.pow(precision.into())
                }))
                .with_precision_and_scale(precision, 4)?,
            ));
        }
        for width in [3, 9, 16] {
            payloads.push(Arc::new(FixedSizeBinaryArray::try_from_iter(
                bits.iter().enumerate().map(|(i, value)| {
                    let mut bytes = value.to_le_bytes().to_vec();
                    bytes.extend_from_slice(&bits[(i + rows / 2) % rows].to_le_bytes());
                    bytes.truncate(width);
                    bytes
                }),
            )?));
        }
        for payload in payloads {
            let dtype = payload.data_type().clone();
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("nullable", dtype.clone(), true),
                Field::new("required", dtype.clone(), false),
                Field::new("empty", dtype.clone(), true),
            ]));
            let nullable = make_array(
                payload
                    .to_data()
                    .into_builder()
                    .nulls(Some(NullBuffer::from(
                        bits.iter().map(|v| v & 1 == 0).collect::<Vec<_>>(),
                    )))
                    .build()?,
            );
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from_iter_values(0..rows as i64)),
                    nullable,
                    payload,
                    new_null_array(&dtype, rows),
                ],
            )?;
            let economical = TransportEstimate {
                request_latency: Duration::from_millis(1),
                shared_throughput_bytes_per_second: 1_000_000,
            };
            let expensive = TransportEstimate {
                request_latency: Duration::from_secs(1),
                ..economical
            };
            let long_levels = TransportEstimate {
                // Wider values save more transfer time, so use a costlier probe
                // to keep this a control where the second read removes the gain.
                request_latency: Duration::from_millis(
                    if matches!(
                        dtype,
                        DataType::Decimal128(19.., _) | DataType::FixedSizeBinary(9..)
                    ) {
                        800
                    } else {
                        400
                    },
                ),
                ..economical
            };
            for (codec, group_rows) in [
                (Compression::UNCOMPRESSED, 16_384),
                (Compression::ZSTD(Default::default()), 16_384),
                (Compression::UNCOMPRESSED, 65_536),
            ] {
                // Logical values have constrained ranges that Zstd compresses; the
                // primitive cases above cover raw Zstd blocks for both widths.
                if codec != Compression::UNCOMPRESSED
                    && !matches!(
                        dtype,
                        DataType::Int32
                            | DataType::Int64
                            | DataType::Float32
                            | DataType::Float64
                            | DataType::FixedSizeBinary(_)
                    )
                {
                    continue;
                }
                let properties = WriterProperties::builder()
                    .set_compression(codec)
                    .set_dictionary_enabled(false)
                    .set_statistics_enabled(EnabledStatistics::Page)
                    .set_max_row_group_row_count(Some(group_rows))
                    .set_data_page_row_count_limit(group_rows)
                    .set_write_batch_size(1_024)
                    .build();
                let mut writer =
                    ArrowWriter::try_new(Vec::new(), schema.clone(), Some(properties))?;
                writer.write(&batch)?;
                let bytes = Bytes::from(writer.into_inner()?);
                let memory = Arc::new(InMemory::new());
                let path = Path::from("data.parquet");
                memory.put(&path, bytes.clone().into()).await?;
                for dense in [false, true] {
                    let mut baseline = None;
                    for (enabled, estimate, overhead) in [
                        (false, None, None),
                        (true, None, None),
                        (true, Some(economical), None),
                        (true, Some(economical), Some(Duration::ZERO)),
                        (true, Some(expensive), Some(Duration::ZERO)),
                        // A nullable 65K-row page needs a second structural read.
                        // Reject before probing when that round removes the gain.
                        (true, Some(long_levels), Some(Duration::ZERO)),
                        (true, Some(economical), Some(Duration::from_secs(1))),
                    ] {
                        let metrics = metrics();
                        let store = Arc::new(MeteredParquetObjectStore::new(
                            memory.clone(),
                            metrics.clone(),
                            MultiRangeReadStrategy::ChooseAutomatically,
                        ));
                        if let Some(estimate) = estimate {
                            store.seed_transport_estimate(estimate);
                        }
                        if let Some(overhead) = overhead {
                            store.seed_request_overhead(overhead);
                        }
                        let mut inner = ParquetObjectReader::new(store.clone(), path.clone())
                            .with_file_size(bytes.len() as u64);
                        let metadata = ArrowReaderMetadata::load_async(
                            &mut inner,
                            arrow_reader_options(true, true)?,
                        )
                        .await?;
                        let projection = ProjectionMask::roots(
                            metadata.metadata().file_metadata().schema_descr(),
                            [0],
                        );
                        let selection = enabled.then(|| {
                            Arc::new(Mutex::new(SelectedRows {
                                predicate_columns: Some(projection.clone()),
                                ..Default::default()
                            }))
                        });
                        let reader = IntraPageReader::new(
                            inner,
                            metadata.metadata().clone(),
                            selection.clone(),
                            bytes.len() as u64,
                            store.clone(),
                            path.clone(),
                        )?;
                        let predicate = ArrowPredicateFn::new(projection, move |batch| {
                            // Keep evidence deterministic at the decision point;
                            // in-memory fixture timings are not network measurements.
                            if let Some(estimate) = estimate {
                                store.seed_transport_estimate(estimate);
                            }
                            let ids = batch
                                .column(0)
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .expect("id");
                            let mask =
                                BooleanArray::from_iter(ids.values().iter().map(|id| {
                                    Some(if dense { id % 2 == 0 } else { id % 8_191 == 0 })
                                }));
                            if let Some(selection) = &selection {
                                let row_numbers = batch
                                    .columns()
                                    .last()
                                    .expect("virtual column")
                                    .as_any()
                                    .downcast_ref::<Int64Array>()
                                    .expect("row numbers");
                                selection
                                    .lock()
                                    .unwrap()
                                    .record_matches(row_numbers, &mask)?;
                            }
                            Ok(mask)
                        });
                        let builder =
                            ParquetRecordBatchStreamBuilder::new_with_metadata(reader, metadata)
                                .with_row_filter(RowFilter::new(vec![Box::new(predicate)]));
                        let output_schema = builder.schema().clone();
                        let batches = builder.build()?.try_collect::<Vec<_>>().await?;
                        let output = concat_batches(&output_schema, &batches)?;
                        let received = metrics
                            .snapshot()
                            .parquet_data_file_bytes_received
                            .ok_or("missing byte metrics")?;
                        if let Some((expected, original_bytes)) = &baseline {
                            assert_same_bits(&output, expected);

                            if !dense
                                && estimate == Some(economical)
                                && overhead == Some(Duration::ZERO)
                            {
                                assert!(
                                    received * 4 < *original_bytes * 3,
                                    "partial reads must save payload bytes for {dtype:?}, {codec:?}: {received} vs {original_bytes}"
                                );
                            } else {
                                assert_eq!(
                                    received, *original_bytes,
                                    "ordinary path should avoid speculative I/O: {dtype:?}, {codec:?}, {group_rows}, dense={dense}, {estimate:?}, {overhead:?}"
                                );
                            }
                        } else {
                            baseline = Some((output, received));
                        }
                    }
                }
            }
        }
        assert!(partial_read_has_clear_savings(89, 100));
        assert!(!partial_read_has_clear_savings(90, 100));
        assert!(!partial_read_has_clear_savings(u128::MAX, u128::MAX));
        Ok(())
    }

    #[tokio::test]
    async fn intra_page_cancellation_and_range_failures_release_resources() -> TestResult {
        use super::super::{
            metered_object_store::MultiRangeReadStrategy,
            tests::{GateRequest, GatedObjectStore, metrics, parquet_bytes},
        };
        use object_store::{ObjectStoreExt, memory::InMemory, path::Path};
        use parquet::{
            arrow::async_reader::ParquetObjectReader, file::metadata::ParquetMetaDataReader,
        };
        use std::time::Duration;
        let bytes = Bytes::from(parquet_bytes()?);
        let metadata = Arc::new(ParquetMetaDataReader::new().parse_and_finish(&bytes)?);
        let store = Arc::new(InMemory::new());
        let path = Path::from("data.parquet");
        let size = bytes.len() as u64;
        store.put(&path, bytes.into()).await?;
        let gated = GatedObjectStore::new(store.clone(), GateRequest::Range(1));
        let metered = Arc::new(MeteredParquetObjectStore::new(
            gated.clone(),
            metrics(),
            MultiRangeReadStrategy::ChooseAutomatically,
        ));
        let inner = ParquetObjectReader::new(metered.clone(), path.clone()).with_file_size(size);
        let reader = Arc::new(super::IntraPageReader::new(
            inner,
            Arc::clone(&metadata),
            None,
            size,
            metered,
            path.clone(),
        )?);
        let budget = Arc::new(Semaphore::new(2));
        let job_budget = budget.clone();
        let job_reader = Arc::clone(&reader);
        let job = tokio::spawn(async move {
            job_reader
                .fetch_ranges(
                    &[0..size / 2, size / 2..size],
                    vec![0..size / 2, size / 2..size],
                    &mut RangeCache::default(),
                    &job_budget,
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), gated.wait_started()).await?;
        job.abort();
        assert!(job.await.is_err_and(|error| error.is_cancelled()));
        assert!(gated.was_cancelled());
        assert_eq!(budget.available_permits(), 2);
        let mut cache = RangeCache::default();
        // InMemory clips the end to the object size. Reject that short response
        // before it can become zero-filled selected values in a reconstructed page.
        let error = reader
            .fetch_ranges(
                &[0..1, 1..size + 1],
                vec![0..1, 1..size + 1],
                &mut cache,
                &budget,
            )
            .await
            .err()
            .ok_or("short range response accepted")?;
        assert!(error.to_string().contains("unexpected length"));
        assert!(
            reader
                .fetch_ranges(
                    &[0..1, size..size + 1],
                    vec![0..1, size..size + 1],
                    &mut cache,
                    &budget
                )
                .await
                .is_err()
        );
        assert!(cache.entries.is_empty());
        // A later failure must cancel an earlier blocked request immediately.
        let gated = GatedObjectStore::new(store, GateRequest::Range(1));
        gated.fail_range(2);
        let metered = Arc::new(MeteredParquetObjectStore::new(
            gated.clone(),
            metrics(),
            MultiRangeReadStrategy::ChooseAutomatically,
        ));
        let inner = ParquetObjectReader::new(metered.clone(), path.clone()).with_file_size(size);
        let reader = super::IntraPageReader::new(inner, metadata, None, size, metered, path)?;
        let error = tokio::time::timeout(
            Duration::from_secs(5),
            reader.fetch_ranges(
                &[0..size / 2, size / 2..size],
                vec![0..size / 2, size / 2..size],
                &mut cache,
                &budget,
            ),
        )
        .await?
        .err()
        .ok_or("range failure swallowed")?;
        assert!(error.to_string().contains("injected range failure"));
        assert!(gated.was_cancelled());
        assert!(cache.entries.is_empty());
        assert_eq!(budget.available_permits(), 2);
        Ok(())
    }
}
