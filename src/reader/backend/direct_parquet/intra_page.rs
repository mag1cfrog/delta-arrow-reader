//! Experimental partial reads through parquet-rs's public reader and predicate APIs.
//!
//! The predicate records real file row numbers. Only non-predicate columns are
//! eligible, after filtering. The normal decoder still owns row selection and DV
//! positions; unread fixed-width values are zero-filled only where it will skip them.

mod page;

use std::{
    ops::Range,
    sync::{Arc, Mutex},
};

use arrow::{
    array::{Array, BooleanArray, Int64Array},
    error::ArrowError,
};
use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt, future::BoxFuture, stream};
use parquet::{
    arrow::{
        ProjectionMask,
        arrow_reader::ArrowReaderOptions,
        async_reader::{AsyncFileReader, ParquetObjectReader},
    },
    basic::{Compression, Type},
    errors::{ParquetError, Result},
    file::metadata::ParquetMetaData,
};
use tokio::sync::Semaphore;

use super::range_planning::{merge_ranges, range_bytes};
use page::{Page, ProbeError, RangeCache, selected_value_ranges};

// ponytail: fixed experimental bounds; #421 adds transport-aware plan selection.
const MAX_READ_BYTES: usize = 16 * 1024 * 1024;
const MAX_CONCURRENT_READS: usize = 512;
const MAX_REQUESTS: usize = 32_768;
const MAX_PAGES: usize = 4_096;
const MAX_SELECTED_ROWS: usize = 8_192;
const MAX_REQUESTED_BYTES: u128 = 128 * 1024 * 1024;
const MAX_PROBE_ROUNDS: usize = 20;
static READ_PERMITS: Semaphore = Semaphore::const_new(MAX_CONCURRENT_READS);

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
}

impl IntraPageReader {
    pub(super) fn new(
        inner: ParquetObjectReader,
        metadata: Arc<ParquetMetaData>,
        selection: Option<SharedSelection>,
        file_size: u64,
    ) -> Result<Self> {
        if let Some(selection) = &selection {
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
        })
    }

    fn candidate_pages(&self, ranges: &[Range<u64>]) -> Option<(Vec<Page>, Vec<u64>)> {
        if ranges.len() > MAX_REQUESTS
            || ranges
                .iter()
                .any(|r| r.start > r.end || r.end > self.file_size)
            || range_bytes(ranges) > MAX_REQUESTED_BYTES
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
            || state.rows.len() as u64 == group_rows.end - group_rows.start
        {
            return None;
        }
        let offset_indexes = self.metadata.offset_index()?.get(group_index)?;
        let row_group = self.metadata.row_group(group_index);
        let predicate_columns = state.predicate_columns.as_ref()?;
        let mut pages = Vec::new();
        for (column, chunk) in row_group.columns().iter().enumerate() {
            let descriptor = chunk.column_descr();
            if predicate_columns.leaf_included(column)
                || descriptor.physical_type() != Type::INT64
                || descriptor.path().parts().len() != 1
                || descriptor.max_def_level() != 1
                || descriptor.max_rep_level() != 0
                || chunk.dictionary_page_offset().is_some()
                || !matches!(
                    chunk.compression(),
                    Compression::UNCOMPRESSED | Compression::ZSTD(_)
                )
            {
                continue;
            }
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
                pages.push(Page {
                    range,
                    first_file_row: group_rows.start + first_row,
                    row_count: row_count as usize,
                    compression: chunk.compression(),
                });
                if pages.len() > MAX_PAGES {
                    return None;
                }
            }
        }
        (!pages.is_empty()).then(|| (pages, state.rows.clone()))
    }

    async fn fetch_ranges(&self, ranges: Vec<Range<u64>>, cache: &mut RangeCache) -> Result<()> {
        let data: Vec<_> = stream::iter(ranges)
            .map(|range| {
                let mut reader = self.inner.clone();
                async move {
                    let _permit = READ_PERMITS
                        .acquire()
                        .await
                        .map_err(|_| invalid_data("range limiter closed"))?;
                    let bytes = reader.get_bytes(range.clone()).await?;
                    if bytes.len() as u64 != range.end - range.start {
                        return Err(invalid_data("truncated range response"));
                    }
                    Ok((range, bytes))
                }
            })
            .buffer_unordered(MAX_CONCURRENT_READS)
            .try_collect()
            .await?;
        cache.entries.extend(data);
        Ok(())
    }

    async fn read_ranges(&mut self, ranges: Vec<Range<u64>>) -> Result<Vec<Bytes>> {
        let Some((pages, selected_rows)) = self.candidate_pages(&ranges) else {
            return self.inner.get_byte_ranges(ranges).await;
        };
        // Never spend the full ordinary payload on probes and gap filling.
        let byte_budget = MAX_READ_BYTES.min((range_bytes(&ranges) / 2) as usize);
        let mut cache = RangeCache::default();
        let mut request_count = 0;
        let mut next_ranges = pages
            .iter()
            .map(|p| p.range.start..(p.range.start + page::PAGE_PREFIX_BYTES).min(p.range.end))
            .collect::<Vec<_>>();
        'probes: for _ in 0..MAX_PROBE_ROUNDS {
            next_ranges = merge_ranges(&next_ranges, 0);
            request_count += next_ranges.len();
            if request_count > MAX_REQUESTS
                || cache.byte_len() as u128 + range_bytes(&next_ranges) > byte_budget as u128
            {
                break;
            }
            self.fetch_ranges(next_ranges, &mut cache).await?;
            let mut probe_ranges = Vec::new();
            let mut data_ranges = Vec::new();
            for page in &pages {
                if cache.get(&page.range).is_some() {
                    continue;
                }
                match selected_value_ranges(&cache, page, &selected_rows) {
                    Ok(ranges) => {
                        data_ranges.extend(ranges.into_iter().filter(|r| cache.get(r).is_none()))
                    }
                    Err(ProbeError::NeedBytes(range)) => probe_ranges.push(range),
                    Err(ProbeError::Unsupported) => probe_ranges.push(page.range.clone()),
                    Err(ProbeError::Invalid(reason)) => return Err(invalid_data(reason)),
                }
                if data_ranges.len() > MAX_REQUESTS {
                    break 'probes;
                }
            }
            if !probe_ranges.is_empty() {
                next_ranges = probe_ranges;
                continue;
            }
            data_ranges.extend(
                ranges
                    .iter()
                    .filter(|r| !pages.iter().any(|p| p.range == **r))
                    .cloned(),
            );
            let exact_ranges = merge_ranges(&data_ranges, 0);
            let remaining_bytes = byte_budget.saturating_sub(cache.byte_len());
            if range_bytes(&exact_ranges) > remaining_bytes as u128 {
                break;
            }
            let read_ranges = merge_ranges_with_budget(&exact_ranges, remaining_bytes);
            if request_count + read_ranges.len() > MAX_REQUESTS {
                break;
            }
            self.fetch_ranges(read_ranges, &mut cache).await?;
            tracing::debug!(target: "delta_arrow_reader::diagnostics::intra_page",
                pages = pages.len(), selected_rows = selected_rows.len(),
                original_bytes = %range_bytes(&ranges), fetched_bytes = cache.byte_len(),
                requests = cache.entries.len(), "experimental partial-page read");
            return Ok(ranges
                .iter()
                .map(|range| cache.read_with_zero_fill(range))
                .collect());
        }
        // The request or byte bound was reached. Reuse parquet-rs's complete-page path.
        self.inner.get_byte_ranges(ranges).await
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

// Merge the cheapest gaps first. Input ranges must be sorted and non-overlapping.
fn merge_ranges_with_budget(exact: &[Range<u64>], budget: usize) -> Vec<Range<u64>> {
    let Some(first) = exact.first() else {
        return Vec::new();
    };
    let mut remaining = (budget as u128).saturating_sub(range_bytes(exact));
    let mut gaps: Vec<_> = exact
        .windows(2)
        .enumerate()
        .map(|(i, w)| (w[1].start - w[0].end, i))
        .collect();
    gaps.sort_unstable();
    let mut merge = vec![false; gaps.len()];
    for (gap, index) in gaps {
        if u128::from(gap) > remaining {
            break;
        }
        remaining -= u128::from(gap);
        merge[index] = true;
    }
    let mut out = Vec::new();
    let mut current = first.clone();
    for (i, next) in exact.iter().enumerate().skip(1) {
        if merge[i - 1] {
            current.end = next.end;
        } else {
            out.push(current);
            current = next.clone();
        }
    }
    out.push(current);
    out
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
    #[allow(deprecated)]
    fn encode_page(
        body: &[u8],
        decoded_size: usize,
        rows: usize,
        crc: Option<i32>,
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
                WireEncoding::PLAIN,
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
    ) -> TestResult<(ColumnDescPtr, SerializedPageReader<Bytes>)> {
        use parquet::{
            file::metadata::ColumnChunkMetaData,
            schema::{parser::parse_message_type, types::SchemaDescriptor},
        };
        let schema = SchemaDescriptor::new(Arc::new(parse_message_type(
            "message test { optional int64 value; }",
        )?));
        let metadata = ColumnChunkMetaData::builder(schema.column(0))
            .set_compression(codec)
            .set_num_values(rows as i64)
            .set_total_compressed_size(bytes.len() as i64)
            .set_data_page_offset(0)
            .build()?;
        let reader = SerializedPageReader::new(Arc::new(bytes), &metadata, rows, None)?;
        Ok((schema.column(0), reader))
    }

    fn decompress_page(bytes: Bytes, rows: usize, codec: Compression) -> TestResult<Bytes> {
        Ok(standard_page_reader(bytes, rows, codec)?
            .1
            .get_next_page()?
            .ok_or("missing page")?
            .buffer()
            .clone())
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
            compression: Compression::UNCOMPRESSED,
        };
        let (desc, reader) = standard_page_reader(bytes.clone(), page.row_count, page.compression)?;
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

    #[test]
    fn intra_page_raw_zstd_block_boundaries_and_checksums() -> TestResult {
        // 20,000 non-null rows; deliberately split a value across two raw blocks.
        let mut body = vec![4, 0, 0, 0, 0xc0, 0xb8, 2, 1];
        for value in 0_i64..20_000 {
            body.extend_from_slice(&value.to_le_bytes());
        }
        let mut frame = vec![0x28, 0xb5, 0x2f, 0xfd, 0xa0];
        frame.extend_from_slice(&(body.len() as u32).to_le_bytes());
        let chunks = [&body[..131_069], &body[131_069..]];
        for (i, chunk) in chunks.iter().enumerate() {
            let block_header = ((chunk.len() as u32) << 3) | u32::from(i == 1);
            frame.extend_from_slice(&block_header.to_le_bytes()[..3]);
            frame.extend_from_slice(chunk);
        }
        let full = encode_page(&frame, body.len(), 20_000, None)?;
        let page = Page {
            range: 0..full.len() as u64,
            first_file_row: 50,
            row_count: 20_000,
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

    #[tokio::test]
    async fn intra_page_cancellation_and_range_failures_release_resources() -> TestResult {
        use super::super::tests::{GateRequest, GatedObjectStore, parquet_bytes};
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
        let inner = ParquetObjectReader::new(gated.clone(), path.clone()).with_file_size(size);
        let reader = Arc::new(super::IntraPageReader::new(
            inner,
            Arc::clone(&metadata),
            None,
            size,
        )?);
        let job_reader = Arc::clone(&reader);
        let job = tokio::spawn(async move {
            job_reader
                .fetch_ranges(
                    vec![0..size / 2, size / 2..size],
                    &mut RangeCache::default(),
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), gated.wait_started()).await?;
        job.abort();
        assert!(job.await.is_err_and(|error| error.is_cancelled()));
        assert!(gated.was_cancelled());
        let mut cache = RangeCache::default();
        // InMemory clips the end to the object size. Reject that short response
        // before it can become zero-filled selected values in a reconstructed page.
        let error = reader
            .fetch_ranges(vec![0..1, 1..size + 1], &mut cache)
            .await
            .err()
            .ok_or("short range response accepted")?;
        assert!(error.to_string().contains("truncated range response"));
        assert!(
            reader
                .fetch_ranges(vec![0..1, size..size + 1], &mut cache)
                .await
                .is_err()
        );
        assert!(cache.entries.is_empty());
        // A later failure must cancel an earlier blocked request immediately.
        let gated = GatedObjectStore::new(store, GateRequest::Range(1));
        gated.fail_range(2);
        let inner = ParquetObjectReader::new(gated.clone(), path).with_file_size(size);
        let reader = super::IntraPageReader::new(inner, metadata, None, size)?;
        let error = tokio::time::timeout(
            Duration::from_secs(5),
            reader.fetch_ranges(vec![0..size / 2, size / 2..size], &mut cache),
        )
        .await?
        .err()
        .ok_or("range failure swallowed")?;
        assert!(error.to_string().contains("injected range failure"));
        assert!(gated.was_cancelled());
        assert!(cache.entries.is_empty());
        drop(
            tokio::time::timeout(
                Duration::from_secs(5),
                super::READ_PERMITS.acquire_many(super::MAX_CONCURRENT_READS as u32),
            )
            .await??,
        );
        Ok(())
    }
}
