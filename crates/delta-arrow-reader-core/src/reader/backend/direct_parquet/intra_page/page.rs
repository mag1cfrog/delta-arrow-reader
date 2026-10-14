//! Inspect bounded page structure to locate selected values without reading the rest.

use std::ops::Range;

use bytes::Bytes;
use parquet::basic::{Compression, Encoding};
use thrift::protocol::TType;

use super::super::compact_thrift::{invalid_metadata, read_field, read_i64};
use super::{
    MAX_SELECTED_ROWS,
    delta_length::{LengthError, MAX_PREFIX_BYTES, decode_lengths},
};

pub(super) const MAX_PAGE_BYTES: usize = 8 * 1024 * 1024;
pub(super) const MAX_PAGE_ROWS: usize = 1024 * 1024;
pub(super) const PAGE_PREFIX_BYTES: u64 = 4096;
pub(super) const MAX_ZSTD_BLOCK_BYTES: usize = 131_072;
const DELTA_LENGTH_PREFIX_BYTES: u64 = 8 * 1024;

pub(super) struct Page {
    pub(super) range: Range<u64>,
    pub(super) first_file_row: u64,
    pub(super) row_count: usize,
    // Upper bound from the row group's null count; this page may contain fewer values.
    pub(super) max_value_count: usize,
    pub(super) compression: Compression,
    // None denotes a variable-length BYTE_ARRAY column.
    pub(super) value_width: Option<usize>,
    // The file's definition level controls its layout, even if the target schema differs.
    pub(super) nullable: bool,
}

impl Page {
    pub(super) fn prefix_range(&self) -> Range<u64> {
        let bytes = if self.value_width.is_some() {
            PAGE_PREFIX_BYTES
        } else {
            DELTA_LENGTH_PREFIX_BYTES
        };
        self.range.start..self.range.start.saturating_add(bytes).min(self.range.end)
    }
}

#[derive(Debug)]
pub(super) enum ProbeError {
    NeedBytes(Range<u64>),
    Unsupported,
    Invalid(&'static str),
}
use ProbeError::{Invalid, NeedBytes, Unsupported};

#[derive(Default)]
pub(super) struct RangeCache {
    pub(super) entries: Vec<(Range<u64>, Bytes)>,
}

impl RangeCache {
    pub(super) fn byte_len(&self) -> usize {
        self.entries.iter().map(|(_, bytes)| bytes.len()).sum()
    }

    pub(super) fn get(&self, range: &Range<u64>) -> Option<Bytes> {
        self.entries.iter().find_map(|(cached_range, bytes)| {
            (cached_range.start <= range.start && range.end <= cached_range.end).then(|| {
                let start = (range.start - cached_range.start) as usize;
                bytes.slice(start..start + (range.end - range.start) as usize)
            })
        })
    }

    fn read(&self, range: Range<u64>) -> Result<Bytes, ProbeError> {
        self.get(&range).ok_or(NeedBytes(range))
    }

    // Adjacent probes may arrive out of order. Never turn a gap into structural bytes.
    fn contiguous_prefix(&self, range: Range<u64>) -> Bytes {
        let mut entries: Vec<_> = self
            .entries
            .iter()
            .filter(|(r, _)| r.start < range.end && r.end > range.start)
            .collect();
        entries.sort_unstable_by_key(|(r, _)| r.start);
        let mut end = range.start;
        for (r, _) in entries {
            if r.start > end {
                break;
            }
            end = end.max(r.end.min(range.end));
        }
        self.read_with_zero_fill(&(range.start..end))
    }

    // All control bytes and selected values must be fetched first. Missing bytes
    // must belong to value payloads that the decoder will discard.
    pub(super) fn read_with_zero_fill(&self, range: &Range<u64>) -> Bytes {
        if let Some(bytes) = self.get(range) {
            return bytes;
        }
        let mut out = vec![0; (range.end - range.start) as usize];
        for (cached_range, bytes) in &self.entries {
            let start = cached_range.start.max(range.start);
            let end = cached_range.end.min(range.end);
            if start < end {
                out[(start - range.start) as usize..(end - range.start) as usize].copy_from_slice(
                    &bytes[(start - cached_range.start) as usize
                        ..(end - cached_range.start) as usize],
                );
            }
        }
        out.into()
    }
}

/// `selected_rows` contains sorted physical file row numbers, including DV holes.
pub(super) fn selected_value_ranges(
    cache: &RangeCache,
    page: &Page,
    selected_rows: &[u64],
) -> Result<Vec<Range<u64>>, ProbeError> {
    let prefix = cache.read(page.prefix_range())?;
    let PageHeader {
        header_len,
        compressed_size,
        uncompressed_size,
        row_count,
        encoding,
    } = parse_page_header(&prefix)?;
    match (page.value_width, encoding, page.compression) {
        (Some(_), Encoding::PLAIN, _) => (),
        (None, Encoding::DELTA_LENGTH_BYTE_ARRAY, Compression::UNCOMPRESSED) => (),
        _ => return Err(Unsupported),
    }
    if row_count != page.row_count {
        return Err(Invalid("page row count"));
    }
    let body = page
        .range
        .start
        .checked_add(header_len as u64)
        .ok_or(Invalid("page offset overflow"))?;
    if body.checked_add(compressed_size as u64) != Some(page.range.end) {
        return Err(Invalid("page boundary mismatch"));
    }
    if page.compression == Compression::UNCOMPRESSED && compressed_size != uncompressed_size {
        return Err(Invalid("uncompressed page length"));
    }
    let Some(value_width) = page.value_width else {
        return delta_length_value_ranges(cache, page, body, selected_rows);
    };
    let blocks = match page.compression {
        Compression::UNCOMPRESSED => vec![RawBlock {
            decoded_range: 0..uncompressed_size,
            file_offset: body,
        }],
        Compression::ZSTD(_) => parse_zstd_blocks(cache, body..page.range.end, uncompressed_size)?,
        _ => return Err(Unsupported),
    };
    let (valid, value_start) = if page.nullable {
        if uncompressed_size < 4 {
            return Err(Invalid("definition levels size"));
        }
        let length = read_page_bytes(cache, &blocks, 0..4)?;
        let levels_len = u32::from_le_bytes([length[0], length[1], length[2], length[3]]) as usize;
        if levels_len > uncompressed_size - 4 {
            return Err(Invalid("definition levels size"));
        }
        let levels = read_page_bytes(cache, &blocks, 4..4 + levels_len)?;
        (
            decode_definition_levels(&levels, page.row_count)?,
            4 + levels_len,
        )
    } else {
        // Required columns omit both the definition levels and their length prefix.
        (vec![true; page.row_count], 0)
    };
    // Check the largest possible value area once so the row loop cannot overflow.
    page.row_count
        .checked_mul(value_width)
        .and_then(|bytes| value_start.checked_add(bytes))
        .ok_or(Invalid("PLAIN value size overflow"))?;
    let mut value_end = value_start;
    let value_offsets: Vec<_> = valid
        .iter()
        .map(|is_valid| {
            let offset = value_end;
            value_end += usize::from(*is_valid) * value_width;
            offset
        })
        .collect();
    if value_end != uncompressed_size {
        return Err(Invalid("PLAIN value count"));
    }
    let end_row = page
        .first_file_row
        .checked_add(page.row_count as u64)
        .ok_or(Invalid("page row offset overflow"))?;
    let mut ranges = Vec::new();
    for row in selected_rows
        .iter()
        .copied()
        .filter(|r| page.first_file_row <= *r && *r < end_row)
    {
        let row = (row - page.first_file_row) as usize;
        if valid[row] {
            let offset = value_offsets[row];
            ranges.extend(map_to_file_ranges(&blocks, offset..offset + value_width)?);
        }
    }
    Ok(ranges)
}

fn delta_length_value_ranges(
    cache: &RangeCache,
    page: &Page,
    body: u64,
    selected_rows: &[u64],
) -> Result<Vec<Range<u64>>, ProbeError> {
    if selected_rows.len() > MAX_SELECTED_ROWS {
        return Err(Unsupported);
    }
    page.first_file_row
        .checked_add(page.row_count as u64)
        .ok_or(Invalid("page row offset overflow"))?;
    let limit = page
        .range
        .start
        .saturating_add(MAX_PREFIX_BYTES as u64)
        .min(page.range.end);
    let prefix = cache.contiguous_prefix(body..limit);
    let available = body + prefix.len() as u64;
    let need_bytes = |end: usize| {
        let Some(required) = body.checked_add(end as u64) else {
            return Invalid("structure offset overflow");
        };
        if required > page.range.end {
            return Invalid("truncated page structure");
        }
        if required > limit {
            return Unsupported;
        }
        // Grow geometrically, but fetch only the missing tail of the prefix.
        let next = page
            .range
            .start
            .saturating_add((available - page.range.start) * 2)
            .min(limit)
            .max(required);
        NeedBytes(available..next)
    };
    let (valid, value_start) = if page.nullable {
        let size = prefix.get(..4).ok_or_else(|| need_bytes(4))?;
        let length = u32::from_le_bytes([size[0], size[1], size[2], size[3]]) as usize;
        let end = length
            .checked_add(4)
            .ok_or(Invalid("definition levels size"))?;
        let levels = prefix.get(4..end).ok_or_else(|| need_bytes(end))?;
        (decode_definition_levels(levels, page.row_count)?, end)
    } else {
        (vec![true; page.row_count], 0)
    };
    let count = valid.iter().filter(|v| **v).count();
    let (length_end, lengths) = decode_lengths(
        prefix.slice(value_start..),
        count,
        (page.range.end - body) as usize - value_start,
    )
    .map_err(|error| match error {
        LengthError::NeedBytes(end) => need_bytes(value_start + end),
        LengthError::Unsupported => Unsupported,
        LengthError::Invalid(reason) => Invalid(reason),
    })?;

    let mut offset = body + (value_start + length_end) as u64;
    let mut lengths = lengths.into_iter();
    let mut selected = selected_rows.iter().copied().peekable();
    let mut ranges = Vec::new();
    for (row, valid) in valid.into_iter().enumerate() {
        let file_row = page.first_file_row + row as u64;
        while selected.peek().is_some_and(|selected| *selected < file_row) {
            selected.next();
        }
        if valid {
            let length = lengths.next().ok_or(Invalid("length count"))? as u64;
            let end = offset + length;
            if length != 0 && selected.peek() == Some(&file_row) {
                ranges.push(offset..end);
            }
            offset = end;
        }
    }
    Ok(ranges)
}

fn read_i32_field(mut bytes: &[u8], id: i16) -> thrift::Result<Option<i32>> {
    read_field(&mut bytes, id, TType::I32, |cursor| {
        i32::try_from(read_i64(cursor)?).map_err(|_| invalid_metadata())
    })
}

struct PageHeader {
    header_len: usize,
    compressed_size: usize,
    uncompressed_size: usize,
    row_count: usize,
    encoding: Encoding,
}

// Reuse the allocation-free, depth-limited Compact Thrift parser used for footer
// probes. A header larger than the prefix is left to the normal Parquet reader.
fn parse_page_header(bytes: &[u8]) -> Result<PageHeader, ProbeError> {
    let read = || -> thrift::Result<_> {
        let page_type = read_i32_field(bytes, 1)?;
        let compressed = read_i32_field(bytes, 3)?;
        let uncompressed = read_i32_field(bytes, 2)?;
        let checksum = read_i32_field(bytes, 4)?.is_some();
        let mut cursor = bytes;
        let data = read_field(&mut cursor, 5, TType::Struct, |cursor| {
            let start = *cursor;
            let count = read_field(cursor, 1, TType::I32, read_i64)?;
            Ok((
                count,
                read_i32_field(start, 2)?,
                read_i32_field(start, 3)?,
                read_i32_field(start, 4)?,
            ))
        })?;
        Ok((
            bytes.len() - cursor.len(),
            page_type,
            compressed,
            uncompressed,
            checksum,
            data,
        ))
    };
    let (length, kind, compressed, uncompressed, checksum, data) =
        read().map_err(|_| Unsupported)?;
    // Parquet wire values: DATA_PAGE = 0, PLAIN = 0, DELTA_LENGTH_BYTE_ARRAY = 6, RLE = 3.
    if checksum || kind != Some(0) {
        return Err(Unsupported);
    }
    let Some((count, Some(encoding), Some(3), Some(3))) = data else {
        return Err(Unsupported);
    };
    let encoding = match encoding {
        0 => Encoding::PLAIN,
        6 => Encoding::DELTA_LENGTH_BYTE_ARRAY,
        _ => return Err(Unsupported),
    };
    let compressed = compressed
        .and_then(|v| usize::try_from(v).ok())
        .ok_or(Invalid("compressed page size"))?;
    let uncompressed = uncompressed
        .and_then(|v| usize::try_from(v).ok())
        .ok_or(Invalid("uncompressed page size"))?;
    let count = count
        .and_then(|v| usize::try_from(v).ok())
        .ok_or(Invalid("page value count"))?;
    if uncompressed > MAX_PAGE_BYTES || count > MAX_PAGE_ROWS {
        return Err(Unsupported);
    }
    Ok(PageHeader {
        header_len: length,
        compressed_size: compressed,
        uncompressed_size: uncompressed,
        row_count: count,
        encoding,
    })
}

// Offsets in the decoded page body, mapped to absolute file bytes.
struct RawBlock {
    decoded_range: Range<usize>,
    file_offset: u64,
}

// Translate offsets in the decoded page body to file offsets. A value can span blocks.
fn map_to_file_ranges(
    blocks: &[RawBlock],
    range: Range<usize>,
) -> Result<Vec<Range<u64>>, ProbeError> {
    let mut out = Vec::new();
    for block in blocks {
        let start = block.decoded_range.start.max(range.start);
        let end = block.decoded_range.end.min(range.end);
        if start < end {
            let file_start = block.file_offset + (start - block.decoded_range.start) as u64;
            out.push(file_start..file_start + (end - start) as u64);
        }
    }
    if out.iter().map(|r| r.end - r.start).sum::<u64>() != (range.end - range.start) as u64 {
        return Err(Invalid("logical range outside raw blocks"));
    }
    Ok(out)
}

fn read_page_bytes(
    cache: &RangeCache,
    blocks: &[RawBlock],
    range: Range<usize>,
) -> Result<Vec<u8>, ProbeError> {
    let mut out = Vec::with_capacity(range.len());
    for r in map_to_file_ranges(blocks, range)? {
        out.extend_from_slice(&cache.read(r)?);
    }
    Ok(out)
}

fn parse_zstd_blocks(
    cache: &RangeCache,
    body: Range<u64>,
    uncompressed_size: usize,
) -> Result<Vec<RawBlock>, ProbeError> {
    let mut blocks = Vec::new();
    let frame = cache.read(body.start..(body.start + 18).min(body.end))?;
    if frame.len() >= 4
        && u32::from_le_bytes([frame[0], frame[1], frame[2], frame[3]]) & !15 == 0x184d_2a50
    {
        return Err(Unsupported); // A legal skippable frame needs the ordinary codec.
    }
    if frame.len() < 5 || frame[..4] != [0x28, 0xb5, 0x2f, 0xfd] {
        return Err(Invalid("Zstd frame header"));
    }
    let descriptor = frame[4];
    // RFC 8878 reserves bit 3 but requires decoders to ignore bit 4.
    if descriptor & 0b0000_1000 != 0 {
        return Err(Invalid("reserved Zstd frame flags"));
    }
    // Dictionary ID or content checksum.
    if descriptor & 0b0000_0111 != 0 {
        return Err(Unsupported);
    }
    let single_segment = descriptor & 0b0010_0000 != 0;
    let content_size_len = [usize::from(single_segment), 2, 4, 8][(descriptor >> 6) as usize];
    let content_size_start = 5 + usize::from(!single_segment);
    let size_bytes = frame
        .get(content_size_start..content_size_start + content_size_len)
        .ok_or(Invalid("truncated Zstd frame"))?;
    if content_size_len > 0 {
        let mut encoded = [0; 8];
        encoded[..content_size_len].copy_from_slice(size_bytes);
        let size = u64::from_le_bytes(encoded)
            .checked_add(if content_size_len == 2 { 256 } else { 0 })
            .ok_or(Invalid("Zstd size overflow"))?;
        if size < uncompressed_size as u64 {
            return Err(Unsupported); // The page may concatenate multiple frames.
        }
        if size > uncompressed_size as u64 {
            return Err(Invalid("Zstd frame size mismatch"));
        }
    }
    let mut offset = body.start + (content_size_start + content_size_len) as u64;
    let mut decoded = 0;
    loop {
        if offset + 3 > body.end {
            return Err(Invalid("truncated Zstd block header"));
        }
        let bytes = cache.read(offset..offset + 3)?;
        let header = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], 0]);
        match (header >> 1) & 3 {
            0 => (),
            3 => return Err(Invalid("reserved Zstd block type")),
            _ => return Err(Unsupported),
        }
        let size = (header >> 3) as usize;
        if size > MAX_ZSTD_BLOCK_BYTES
            || offset + 3 + size as u64 > body.end
            || decoded + size > uncompressed_size
        {
            return Err(Invalid("Zstd raw block size"));
        }
        blocks.push(RawBlock {
            decoded_range: decoded..decoded + size,
            file_offset: offset + 3,
        });
        decoded += size;
        offset += 3 + size as u64;
        if header & 1 != 0 {
            break;
        }
        if blocks.len() > 128 {
            return Err(Unsupported);
        }
    }
    if offset < body.end {
        return Err(Unsupported); // Another ordinary or skippable frame may follow.
    }
    if offset != body.end || decoded != uncompressed_size {
        return Err(Invalid("Zstd frame boundary mismatch"));
    }
    Ok(blocks)
}

// Nullable, non-repeated columns have exactly one definition bit per row.
// Decode only this bounded one-bit subset of Parquet's RLE/bit-packed format.
fn decode_definition_levels(mut bytes: &[u8], count: usize) -> Result<Vec<bool>, ProbeError> {
    let mut out = Vec::with_capacity(count);
    while out.len() < count {
        let mut header = 0_u32;
        for shift in (0..35).step_by(7) {
            let (&byte, rest) = bytes
                .split_first()
                .ok_or(Invalid("truncated definition run"))?;
            bytes = rest;
            if shift == 28 && byte > 15 {
                return Err(Invalid("definition run overflow"));
            }
            header |= u32::from(byte & 127) << shift;
            if byte & 128 == 0 {
                break;
            }
        }
        let length = (header >> 1) as usize;
        if length == 0 {
            return Err(Invalid("empty definition run"));
        }
        let remaining = count - out.len();
        if header & 1 == 0 {
            let (&value, rest) = bytes
                .split_first()
                .ok_or(Invalid("truncated definition value"))?;
            if value > 1 || length > remaining {
                return Err(Invalid("invalid definition run"));
            }
            bytes = rest;
            out.resize(out.len() + length, value != 0);
        } else {
            let packed = bytes
                .get(..length)
                .ok_or(Invalid("truncated packed definitions"))?;
            if length > remaining.div_ceil(8) {
                return Err(Invalid("too many packed definitions"));
            }
            let bits = (length * 8).min(remaining);
            out.extend((0..bits).map(|i| packed[i / 8] & (1 << (i % 8)) != 0));
            bytes = &bytes[length..];
        }
    }
    if !bytes.is_empty() {
        // The ordinary reader accepts zero padding emitted by some writers.
        return Err(if bytes.iter().all(|byte| *byte == 0) {
            Unsupported
        } else {
            Invalid("trailing definition levels")
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::{
        array::{Array, ArrayRef, BinaryArray, UInt32Array},
        compute::{cast, concat, take},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use parquet::{
        arrow::{
            ArrowWriter,
            arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowSelection},
        },
        data_type::Int32Type,
        encodings::encoding::{DeltaBitPackEncoder, Encoder},
        file::{
            metadata::PageIndexPolicy,
            properties::{WriterProperties, WriterVersion},
        },
    };

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

    fn read_selected_array(
        bytes: Bytes,
        schema: Arc<Schema>,
        selected: &[u64],
        rows: usize,
    ) -> TestResult<ArrayRef> {
        let batches = ParquetRecordBatchReaderBuilder::try_new_with_options(
            bytes,
            ArrowReaderOptions::new().with_schema(schema),
        )?
        .with_row_selection(RowSelection::from_consecutive_ranges(
            selected.iter().map(|r| *r as usize..*r as usize + 1),
            rows,
        ))
        .with_batch_size(3)
        .build()?
        .collect::<Result<Vec<_>, _>>()?;
        let arrays: Vec<&dyn Array> = batches.iter().map(|b| b.column(0).as_ref()).collect();
        Ok(concat(&arrays)?)
    }

    fn sparse_page(full: &Bytes, page: &Page, selected: &[u64]) -> TestResult<Bytes> {
        let prefix = page.prefix_range();
        let mut cache = RangeCache {
            entries: vec![(
                prefix.clone(),
                full.slice(prefix.start as usize..prefix.end as usize),
            )],
        };
        let mut probes = 1;
        let ranges = loop {
            match selected_value_ranges(&cache, page, selected) {
                Ok(ranges) => break ranges,
                Err(NeedBytes(range)) => {
                    assert!(probes < 9);
                    assert!(range.start < range.end && range.end <= page.range.end);
                    assert!(cache.entries.iter().all(|(r, _)| r.end <= range.start));
                    cache.entries.push((
                        range.clone(),
                        full.slice(range.start as usize..range.end as usize),
                    ));
                    probes += 1;
                }
                Err(error) => return Err(format!("unexpected probe: {error:?}").into()),
            }
        };
        for range in ranges {
            if cache.get(&range).is_none() {
                cache.entries.push((
                    range.clone(),
                    full.slice(range.start as usize..range.end as usize),
                ));
            }
        }
        if page.row_count >= 8192 {
            assert!(probes > 1, "the length stream must cross the first prefix");
            assert!(cache.byte_len() < (page.range.end - page.range.start) as usize / 10);
        }
        Ok(cache.read_with_zero_fill(&page.range))
    }

    #[test]
    fn delta_length_sparse_pages_preserve_arrow_values_and_views() -> TestResult {
        let rows = 16_387;
        let selected = [
            0, 1, 12, 23, 4096, 8191, 8192, 8193, 16_383, 16_384, 16_385, 16_386,
        ];
        for nullable in [false, true] {
            for utf8 in [false, true] {
                let values: Vec<Option<Vec<u8>>> = (0..rows)
                    .map(|row| {
                        if nullable && row % 23 == 0 {
                            None
                        } else if row % 17 == 1 {
                            Some(vec![])
                        } else if utf8 {
                            Some(format!("{row}:{}", "é".repeat(row * 7919 % 256)).into_bytes())
                        } else {
                            Some(vec![(row % 256) as u8; row * 7919 % 512])
                        }
                    })
                    .collect();
                let binary = BinaryArray::from_iter(values.iter().map(|v| v.as_deref()));
                let data_type = if utf8 {
                    DataType::Utf8
                } else {
                    DataType::Binary
                };
                let array = cast(&binary, &data_type)?;
                let schema = Arc::new(Schema::new(vec![Field::new(
                    "value",
                    data_type.clone(),
                    nullable,
                )]));
                let batch = RecordBatch::try_new(schema.clone(), vec![array.clone()])?;
                let properties = WriterProperties::builder()
                    .set_writer_version(WriterVersion::PARQUET_1_0)
                    .set_dictionary_enabled(false)
                    .set_encoding(Encoding::DELTA_LENGTH_BYTE_ARRAY)
                    .set_compression(Compression::UNCOMPRESSED)
                    .set_write_batch_size(8192)
                    .set_data_page_row_count_limit(8192)
                    .set_data_page_size_limit(MAX_PAGE_BYTES)
                    .set_max_row_group_row_count(Some(16_384))
                    .build();
                let mut writer = ArrowWriter::try_new(Vec::new(), schema, Some(properties))?;
                writer.write(&batch)?;
                let full = Bytes::from(writer.into_inner()?);
                let reader = ParquetRecordBatchReaderBuilder::try_new_with_options(
                    full.clone(),
                    ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required),
                )?;
                let metadata = reader.metadata();
                let indexes = metadata.offset_index().ok_or("missing offset indexes")?;
                let mut reconstructed = full.to_vec();
                let mut bad_utf8_byte = None;
                let mut first_row = 0;
                let mut pages = 0;
                for (group_index, group) in metadata.row_groups().iter().enumerate() {
                    let locations = &indexes[group_index][0].page_locations;
                    for (i, location) in locations.iter().enumerate() {
                        let next_row = locations
                            .get(i + 1)
                            .map_or(group.num_rows(), |p| p.first_row_index);
                        let page = Page {
                            range: location.offset as u64
                                ..(location.offset + i64::from(location.compressed_page_size))
                                    as u64,
                            first_file_row: first_row + location.first_row_index as u64,
                            row_count: (next_row - location.first_row_index) as usize,
                            max_value_count: (next_row - location.first_row_index) as usize,
                            compression: Compression::UNCOMPRESSED,
                            value_width: None,
                            nullable,
                        };
                        let bytes = sparse_page(&full, &page, &selected)?;
                        if utf8 && page.first_file_row == 0 {
                            let cache = RangeCache {
                                entries: vec![(page.range.clone(), bytes.clone())],
                            };
                            let ranges = selected_value_ranges(&cache, &page, &[12])
                                .map_err(|e| format!("unexpected probe: {e:?}"))?;
                            bad_utf8_byte = Some(
                                ranges.first().ok_or("missing selected value")?.start as usize,
                            );
                        }
                        reconstructed[page.range.start as usize..page.range.end as usize]
                            .copy_from_slice(&bytes);
                        assert!(
                            super::super::estimated_value_ranges(
                                std::slice::from_ref(&page),
                                &selected
                            )
                            .is_none()
                        );
                        pages += 1;
                    }
                    first_row += group.num_rows() as u64;
                }
                assert_eq!(metadata.num_row_groups(), 2);
                assert_eq!(pages, 3);
                let expected = take(
                    array.as_ref(),
                    &UInt32Array::from(selected.map(|r| r as u32).to_vec()),
                    None,
                )?;
                for output_type in [
                    data_type.clone(),
                    if utf8 {
                        DataType::Utf8View
                    } else {
                        DataType::BinaryView
                    },
                ] {
                    let output_schema = Arc::new(Schema::new(vec![Field::new(
                        "value",
                        output_type,
                        nullable,
                    )]));
                    let actual = read_selected_array(
                        Bytes::from(reconstructed.clone()),
                        output_schema.clone(),
                        &selected,
                        rows,
                    )?;
                    let actual = cast(actual.as_ref(), &data_type)?;
                    assert_eq!(
                        actual.to_data(),
                        expected.to_data(),
                        "nullable={nullable}, utf8={utf8}"
                    );
                    if let Some(offset) = bad_utf8_byte {
                        let mut corrupt = reconstructed.clone();
                        corrupt[offset] = 255;
                        let error =
                            read_selected_array(corrupt.into(), output_schema, &selected, rows)
                                .err()
                                .ok_or("invalid selected UTF-8 accepted")?;
                        assert!(error.to_string().to_ascii_lowercase().contains("utf"));
                    }
                }
            }
        }
        Ok(())
    }

    fn delta_page(body: &[u8], rows: usize, nullable: bool) -> TestResult<(Page, Bytes)> {
        let bytes = super::super::tests::encode_page_with_encoding(
            body,
            body.len(),
            rows,
            None,
            Encoding::DELTA_LENGTH_BYTE_ARRAY,
        )?;
        Ok((
            Page {
                range: 100..100 + bytes.len() as u64,
                first_file_row: 50,
                row_count: rows,
                max_value_count: rows,
                compression: Compression::UNCOMPRESSED,
                value_width: None,
                nullable,
            },
            bytes,
        ))
    }

    #[test]
    fn delta_length_nulls_and_page_boundaries_are_checked() -> TestResult {
        let mut lengths = DeltaBitPackEncoder::<Int32Type>::new();
        lengths.put(&[2, 0, 3])?;
        let stream = lengths.flush_buffer()?;
        let mut body = vec![2, 0, 0, 0, 3, 0b10101];
        body.extend_from_slice(&stream);
        body.extend_from_slice(b"okabc");
        for (body, rows, valid) in [
            (body.clone(), 5, true),
            (vec![2, 0, 0, 0, 10, 0], 5, true), // All null, no value stream.
            (body.clone(), 4, false),           // Null levels and length count disagree.
            (body[..body.len() - 1].to_vec(), 5, false),
            (vec![255; 4], 5, false),
        ] {
            let (page, bytes) = delta_page(&body, rows, true)?;
            let cache = RangeCache {
                entries: vec![(page.range.clone(), bytes.clone())],
            };
            let result = selected_value_ranges(&cache, &page, &[49, 50, 51, 52, 54, 55]);
            if valid {
                let ranges = result.map_err(|e| format!("unexpected probe: {e:?}"))?;
                let payload: Vec<u8> = ranges
                    .iter()
                    .flat_map(|r| {
                        bytes[(r.start - 100) as usize..(r.end - 100) as usize]
                            .iter()
                            .copied()
                    })
                    .collect();
                assert_eq!(
                    payload,
                    if body.len() == 6 {
                        &b""[..]
                    } else {
                        &b"okabc"[..]
                    }
                );
                assert!(matches!(
                    selected_value_ranges(
                        &cache,
                        &Page {
                            first_file_row: u64::MAX,
                            ..page
                        },
                        &[50]
                    ),
                    Err(Invalid(_))
                ));
            } else {
                assert!(matches!(result, Err(Invalid(_))), "{result:?}");
            }
        }
        let (page, bytes) = delta_page(&body, 5, true)?;
        let cache = RangeCache {
            entries: vec![(page.range.clone(), bytes)],
        };
        for unsupported in [
            Page {
                range: page.range.clone(),
                value_width: Some(8),
                ..page
            },
            Page {
                range: page.range.clone(),
                compression: Compression::ZSTD(Default::default()),
                ..page
            },
        ] {
            assert!(matches!(
                selected_value_ranges(&cache, &unsupported, &[50]),
                Err(Unsupported)
            ));
        }
        assert!(matches!(
            selected_value_ranges(&cache, &page, &vec![50; 8193]),
            Err(Unsupported)
        ));
        let mut oversized = vec![0; MAX_PREFIX_BYTES + 4];
        oversized[..4].copy_from_slice(&(MAX_PREFIX_BYTES as u32).to_le_bytes());
        let (page, bytes) = delta_page(&oversized, 5, true)?;
        let prefix = page.prefix_range();
        let cache = RangeCache {
            entries: vec![(
                prefix.clone(),
                bytes.slice(..(prefix.end - prefix.start) as usize),
            )],
        };
        assert!(matches!(
            selected_value_ranges(&cache, &page, &[50]),
            Err(Unsupported)
        ));
        Ok(())
    }

    #[test]
    fn cached_prefix_stops_at_gaps_and_combines_overlapping_reads() {
        let mut cache = RangeCache {
            entries: vec![
                (14..18, Bytes::from_static(b"efgh")),
                (10..12, Bytes::from_static(b"ab")),
            ],
        };
        assert_eq!(cache.contiguous_prefix(10..18).as_ref(), b"ab");
        cache.entries.push((11..15, Bytes::from_static(b"bcde")));
        assert_eq!(cache.contiguous_prefix(10..17).as_ref(), b"abcdefg");
        assert!(cache.contiguous_prefix(0..10).is_empty());
    }
}
