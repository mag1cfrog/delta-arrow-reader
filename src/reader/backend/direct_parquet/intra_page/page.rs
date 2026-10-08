//! Inspect only bounded headers, null levels and addressable fixed-width values.

use std::ops::Range;

use bytes::Bytes;
use parquet::basic::Compression;
use thrift::protocol::TType;

use super::super::compact_thrift::{invalid_metadata, read_field, read_i64};

pub(super) const MAX_PAGE_BYTES: usize = 8 * 1024 * 1024;
pub(super) const MAX_PAGE_ROWS: usize = 1024 * 1024;
pub(super) const PAGE_PREFIX_BYTES: u64 = 4096;
pub(super) const MAX_ZSTD_BLOCK_BYTES: usize = 131_072;

pub(super) struct Page {
    pub(super) range: Range<u64>,
    pub(super) first_file_row: u64,
    pub(super) row_count: usize,
    pub(super) compression: Compression,
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

    // Only call after fetching all structural bytes and selected values. Missing
    // bytes must belong to fixed-width values that the decoder will discard.
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

pub(super) fn selected_value_ranges(
    cache: &RangeCache,
    page: &Page,
    selected_rows: &[u64],
) -> Result<Vec<Range<u64>>, ProbeError> {
    let prefix =
        cache.read(page.range.start..(page.range.start + PAGE_PREFIX_BYTES).min(page.range.end))?;
    let PageHeader {
        header_len,
        compressed_size,
        uncompressed_size,
        row_count,
    } = parse_page_header(&prefix)?;
    if row_count != page.row_count || uncompressed_size < 4 {
        return Err(Invalid("page row count or size"));
    }
    let body = page.range.start + header_len as u64;
    if body.checked_add(compressed_size as u64) != Some(page.range.end) {
        return Err(Invalid("page boundary mismatch"));
    }
    let blocks = match page.compression {
        Compression::UNCOMPRESSED => {
            if compressed_size != uncompressed_size {
                return Err(Invalid("uncompressed page length"));
            }
            vec![RawBlock {
                decoded_range: 0..uncompressed_size,
                file_offset: body,
            }]
        }
        Compression::ZSTD(_) => parse_zstd_blocks(cache, body..page.range.end, uncompressed_size)?,
        _ => return Err(Unsupported),
    };
    let length = read_page_bytes(cache, &blocks, 0..4)?;
    let levels_len = u32::from_le_bytes([length[0], length[1], length[2], length[3]]) as usize;
    if levels_len > uncompressed_size - 4 {
        return Err(Invalid("definition levels size"));
    }
    let levels = read_page_bytes(cache, &blocks, 4..4 + levels_len)?;
    let valid = decode_definition_levels(&levels, page.row_count)?;
    let value_start = 4 + levels_len;
    let mut value_end = value_start;
    let value_offsets: Vec<_> = valid
        .iter()
        .map(|is_valid| {
            let offset = value_end;
            value_end += usize::from(*is_valid) * 8;
            offset
        })
        .collect();
    if value_end != uncompressed_size {
        return Err(Invalid("PLAIN value count"));
    }
    let mut ranges = Vec::new();
    for row in selected_rows
        .iter()
        .copied()
        .filter(|r| page.first_file_row <= *r && *r < page.first_file_row + page.row_count as u64)
    {
        let row = (row - page.first_file_row) as usize;
        if valid[row] {
            let offset = value_offsets[row];
            ranges.extend(map_to_file_ranges(&blocks, offset..offset + 8)?);
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
    // Parquet wire values: DATA_PAGE = 0, PLAIN = 0, RLE = 3.
    if checksum || kind != Some(0) {
        return Err(Unsupported);
    }
    let Some((count, Some(0), Some(3), Some(3))) = data else {
        return Err(Unsupported);
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
