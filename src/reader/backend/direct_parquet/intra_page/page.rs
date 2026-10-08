//! Inspect only bounded headers, null levels and addressable fixed-width values.

use std::ops::Range;

use bytes::Bytes;
use parquet::basic::Compression;
use thrift::protocol::TType;

use super::super::compact_thrift::{invalid_metadata, read_field, read_i64};

pub(super) const MAX_PAGE_BYTES: usize = 8 * 1024 * 1024;
pub(super) const MAX_PAGE_ROWS: usize = 1024 * 1024;

pub(super) struct Page {
    pub(super) range: Range<u64>,
    pub(super) first: u64,
    pub(super) rows: usize,
    pub(super) codec: Compression,
}

#[derive(Debug)]
pub(super) enum ProbeError {
    Need(Range<u64>),
    Unsupported,
    Invalid(&'static str),
}
use ProbeError::{Invalid, Need, Unsupported};

#[derive(Default)]
pub(super) struct Cache(pub(super) Vec<(Range<u64>, Bytes)>);

impl Cache {
    pub(super) fn bytes(&self) -> usize {
        self.0.iter().map(|(_, b)| b.len()).sum()
    }

    pub(super) fn get(&self, range: &Range<u64>) -> Option<Bytes> {
        self.0.iter().find_map(|(r, b)| {
            (r.start <= range.start && range.end <= r.end)
                .then(|| b.slice((range.start - r.start) as usize..(range.end - r.start) as usize))
        })
    }

    fn read(&self, range: Range<u64>) -> Result<Bytes, ProbeError> {
        self.get(&range).ok_or(Need(range))
    }

    pub(super) fn reconstruct(&self, range: &Range<u64>) -> Bytes {
        if let Some(bytes) = self.get(range) {
            return bytes;
        }
        let mut out = vec![0; (range.end - range.start) as usize];
        for (r, bytes) in &self.0 {
            let lo = r.start.max(range.start);
            let hi = r.end.min(range.end);
            if lo < hi {
                out[(lo - range.start) as usize..(hi - range.start) as usize]
                    .copy_from_slice(&bytes[(lo - r.start) as usize..(hi - r.start) as usize]);
            }
        }
        out.into()
    }
}

fn i32_field(bytes: &[u8], id: i16) -> thrift::Result<Option<i32>> {
    read_field(&mut &*bytes, id, TType::I32, |p| {
        i32::try_from(read_i64(p)?).map_err(|_| invalid_metadata())
    })
}

// Reuse the allocation-free, depth-limited Compact Thrift parser used for footer
// probes. A header larger than the prefix is left to the normal Parquet reader.
fn header(bytes: &[u8]) -> Result<(usize, usize, usize, usize), ProbeError> {
    let read = || -> thrift::Result<_> {
        let page_type = i32_field(bytes, 1)?;
        let compressed = i32_field(bytes, 3)?;
        let uncompressed = i32_field(bytes, 2)?;
        let checksum = i32_field(bytes, 4)?.is_some();
        let mut cursor = bytes;
        let data = read_field(&mut cursor, 5, TType::Struct, |p| {
            let start = *p;
            let count = read_field(p, 1, TType::I32, read_i64)?;
            Ok((
                count,
                i32_field(start, 2)?,
                i32_field(start, 3)?,
                i32_field(start, 4)?,
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
    Ok((length, compressed, uncompressed, count))
}

// Logical uncompressed range and the raw bytes' physical offset.
type Block = (Range<usize>, u64);

fn physical(blocks: &[Block], range: Range<usize>) -> Result<Vec<Range<u64>>, ProbeError> {
    let mut out = Vec::new();
    for (logical, offset) in blocks {
        let lo = logical.start.max(range.start);
        let hi = logical.end.min(range.end);
        if lo < hi {
            out.push(offset + (lo - logical.start) as u64..offset + (hi - logical.start) as u64);
        }
    }
    if out.iter().map(|r| r.end - r.start).sum::<u64>() != (range.end - range.start) as u64 {
        return Err(Invalid("logical range outside raw blocks"));
    }
    Ok(out)
}

fn read_logical(
    cache: &Cache,
    blocks: &[Block],
    range: Range<usize>,
) -> Result<Vec<u8>, ProbeError> {
    let mut out = Vec::with_capacity(range.len());
    for r in physical(blocks, range)? {
        out.extend_from_slice(&cache.read(r)?);
    }
    Ok(out)
}

pub(super) fn probe(
    cache: &Cache,
    page: &Page,
    selected: &[u64],
) -> Result<Vec<Range<u64>>, ProbeError> {
    let prefix = cache.read(page.range.start..(page.range.start + 4096).min(page.range.end))?;
    let (header_len, compressed, uncompressed, count) = header(&prefix)?;
    if count != page.rows || uncompressed < 4 {
        return Err(Invalid("page row count or size"));
    }
    let body = page.range.start + header_len as u64;
    if body.checked_add(compressed as u64) != Some(page.range.end) {
        return Err(Invalid("page boundary mismatch"));
    }
    let mut blocks = Vec::new();
    match page.codec {
        Compression::UNCOMPRESSED => {
            if compressed != uncompressed {
                return Err(Invalid("uncompressed page length"));
            }
            blocks.push((0..uncompressed, body));
        }
        Compression::ZSTD(_) => {
            let frame = cache.read(body..(body + 18).min(page.range.end))?;
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
            if descriptor & 8 != 0 {
                return Err(Invalid("reserved Zstd frame flags"));
            }
            if descriptor & 7 != 0 {
                return Err(Unsupported);
            } // Dictionary or frame checksum.
            let single = descriptor & 32 != 0;
            let size_len = [usize::from(single), 2, 4, 8][(descriptor >> 6) as usize];
            let size_start = 5 + usize::from(!single);
            let size_bytes = frame
                .get(size_start..size_start + size_len)
                .ok_or(Invalid("truncated Zstd frame"))?;
            if size_len > 0 {
                let mut encoded = [0; 8];
                encoded[..size_len].copy_from_slice(size_bytes);
                let size = u64::from_le_bytes(encoded)
                    .checked_add(if size_len == 2 { 256 } else { 0 })
                    .ok_or(Invalid("Zstd size overflow"))?;
                if size < uncompressed as u64 {
                    return Err(Unsupported); // The page may concatenate multiple frames.
                }
                if size > uncompressed as u64 {
                    return Err(Invalid("Zstd frame size mismatch"));
                }
            }
            let mut offset = body + (size_start + size_len) as u64;
            let mut decoded = 0;
            loop {
                if offset + 3 > page.range.end {
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
                if size > 131_072
                    || offset + 3 + size as u64 > page.range.end
                    || decoded + size > uncompressed
                {
                    return Err(Invalid("Zstd raw block size"));
                }
                blocks.push((decoded..decoded + size, offset + 3));
                decoded += size;
                offset += 3 + size as u64;
                if header & 1 != 0 {
                    break;
                }
                if blocks.len() > 128 {
                    return Err(Unsupported);
                }
            }
            if offset < page.range.end {
                return Err(Unsupported); // Another ordinary or skippable frame may follow.
            }
            if offset != page.range.end || decoded != uncompressed {
                return Err(Invalid("Zstd frame boundary mismatch"));
            }
        }
        _ => return Err(Unsupported),
    }
    let length = read_logical(cache, &blocks, 0..4)?;
    let levels_len = u32::from_le_bytes([length[0], length[1], length[2], length[3]]) as usize;
    if levels_len > uncompressed - 4 {
        return Err(Invalid("definition levels size"));
    }
    let levels = read_logical(cache, &blocks, 4..4 + levels_len)?;
    let valid = definition_levels(&levels, page.rows)?;
    let value_start = 4 + levels_len;
    let value_count = valid.iter().filter(|v| **v).count();
    if value_start + value_count * 8 != uncompressed {
        return Err(Invalid("PLAIN value count"));
    }
    let mut rank = 0;
    let ranks: Vec<_> = valid
        .iter()
        .map(|v| {
            let current = rank;
            rank += usize::from(*v);
            current
        })
        .collect();
    let mut ranges = Vec::new();
    for row in selected
        .iter()
        .copied()
        .filter(|r| page.first <= *r && *r < page.first + page.rows as u64)
    {
        let row = (row - page.first) as usize;
        if valid[row] {
            let offset = value_start + ranks[row] * 8;
            ranges.extend(physical(&blocks, offset..offset + 8)?);
        }
    }
    Ok(ranges)
}

// Nullable, non-repeated columns have exactly one definition bit per row.
// Decode only this bounded one-bit subset of Parquet's RLE/bit-packed format.
fn definition_levels(mut bytes: &[u8], count: usize) -> Result<Vec<bool>, ProbeError> {
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
