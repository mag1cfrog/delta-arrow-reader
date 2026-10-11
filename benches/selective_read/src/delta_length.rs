//! Bounded discovery of flat, uncompressed DELTA_LENGTH_BYTE_ARRAY V1 bodies.
//! This experiment receives an already checked page boundary and row selection.

use std::ops::Range;

use parquet::data_type::Int32Type;
use parquet::encodings::decoding::{Decoder, DeltaBitPackDecoder};

const MAX_PAGE_BYTES: usize = 8 * 1024 * 1024;
const MAX_ROWS: usize = 1024 * 1024;
const MAX_PREFIX_BYTES: usize = 1024 * 1024;
const MAX_BLOCK_VALUES: usize = 32768;
const MAX_RANGES: usize = 8192;
const MAX_PROBES: usize = 9;
const INITIAL_PREFIX: usize = 4096;

#[derive(Debug, PartialEq)]
pub enum Error {
    Invalid(&'static str),
    Fallback(&'static str),
    NeedBytes(usize),
}
type Result<T> = std::result::Result<T, Error>;
use Error::{Fallback, Invalid, NeedBytes};

pub struct Plan {
    pub prefix: Vec<u8>,
    pub ranges: Vec<Range<usize>>,
    pub payload_start: usize,
    pub selected_bytes: usize,
    pub probes: usize,
}

/// Fetch each prefix extension once. Unsupported layouts and exhausted budgets mean ordinary-read fallback; malformed
/// supported input is an error. No payload bytes may replace missing lengths/nulls.
pub fn discover(
    page_bytes: usize,
    rows: usize,
    nullable: bool,
    selected: &[usize],
    mut fetch: impl FnMut(Range<usize>) -> Result<Vec<u8>>,
) -> Result<Plan> {
    if page_bytes > MAX_PAGE_BYTES || rows > MAX_ROWS {
        return Err(Fallback("page size or row count"));
    }
    if selected.last().is_some_and(|row| *row >= rows)
        || selected.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(Invalid("row selection"));
    }
    let mut prefix = Vec::new();
    for probes in 0..=MAX_PROBES {
        match value_ranges(&prefix, page_bytes, rows, nullable, selected) {
            Ok((payload_start, mut ranges)) => {
                let selected_bytes = ranges.iter().map(|r| r.len()).sum();
                for range in &mut ranges {
                    range.start = range.start.max(prefix.len()).min(range.end);
                }
                ranges.retain(|range| !range.is_empty());
                return Ok(Plan {
                    prefix,
                    ranges,
                    payload_start,
                    selected_bytes,
                    probes,
                });
            }
            Err(NeedBytes(end)) => {
                if end > page_bytes || end <= prefix.len() {
                    return Err(Invalid("truncated page or non-progressing probe"));
                }
                if end > MAX_PREFIX_BYTES || probes == MAX_PROBES {
                    return Err(Fallback("length discovery budget"));
                }
                let end = end
                    .max(INITIAL_PREFIX)
                    .max(prefix.len() * 2)
                    .min(page_bytes)
                    .min(MAX_PREFIX_BYTES);
                let bytes = fetch(prefix.len()..end)?;
                if bytes.len() != end - prefix.len() {
                    return Err(Invalid("short prefix read"));
                }
                prefix.extend_from_slice(&bytes);
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("the final probe returns a plan or an error")
}

fn take<'a>(bytes: &'a [u8], at: &mut usize, size: usize) -> Result<&'a [u8]> {
    let end = at.checked_add(size).ok_or(Invalid("offset overflow"))?;
    let value = bytes.get(*at..end).ok_or(NeedBytes(end))?;
    *at = end;
    Ok(value)
}

fn varint(bytes: &[u8], at: &mut usize) -> Result<u64> {
    let mut value = 0;
    for shift in (0..70).step_by(7) {
        let byte = take(bytes, at, 1)?[0];
        if shift == 63 && byte > 1 {
            return Err(Invalid("varint overflow"));
        }
        value |= u64::from(byte & 127) << shift;
        if byte & 128 == 0 {
            return Ok(value);
        }
    }
    Err(Invalid("varint overflow"))
}

fn signed(value: u64) -> i64 {
    (value >> 1) as i64 ^ -((value & 1) as i64)
}

fn definitions(bytes: &[u8], rows: usize) -> Result<Vec<bool>> {
    let mut valid = Vec::with_capacity(rows);
    let mut at = 0;
    while valid.len() < rows {
        let header = varint(bytes, &mut at)?;
        let count = usize::try_from(header >> 1).map_err(|_| Invalid("definition run"))?;
        let remaining = rows - valid.len();
        if count == 0 {
            return Err(Invalid("empty definition run"));
        }
        if header & 1 == 0 {
            let value = take(bytes, &mut at, 1)?[0];
            if count > remaining || value > 1 {
                return Err(Invalid("definition run"));
            }
            valid.resize(valid.len() + count, value == 1);
        } else {
            if count > remaining.div_ceil(8) {
                return Err(Invalid("packed definition count"));
            }
            let packed = take(bytes, &mut at, count)?;
            valid
                .extend((0..remaining.min(count * 8)).map(|i| packed[i / 8] & (1 << (i % 8)) != 0));
        }
    }
    if at != bytes.len() {
        return Err(if bytes[at..].iter().all(|byte| *byte == 0) {
            Fallback("definition padding")
        } else {
            Invalid("trailing definition bytes")
        });
    }
    Ok(valid)
}

// The dependency trusts its encoded count and block geometry. Check both, and
// locate the complete length stream, before allocating or calling its decoder.
fn length_stream_end(bytes: &[u8], count: usize) -> Result<usize> {
    let mut at = 0;
    let block = varint(bytes, &mut at)?;
    let miniblocks = varint(bytes, &mut at)?;
    let encoded_count = varint(bytes, &mut at)?;
    let first = signed(varint(bytes, &mut at)?);
    if block == 0
        || miniblocks == 0
        || block % 128 != 0
        || block % miniblocks != 0
        || (block / miniblocks) % 32 != 0
    {
        return Err(Invalid("delta block geometry"));
    }
    if block > MAX_BLOCK_VALUES as u64 {
        return Err(Fallback("delta block size"));
    }
    if encoded_count != count as u64 || i32::try_from(first).is_err() || (count != 0 && first < 0) {
        return Err(Invalid("length count or first length"));
    }
    let values_per_miniblock = (block / miniblocks) as usize;
    let mut remaining = count.saturating_sub(1);
    while remaining != 0 {
        i32::try_from(signed(varint(bytes, &mut at)?)).map_err(|_| Invalid("minimum delta"))?;
        let widths = take(bytes, &mut at, miniblocks as usize)?;
        for &width in widths {
            if remaining == 0 {
                break; // The spec permits arbitrary widths in unused miniblocks.
            }
            if width > 32 {
                return Err(Invalid("delta bit width"));
            }
            take(
                bytes,
                &mut at,
                values_per_miniblock * usize::from(width) / 8,
            )?;
            remaining = remaining.saturating_sub(values_per_miniblock);
        }
    }
    Ok(at)
}

fn value_ranges(
    prefix: &[u8],
    page_bytes: usize,
    rows: usize,
    nullable: bool,
    selected: &[usize],
) -> Result<(usize, Vec<Range<usize>>)> {
    let mut at = 0;
    let valid = if nullable {
        let size = u32::from_le_bytes(take(prefix, &mut at, 4)?.try_into().unwrap()) as usize;
        let levels = take(prefix, &mut at, size)?;
        definitions(levels, rows).map_err(|error| match error {
            NeedBytes(_) => Invalid("truncated definition levels"),
            error => error,
        })?
    } else {
        vec![true; rows]
    };
    let count = valid.iter().filter(|value| **value).count();
    // Some writers omit the value stream on an all-null page.
    if count == 0 && at == page_bytes {
        return Ok((at, Vec::new()));
    }
    let end = length_stream_end(&prefix[at..], count).map_err(|error| match error {
        NeedBytes(end) => NeedBytes(at + end),
        error => error,
    })?;
    let mut decoder = DeltaBitPackDecoder::<Int32Type>::new();
    decoder
        .set_data(prefix[at..at + end].to_vec().into(), count)
        .map_err(|_| Invalid("length decoder header"))?;
    let mut lengths = vec![0; count];
    if count != 0
        && decoder
            .get(&mut lengths)
            .map_err(|_| Invalid("length decoder"))?
            != count
    {
        return Err(Invalid("decoded length count"));
    }
    if decoder.get_offset() != end {
        return Err(Invalid("length stream boundary"));
    }
    let payload_start = at + end;
    let mut offset = payload_start;
    let mut lengths = lengths.into_iter();
    let mut selected = selected.iter().peekable();
    let mut ranges: Vec<Range<usize>> = Vec::new();
    for (row, valid) in valid.into_iter().enumerate() {
        let length = if valid {
            usize::try_from(lengths.next().ok_or(Invalid("missing length"))?)
                .map_err(|_| Invalid("negative length"))?
        } else {
            0
        };
        let end = offset
            .checked_add(length)
            .filter(|end| *end <= page_bytes)
            .ok_or(Invalid("payload length"))?;
        if selected.peek() == Some(&&row) {
            selected.next();
            if offset < end {
                if let Some(last) = ranges.last_mut().filter(|range| range.end == offset) {
                    last.end = end;
                } else {
                    if ranges.len() == MAX_RANGES {
                        return Err(Fallback("selected ranges"));
                    }
                    ranges.push(offset..end);
                }
            }
        }
        offset = end;
    }
    if offset != page_bytes {
        return Err(Invalid("payload length sum"));
    }
    Ok((payload_start, ranges))
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::encodings::encoding::{DeltaBitPackEncoder, Encoder};

    fn body(lengths: &[i32]) -> Vec<u8> {
        let mut encoder = DeltaBitPackEncoder::<Int32Type>::new();
        encoder.put(lengths).unwrap();
        let mut bytes = encoder.flush_buffer().unwrap().to_vec();
        for (i, &length) in lengths.iter().enumerate() {
            bytes.extend(std::iter::repeat_n((i % 251) as u8, length.max(0) as usize));
        }
        bytes
    }

    fn plan(bytes: &[u8], rows: usize, nullable: bool, selected: &[usize]) -> Result<Plan> {
        discover(bytes.len(), rows, nullable, selected, |range| {
            Ok(bytes[range].to_vec())
        })
    }

    #[test]
    fn discovery_crosses_prefixes_and_preserves_exact_selected_bytes() {
        let lengths: Vec<_> = (0usize..8193).map(|i| (i * 7919 % 1024) as i32).collect();
        let bytes = body(&lengths);
        let selected = [0, 31, 32, 127, 128, 4096, 8192];
        let plan = plan(&bytes, lengths.len(), false, &selected).unwrap();
        assert!(plan.probes > 1);
        assert!(plan.prefix.len() >= plan.payload_start);
        let mut partial = vec![0; bytes.len()];
        partial[..plan.prefix.len()].copy_from_slice(&plan.prefix);
        for range in &plan.ranges {
            partial[range.clone()].copy_from_slice(&bytes[range.clone()]);
        }
        let mut at = plan.payload_start;
        for (row, length) in lengths.iter().enumerate() {
            let end = at + *length as usize;
            if selected.contains(&row) {
                assert_eq!(partial[at..end], bytes[at..end]);
            }
            at = end;
        }
        assert!(
            plan.prefix.len() + plan.ranges.iter().map(|r| r.len()).sum::<usize>()
                < bytes.len() / 10
        );
    }

    #[test]
    fn empty_and_all_null_streams_do_not_decode_a_first_value() {
        assert!(plan(&[], 0, false, &[]).unwrap().ranges.is_empty());
        assert!(plan(&body(&[]), 0, false, &[]).unwrap().ranges.is_empty());
        let mut empty = body(&[]);
        *empty.last_mut().unwrap() = 1; // The first-value placeholder is unused when count is zero.
        assert!(plan(&empty, 0, false, &[]).is_ok());
        let mut bytes = vec![2, 0, 0, 0, 8, 0]; // Four nulls, V1 RLE definitions.
        assert!(plan(&bytes, 4, true, &[0, 3]).unwrap().ranges.is_empty());
        bytes.extend(body(&[]));
        assert!(plan(&bytes, 4, true, &[1]).unwrap().ranges.is_empty());
        bytes[5] = 2;
        assert_eq!(
            plan(&bytes, 4, true, &[]).err(),
            Some(Invalid("definition run"))
        );
    }

    #[test]
    fn malformed_lengths_and_definitions_are_rejected() {
        assert_eq!(
            plan(&body(&[-1]), 1, false, &[]).err(),
            Some(Invalid("length count or first length"))
        );
        assert_eq!(
            plan(&body(&[1, -1]), 2, false, &[0]).err(),
            Some(Invalid("negative length"))
        );
        let huge_count = [128, 1, 4, 255, 255, 255, 255, 15, 0];
        assert_eq!(
            plan(&huge_count, 2, false, &[0]).err(),
            Some(Invalid("length count or first length"))
        );
        let bytes = body(&[5, 0, 3]);
        for end in 0..bytes.len() {
            assert!(
                plan(&bytes[..end], 3, false, &[2]).is_err(),
                "accepted truncation at {end}"
            );
        }
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert_eq!(
            plan(&trailing, 3, false, &[]).err(),
            Some(Invalid("payload length sum"))
        );
        assert!(plan(&[1, 0, 0, 0, 128], 1, true, &[0]).is_err());
        assert!(plan(&bytes, 3, false, &[1, 1]).is_err());
        assert!(plan(&bytes, 3, false, &[3]).is_err());
    }

    #[test]
    fn used_bit_widths_are_checked_and_unused_widths_are_ignored() {
        let mut bytes = body(&[1, 2]);
        // Header, minimum delta, then four miniblock widths; only the first is used.
        bytes[7..10].fill(255);
        assert!(plan(&bytes, 2, false, &[1]).is_ok());
        bytes[6] = 33;
        assert_eq!(
            plan(&bytes, 2, false, &[]).err(),
            Some(Invalid("delta bit width"))
        );
    }

    #[test]
    fn parser_and_fetch_budgets_are_bounded() {
        assert_eq!(
            length_stream_end(&[0, 4, 0, 0], 0),
            Err(Invalid("delta block geometry"))
        );
        assert_eq!(
            length_stream_end(&[128, 1, 0, 0, 0], 0),
            Err(Invalid("delta block geometry"))
        );
        assert!(length_stream_end(&[255; 16], 1).is_err());
        assert_eq!(
            discover(10, 1, false, &[0], |_| Ok(vec![])).err(),
            Some(Invalid("short prefix read"))
        );
        assert_eq!(
            discover(MAX_PAGE_BYTES + 1, 1, false, &[], |_| panic!()).err(),
            Some(Fallback("page size or row count"))
        );
        assert_eq!(
            discover(1, MAX_ROWS + 1, false, &[], |_| panic!()).err(),
            Some(Fallback("page size or row count"))
        );
        let bytes = [1, 0, 16, 0]; // Definition stream exceeds the prefix budget.
        let mut requests = 0;
        let result = discover(MAX_PAGE_BYTES, 1, true, &[], |range| {
            requests += 1;
            let mut prefix = vec![0; range.len()];
            prefix[..4].copy_from_slice(&bytes);
            Ok(prefix)
        });
        assert_eq!(result.err(), Some(Fallback("length discovery budget")));
        assert_eq!(requests, 1);
    }
}
