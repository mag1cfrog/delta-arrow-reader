//! Bounded decoding of the lengths preceding a DELTA_LENGTH_BYTE_ARRAY payload.

use bytes::Bytes;
use parquet::{
    data_type::Int32Type,
    encodings::decoding::{Decoder, DeltaBitPackDecoder},
};

use super::page::{MAX_PAGE_BYTES, MAX_PAGE_ROWS};

pub(super) const MAX_PREFIX_BYTES: usize = 1024 * 1024;
const MAX_BLOCK_VALUES: u64 = 32_768;

#[derive(Debug, PartialEq, Eq)]
pub(super) enum LengthError {
    NeedBytes(usize),
    Unsupported,
    Invalid(&'static str),
}
use LengthError::{Invalid, NeedBytes, Unsupported};

/// Returns the payload offset and nonnegative lengths in physical value order.
///
/// `prefix` starts after the page header and null levels. `value_count` must be
/// the non-null count checked by the caller; `total_bytes` includes the complete
/// length stream and payload, even when only a prefix has been downloaded.
/// A `NeedBytes` offset is relative to this prefix, not an absolute file offset.
pub(super) fn decode_lengths(
    prefix: Bytes,
    value_count: usize,
    total_bytes: usize,
) -> Result<(usize, Vec<i32>), LengthError> {
    if value_count > MAX_PAGE_ROWS || total_bytes > MAX_PAGE_BYTES {
        return Err(Unsupported);
    }
    if prefix.len() > total_bytes {
        return Err(Invalid("prefix exceeds value area"));
    }
    if prefix.len() > MAX_PREFIX_BYTES {
        return Err(Unsupported);
    }
    // Writers may omit the value stream entirely when every row is null.
    if value_count == 0 && total_bytes == 0 {
        return Ok((0, Vec::new()));
    }
    let end = length_stream_end(&prefix, value_count).map_err(|error| match error {
        NeedBytes(end) if end > total_bytes => Invalid("truncated length stream"),
        NeedBytes(end) if end > MAX_PREFIX_BYTES => Unsupported,
        error => error,
    })?;

    // The dependency reads its own count. Validate it and the block geometry
    // before allocating, then give it only the checked structural bytes.
    let mut decoder = DeltaBitPackDecoder::<Int32Type>::new();
    decoder
        .set_data(prefix.slice(..end), value_count)
        .map_err(|_| Invalid("length decoder header"))?;
    let mut lengths = vec![0; value_count];
    if value_count != 0
        && decoder
            .get(&mut lengths)
            .map_err(|_| Invalid("length decoder"))?
            != value_count
    {
        return Err(Invalid("decoded length count"));
    }
    if decoder.get_offset() != end {
        return Err(Invalid("length stream boundary"));
    }
    let mut payload_end = end;
    for &length in &lengths {
        let length = usize::try_from(length).map_err(|_| Invalid("negative length"))?;
        payload_end = payload_end
            .checked_add(length)
            .filter(|end| *end <= total_bytes)
            .ok_or(Invalid("payload length"))?;
    }
    if payload_end != total_bytes {
        return Err(Invalid("payload length sum"));
    }
    Ok((end, lengths))
}

fn take<'a>(bytes: &'a [u8], offset: &mut usize, count: usize) -> Result<&'a [u8], LengthError> {
    let end = offset
        .checked_add(count)
        .ok_or(Invalid("offset overflow"))?;
    let value = bytes.get(*offset..end).ok_or(NeedBytes(end))?;
    *offset = end;
    Ok(value)
}

fn read_varint(bytes: &[u8], offset: &mut usize) -> Result<u64, LengthError> {
    let mut value = 0;
    for shift in (0..70).step_by(7) {
        let byte = take(bytes, offset, 1)?[0];
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

fn decode_zigzag(value: u64) -> i64 {
    (value >> 1) as i64 ^ -((value & 1) as i64)
}

fn length_stream_end(bytes: &[u8], count: usize) -> Result<usize, LengthError> {
    let mut offset = 0;
    let block = read_varint(bytes, &mut offset)?;
    let miniblocks = read_varint(bytes, &mut offset)?;
    let encoded_count = read_varint(bytes, &mut offset)?;
    let first = decode_zigzag(read_varint(bytes, &mut offset)?);
    if block == 0
        || miniblocks == 0
        || block % 128 != 0
        || block % miniblocks != 0
        || (block / miniblocks) % 32 != 0
    {
        return Err(Invalid("delta block geometry"));
    }
    if block > MAX_BLOCK_VALUES {
        return Err(Unsupported);
    }
    if encoded_count != count as u64 || i32::try_from(first).is_err() || (count != 0 && first < 0) {
        return Err(Invalid("length count or first length"));
    }
    let values_per_miniblock = (block / miniblocks) as usize;
    let mut remaining = count.saturating_sub(1);
    while remaining != 0 {
        i32::try_from(decode_zigzag(read_varint(bytes, &mut offset)?))
            .map_err(|_| Invalid("minimum delta"))?;
        let widths = take(bytes, &mut offset, miniblocks as usize)?;
        for &width in widths {
            if remaining == 0 {
                break; // Unused miniblocks may carry arbitrary width bytes.
            }
            if width > 32 {
                return Err(Invalid("delta bit width"));
            }
            take(
                bytes,
                &mut offset,
                values_per_miniblock * usize::from(width) / 8,
            )?;
            remaining = remaining.saturating_sub(values_per_miniblock);
        }
    }
    Ok(offset)
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::encodings::encoding::{DeltaBitPackEncoder, Encoder};

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn encoded(lengths: &[i32]) -> parquet::errors::Result<Bytes> {
        let mut encoder = DeltaBitPackEncoder::<Int32Type>::new();
        encoder.put(lengths)?;
        encoder.flush_buffer()
    }

    #[test]
    fn lengths_locate_the_payload_without_downloading_it() -> TestResult {
        let expected: Vec<i32> = (0..8193).map(|i| i * 7919 % 1024).collect();
        let stream = encoded(&expected)?;
        assert!(stream.len() > 4096);
        let payload_bytes = expected.iter().map(|v| *v as usize).sum::<usize>();
        let total = stream.len() + payload_bytes;
        let mut available = 0;
        loop {
            match decode_lengths(stream.slice(..available), expected.len(), total) {
                Err(NeedBytes(end)) => {
                    assert!(end > available && end <= stream.len());
                    available = end;
                }
                Ok((offset, lengths)) => {
                    assert_eq!(offset, stream.len());
                    assert_eq!(lengths, expected);
                    break;
                }
                Err(error) => return Err(format!("unexpected length error: {error:?}").into()),
            }
        }
        Ok(())
    }

    #[test]
    fn empty_streams_and_unused_placeholders_are_valid() -> TestResult {
        assert_eq!(decode_lengths(Bytes::new(), 0, 0), Ok((0, vec![])));
        for expected in [vec![], vec![0], vec![17, 0, 3]] {
            let stream = encoded(&expected)?;
            let end = stream.len();
            let total = end + expected.iter().map(|v| *v as usize).sum::<usize>();
            assert_eq!(
                decode_lengths(stream, expected.len(), total),
                Ok((end, expected))
            );
        }
        let mut empty = encoded(&[])?.to_vec();
        let last = empty.last_mut().ok_or("missing first-value placeholder")?;
        *last = 1; // Negative placeholder is unused when the count is zero.
        let total = empty.len();
        assert_eq!(decode_lengths(empty.into(), 0, total), Ok((total, vec![])));
        Ok(())
    }

    #[test]
    fn malformed_lengths_cannot_change_payload_boundaries() -> TestResult {
        let stream = encoded(&[5, 0, 3])?;
        let total = stream.len() + 8;
        for truncated in 0..total {
            let prefix = stream.slice(..truncated.min(stream.len()));
            assert!(matches!(
                decode_lengths(prefix, 3, truncated),
                Err(Invalid(_))
            ));
        }
        assert!(matches!(
            decode_lengths(stream.clone(), 3, total + 1),
            Err(Invalid(_))
        ));
        assert!(matches!(decode_lengths(stream, 4, total), Err(Invalid(_))));
        for lengths in [vec![-1], vec![1, -1], vec![i32::MAX]] {
            let stream = encoded(&lengths)?;
            let total = stream.len() + 1;
            assert!(matches!(
                decode_lengths(stream, lengths.len(), total),
                Err(Invalid(_))
            ));
        }
        Ok(())
    }

    #[test]
    fn used_widths_are_validated_before_dependency_decoding() {
        // Block 128, four miniblocks, two lengths, first 1, minimum delta 1.
        // Width zero encodes that delta; the three unused widths are arbitrary.
        let mut stream = vec![128, 1, 4, 2, 2, 2, 0, 255, 255, 255];
        let total = stream.len() + 3;
        assert_eq!(
            decode_lengths(stream.clone().into(), 2, total),
            Ok((10, vec![1, 2]))
        );
        stream[6] = 33;
        assert_eq!(
            decode_lengths(stream.into(), 2, total),
            Err(Invalid("delta bit width"))
        );
    }

    #[test]
    fn discovering_an_oversized_length_stream_stops_at_the_prefix_limit() {
        // One million zero lengths, deliberately encoded at width 32. The
        // complete page fits the page limit but its length stream exceeds 1 MiB.
        let mut prefix = vec![128, 1, 4, 128, 128, 64, 0];
        let mut block = vec![0, 32, 32, 32, 32];
        block.resize(5 + 128 * 4, 0);
        let total = prefix.len() + (MAX_PAGE_ROWS - 1).div_ceil(128) * block.len();
        while prefix.len() < MAX_PREFIX_BYTES {
            prefix.extend_from_slice(&block[..block.len().min(MAX_PREFIX_BYTES - prefix.len())]);
        }
        assert_eq!(
            decode_lengths(prefix.into(), MAX_PAGE_ROWS, total),
            Err(Unsupported)
        );
    }

    #[test]
    fn untrusted_counts_geometry_and_probe_sizes_are_bounded() {
        for header in [
            vec![0, 4, 0, 0],
            vec![128, 1, 0, 0, 0],
            vec![128, 1, 4, 255, 255, 255, 255, 15, 0],
            vec![255; 16],
        ] {
            let total = header.len();
            assert!(matches!(
                decode_lengths(header.into(), 1, total),
                Err(Invalid(_))
            ));
        }
        assert_eq!(
            decode_lengths(Bytes::new(), MAX_PAGE_ROWS + 1, 1),
            Err(Unsupported)
        );
        assert_eq!(
            decode_lengths(Bytes::new(), 1, MAX_PAGE_BYTES + 1),
            Err(Unsupported)
        );
        assert_eq!(
            decode_lengths(vec![0; MAX_PREFIX_BYTES + 1].into(), 1, MAX_PAGE_BYTES),
            Err(Unsupported)
        );
        assert!(matches!(
            decode_lengths(Bytes::from_static(&[0]), 0, 0),
            Err(Invalid(_))
        ));
        // A valid block shape outside the supported bound must not reach the decoder.
        assert_eq!(
            decode_lengths(Bytes::from_static(&[128, 128, 4, 4, 1, 0]), 1, 6),
            Err(Unsupported)
        );
    }
}
