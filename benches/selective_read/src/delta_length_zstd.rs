//! Offline layout inspection after the ordinary decoder has validated each page.
//! This records block types, not decoded offsets inside compressed blocks.

#[allow(dead_code)]
#[path = "../../../src/reader/backend/direct_parquet/compact_thrift.rs"]
mod compact_thrift;

use serde_json::{Value, json};
use thrift::protocol::TType;

use super::Result;

pub fn inspect(page: &[u8], decoded_size: usize) -> Result<Value> {
    let mut cursor = page;
    let body_size = compact_thrift::read_field(&mut cursor, 3, TType::I32, |cursor| {
        usize::try_from(compact_thrift::read_i64(cursor)?)
            .map_err(|_| compact_thrift::invalid_metadata())
    })?
    .ok_or("missing compressed page size")?;
    if cursor.len() != body_size {
        return Err("page header/index size mismatch".into());
    }
    let mut layout = blocks(cursor, decoded_size)?;
    layout["page_header_bytes"] = (page.len() - cursor.len()).into();
    Ok(layout)
}

// The ordinary codec checks compressed contents and checksums. This bounded
// inspector only accepts one standard frame, as emitted by these fixtures.
fn blocks(frame: &[u8], decoded_size: usize) -> Result<Value> {
    if frame.len() > 8 * 1024 * 1024 + 1024 || decoded_size > 8 * 1024 * 1024 {
        return Err("Zstd inspection page bound".into());
    }
    let mut cursor = frame;
    if take(&mut cursor, 4)? != [0x28, 0xb5, 0x2f, 0xfd] {
        return Err("expected one standard Zstd frame".into());
    }
    let descriptor = take(&mut cursor, 1)?[0];
    if descriptor & 8 != 0 {
        return Err("reserved Zstd frame flag".into());
    }
    let single = descriptor & 32 != 0;
    take(&mut cursor, usize::from(!single))?;
    let dictionary_bytes = [0, 1, 2, 4][(descriptor & 3) as usize];
    take(&mut cursor, dictionary_bytes)?;
    let size_bytes = [usize::from(single), 2, 4, 8][(descriptor >> 6) as usize];
    let mut size = [0; 8];
    size[..size_bytes].copy_from_slice(take(&mut cursor, size_bytes)?);
    let declared_size = u64::from_le_bytes(size)
        .checked_add(if size_bytes == 2 { 256 } else { 0 })
        .ok_or("Zstd frame size overflow")?;
    if size_bytes != 0 && declared_size != decoded_size as u64 {
        return Err("Zstd decoded size mismatch".into());
    }
    let mut records = Vec::new();
    let mut counts = [0; 3];
    let mut raw_bytes = 0;
    let mut known_decoded = 0;
    loop {
        if records.len() == 8192 {
            return Err("Zstd inspection block bound".into());
        }
        let offset = frame.len() - cursor.len();
        let header = take(&mut cursor, 3)?;
        let header = u32::from_le_bytes([header[0], header[1], header[2], 0]);
        let kind = ((header >> 1) & 3) as usize;
        let size = (header >> 3) as usize;
        if kind == 3 || size > 131072 {
            return Err("Zstd block type or size".into());
        }
        let stored = if kind == 1 { 1 } else { size };
        take(&mut cursor, stored)?;
        counts[kind] += 1;
        raw_bytes += if kind == 0 { size } else { 0 };
        known_decoded += if kind != 2 { size } else { 0 };
        records.push(
            json!({"header_offset":offset,"kind":(["raw","rle","compressed"][kind]),
            "stored_bytes":stored,"decoded_bytes":(kind != 2).then_some(size)}),
        );
        if header & 1 != 0 {
            break;
        }
    }
    let checksum = descriptor & 4 != 0;
    take(&mut cursor, if checksum { 4 } else { 0 })?;
    if !cursor.is_empty() || (counts[2] == 0 && known_decoded != decoded_size) {
        return Err("Zstd frame boundary mismatch".into());
    }
    Ok(
        json!({"raw_blocks":counts[0],"rle_blocks":counts[1],"compressed_blocks":counts[2],
        "raw_payload_bytes":raw_bytes,"frame_bytes":frame.len(),"checksum":checksum,
        "dictionary_id_bytes":dictionary_bytes,
        "all_raw_without_checksum_or_dictionary":counts[1] == 0 && counts[2] == 0 && !checksum && dictionary_bytes == 0,
        "blocks":records}),
    )
}

fn take<'a>(cursor: &mut &'a [u8], size: usize) -> Result<&'a [u8]> {
    let value = cursor.get(..size).ok_or("truncated Zstd frame")?;
    *cursor = &cursor[size..];
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_and_rle_counts_follow_stored_lengths() {
        // One 3-byte raw block and one byte repeated 4 times.
        let frame = [
            0x28, 0xb5, 0x2f, 0xfd, 32, 7, 24, 0, 0, 1, 2, 3, 35, 0, 0, 9,
        ];
        let result = blocks(&frame, 7).unwrap();
        assert_eq!(result["raw_blocks"], 1);
        assert_eq!(result["rle_blocks"], 1);
        assert_eq!(result["raw_payload_bytes"], 3);
        assert_eq!(result["blocks"][1]["stored_bytes"], 1);
        for end in 0..frame.len() {
            assert!(blocks(&frame[..end], 7).is_err());
        }
        assert!(blocks(&frame, 8).is_err());
    }

    #[test]
    fn compressed_block_size_is_not_a_decoded_offset() {
        // Only the block envelope is inspected; the ordinary codec validates contents.
        let frame = [0x28, 0xb5, 0x2f, 0xfd, 32, 10, 13, 0, 0, 0];
        let result = blocks(&frame, 10).unwrap();
        assert_eq!(result["compressed_blocks"], 1);
        assert!(result["blocks"][0]["decoded_bytes"].is_null());
        assert_eq!(result["all_raw_without_checksum_or_dictionary"], false);
    }

    #[test]
    fn raw_eligibility_checks_frame_flags_and_boundaries() {
        let mut frame = vec![0x28, 0xb5, 0x2f, 0xfd, 32, 1, 9, 0, 0, 42];
        assert_eq!(
            blocks(&frame, 1).unwrap()["all_raw_without_checksum_or_dictionary"],
            true
        );
        frame[4] |= 4;
        frame.extend_from_slice(&[0; 4]);
        assert_eq!(
            blocks(&frame, 1).unwrap()["all_raw_without_checksum_or_dictionary"],
            false
        );
        frame.push(0);
        assert!(blocks(&frame, 1).is_err());
        frame.truncate(10);
        frame[4] = 32;
        frame[6] |= 6;
        assert!(blocks(&frame, 1).is_err());
        assert!(blocks(&frame, 8 * 1024 * 1024 + 1).is_err());
    }

    #[test]
    fn inspection_records_many_small_blocks() {
        let mut frame = vec![0x28, 0xb5, 0x2f, 0xfd, 32, 129];
        for block in 0..129 {
            frame.extend_from_slice(&[8 | u8::from(block == 128), 0, 0, 42]);
        }
        let result = blocks(&frame, 129).unwrap();
        assert_eq!(result["raw_blocks"], 129);
        assert_eq!(result["blocks"].as_array().unwrap().len(), 129);
    }
}
