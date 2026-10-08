//! Bounded Compact Thrift reads shared by optional Parquet metadata probes.

use thrift::{ProtocolErrorKind, new_protocol_error, protocol::TType};

pub(super) type Protocol<'a> = &'a [u8];

pub(super) fn invalid_metadata() -> thrift::Error {
    new_protocol_error(
        ProtocolErrorKind::InvalidData,
        "invalid Compact Thrift metadata",
    )
}

/// Reject over-wide encodings before decoding ZigZag. thrift 0.17 can truncate
/// an overflowing varint to zero, falsely establishing that a column has no NaNs.
fn read_varint(p: &mut Protocol<'_>) -> thrift::Result<u64> {
    let mut value = 0_u64;
    for shift in (0..70).step_by(7) {
        let byte = read_byte(p)?;
        if shift == 63 && byte > 1 {
            return Err(invalid_metadata());
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(invalid_metadata())
}

pub(super) fn read_i64(p: &mut Protocol<'_>) -> thrift::Result<i64> {
    let value = read_varint(p)?;
    Ok((value >> 1) as i64 ^ -((value & 1) as i64))
}

// A slice cursor makes bounds checks explicit and skips binary data without
// allocations. Only the compact types used by Parquet metadata are supported.
fn read_byte(p: &mut Protocol<'_>) -> thrift::Result<u8> {
    let (&byte, rest) = p.split_first().ok_or_else(invalid_metadata)?;
    *p = rest;
    Ok(byte)
}

fn skip_bytes(p: &mut Protocol<'_>, length: usize) -> thrift::Result<()> {
    *p = p.get(length..).ok_or_else(invalid_metadata)?;
    Ok(())
}

fn read_type(kind: u8) -> thrift::Result<TType> {
    Ok(match kind {
        0 => TType::Stop,
        1 | 2 => TType::Bool,
        3 => TType::I08,
        4 => TType::I16,
        5 => TType::I32,
        6 => TType::I64,
        7 => TType::Double,
        8 => TType::String,
        9 => TType::List,
        12 => TType::Struct,
        // parquet-rs 58 does not accept set/map fields either.
        _ => return Err(invalid_metadata()),
    })
}

pub(super) fn read_field_header(p: &mut Protocol<'_>, last_id: &mut i16) -> thrift::Result<TType> {
    let header = read_byte(p)?;
    if header == 0 {
        return Ok(TType::Stop);
    }
    let kind = read_type(header & 0x0f)?;
    if kind == TType::Stop {
        return Err(invalid_metadata());
    }
    let delta = i16::from(header >> 4);
    *last_id = if delta == 0 {
        i16::try_from(read_i64(p)?).map_err(|_| invalid_metadata())?
    } else {
        last_id.checked_add(delta).ok_or_else(invalid_metadata)?
    };
    Ok(kind)
}

/// Visit one optional field in a struct, checking its type and uniqueness.
pub(super) fn read_field<T>(
    p: &mut Protocol<'_>,
    id: i16,
    field_type: TType,
    mut read: impl FnMut(&mut Protocol<'_>) -> thrift::Result<T>,
) -> thrift::Result<Option<T>> {
    let mut last_id = 0;
    let mut value = None;
    loop {
        let kind = read_field_header(p, &mut last_id)?;
        if kind == TType::Stop {
            break;
        }
        if last_id == id {
            if kind != field_type || value.is_some() {
                return Err(invalid_metadata());
            }
            value = Some(read(p)?);
        } else if kind != TType::Bool {
            // Struct boolean values are already part of the field header.
            skip_value(p, kind, 64)?;
        }
    }
    Ok(value)
}

fn read_list(p: &mut Protocol<'_>) -> thrift::Result<(TType, usize)> {
    let header = read_byte(p)?;
    // Some Parquet writers use zero rather than a type for an empty list.
    if header == 0 {
        return Ok((TType::I08, 0));
    }
    let kind = read_type(header & 0x0f)?;
    if kind == TType::Stop {
        return Err(invalid_metadata());
    }
    let size = if header >> 4 == 15 {
        i32::try_from(read_varint(p)?).map_err(|_| invalid_metadata())? as usize
    } else {
        usize::from(header >> 4)
    };
    // Each element needs at least one byte, including bools and empty structs.
    if size > p.len() {
        return Err(invalid_metadata());
    }
    // parquet-rs 58 skips boolean collections without consuming their values.
    // They are not used by the footer schema. Reject this extension so the two
    // readers cannot associate counts using different interpretations of a footer.
    if kind == TType::Bool && size != 0 {
        return Err(invalid_metadata());
    }
    Ok((kind, size))
}

pub(super) fn read_struct_list(
    p: &mut Protocol<'_>,
    expected: usize,
    mut read: impl FnMut(&mut Protocol<'_>, usize) -> thrift::Result<()>,
) -> thrift::Result<()> {
    let (kind, size) = read_list(p)?;
    if kind != TType::Struct || size != expected {
        return Err(invalid_metadata());
    }
    for index in 0..expected {
        read(p, index)?;
    }
    Ok(())
}

/// Skip unknown values without allocation. Every loop consumes input or returns
/// an error, and recursive unknown fields have a fixed depth limit.
fn skip_value(p: &mut Protocol<'_>, kind: TType, depth: usize) -> thrift::Result<()> {
    if depth == 0 {
        return Err(invalid_metadata());
    }
    match kind {
        TType::I08 => skip_bytes(p, 1),
        TType::I16 => i16::try_from(read_i64(p)?)
            .map(|_| ())
            .map_err(|_| invalid_metadata()),
        TType::I32 => i32::try_from(read_i64(p)?)
            .map(|_| ())
            .map_err(|_| invalid_metadata()),
        TType::I64 => read_i64(p).map(|_| ()),
        TType::Double => skip_bytes(p, 8),
        TType::String => {
            let length = u32::try_from(read_varint(p)?).map_err(|_| invalid_metadata())?;
            skip_bytes(p, length as usize)
        }
        TType::Struct => {
            let mut last_id = 0;
            loop {
                let kind = read_field_header(p, &mut last_id)?;
                if kind == TType::Stop {
                    break;
                }
                if kind != TType::Bool {
                    skip_value(p, kind, depth - 1)?;
                }
            }
            Ok(())
        }
        TType::List => {
            let (kind, size) = read_list(p)?;
            for _ in 0..size {
                skip_value(p, kind, depth - 1)?;
            }
            Ok(())
        }
        _ => Err(invalid_metadata()),
    }
}
