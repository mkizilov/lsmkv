//! Little-endian fixed-width integers and unsigned LEB128 varints.

pub fn put_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_le_bytes());
}

pub fn put_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_le_bytes());
}

pub fn get_u32(input: &mut &[u8]) -> Option<u32> {
    let value = u32::from_le_bytes(input.get(..4)?.try_into().ok()?);
    *input = input.get(4..)?;
    Some(value)
}

pub fn get_u64(input: &mut &[u8]) -> Option<u64> {
    let value = u64::from_le_bytes(input.get(..8)?.try_into().ok()?);
    *input = input.get(8..)?;
    Some(value)
}

pub fn put_varint(output: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        output.push((value as u8) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

/// Decode a canonical u64 varint without consuming input on failure.
pub fn get_varint(input: &mut &[u8]) -> Option<u64> {
    let mut value = 0u64;
    for i in 0..10 {
        let byte = *input.get(i)?;
        if i == 9 && byte > 1 {
            return None;
        }
        value |= u64::from(byte & 0x7f) << (i * 7);
        if byte & 0x80 == 0 {
            if i > 0 && byte == 0 {
                return None;
            }
            *input = input.get(i + 1..)?;
            return Some(value);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{get_u32, get_u64, get_varint, put_u32, put_u64, put_varint};

    #[test]
    fn fixed_width_little_endian_and_short_inputs() {
        let mut bytes = Vec::new();
        put_u32(&mut bytes, 0x12345678);
        put_u64(&mut bytes, 0x0102030405060708);
        assert_eq!(bytes, [0x78, 0x56, 0x34, 0x12, 8, 7, 6, 5, 4, 3, 2, 1]);
        let mut input = bytes.as_slice();
        assert_eq!(get_u32(&mut input), Some(0x12345678));
        assert_eq!(get_u64(&mut input), Some(0x0102030405060708));
        assert_eq!(get_u32(&mut input), None);
        let mut short = &[1, 2, 3][..];
        assert_eq!(get_u64(&mut short), None);
        assert_eq!(short, &[1, 2, 3]);
    }

    #[test]
    fn varint_boundaries_and_invalid_encodings() {
        for value in [0, 1, 127, 128, 16383, 16384, u32::MAX as u64, u64::MAX] {
            let mut bytes = Vec::new();
            put_varint(&mut bytes, value);
            let mut input = bytes.as_slice();
            assert_eq!(get_varint(&mut input), Some(value));
            assert!(input.is_empty());
        }
        for bytes in [
            &[0x80][..],
            &[0x80, 0][..],
            &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 2][..],
        ] {
            let mut input = bytes;
            assert_eq!(get_varint(&mut input), None);
            assert_eq!(input, bytes);
        }
    }
}
