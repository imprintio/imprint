use crate::error::ImprintError;
use bytes::{Buf, BufMut, Bytes, BytesMut};

const CONTINUATION_BIT: u8 = 0x80;
const SEGMENT_BITS: u8 = 0x7f;
const MAX_VARINT_LEN: usize = 5; // Enough for u32, which is our max use case

/// Encode a u32 as a VarInt into the provided buffer
pub fn encode(value: u32, buf: &mut BytesMut) {
    let mut val = value;
    loop {
        let mut byte = (val & (SEGMENT_BITS as u32)) as u8;
        val >>= 7;
        if val != 0 {
            byte |= CONTINUATION_BIT;
        }
        buf.put_u8(byte);
        if val == 0 {
            break;
        }
    }
}

/// Decode a VarInt from the provided bytes, returning the value and number of bytes read
pub fn decode(mut bytes: Bytes) -> Result<(u32, usize), ImprintError> {
    let mut result: u32 = 0;
    let mut shift = 0;
    let mut bytes_read = 0;

    loop {
        if bytes_read >= MAX_VARINT_LEN {
            return Err(ImprintError::InvalidVarInt);
        }
        if !bytes.has_remaining() {
            return Err(ImprintError::BufferUnderflow {
                needed: 1,
                available: 0,
            });
        }

        let byte = bytes.get_u8();
        bytes_read += 1;

        // Check if adding these 7 bits would overflow
        let segment = (byte & SEGMENT_BITS) as u32;
        if shift >= 32 || (shift == 28 && segment > 0xF) {
            return Err(ImprintError::InvalidVarInt);
        }

        // Add the bottom 7 bits to the result
        result |= segment << shift;

        // If the high bit is not set, this is the last byte
        if byte & CONTINUATION_BIT == 0 {
            break;
        }

        shift += 7;
    }

    Ok((result, bytes_read))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_roundtrip_common_u32_values() {
        // Given a set of common u32 values
        let test_cases = [
            0u32,
            1,
            127,
            128,
            16383,
            16384,
            2097151,
            2097152,
            268435455,
            268435456,
            u32::MAX,
        ];

        for &value in &test_cases {
            // When encoding and then decoding the value
            let mut buf = BytesMut::new();
            encode(value, &mut buf);

            let bytes = buf.freeze();
            let (decoded, _) = decode(bytes).unwrap();

            // Then the decoded value should match the original
            assert_eq!(value, decoded, "Failed to roundtrip {}", value);
        }
    }

    #[test]
    fn should_encode_known_values_correctly() {
        // Given a set of values with known encodings
        let cases = [
            (0u32, vec![0x00]),
            (1, vec![0x01]),
            (127, vec![0x7f]),
            (128, vec![0x80, 0x01]),
            (16383, vec![0xff, 0x7f]),
            (16384, vec![0x80, 0x80, 0x01]),
        ];

        for (value, expected) in cases {
            // When encoding the value
            let mut buf = BytesMut::new();
            encode(value, &mut buf);

            // Then the encoding should match the expected bytes
            assert_eq!(&buf[..], &expected[..], "Encoding failed for {}", value);

            // And when decoding
            let (decoded, len) = decode(buf.freeze()).unwrap();

            // Then the decoded value and length should be correct
            assert_eq!(decoded, value, "Decoding failed for {}", value);
            assert_eq!(len, expected.len(), "Wrong length for {}", value);
        }
    }

    #[test]
    fn should_handle_error_cases_correctly() {
        // Given a truncated input
        let mut buf = BytesMut::new();
        encode(16384, &mut buf);
        buf.truncate(buf.len() - 1);

        // When decoding truncated input
        // Then it should return a buffer underflow error
        assert!(matches!(
            decode(buf.freeze()),
            Err(ImprintError::BufferUnderflow { .. })
        ));

        // Given an overlong encoding
        let buf = Bytes::from(vec![0x80, 0x80, 0x80, 0x80, 0x80, 0x01]);

        // When decoding overlong input
        // Then it should return an invalid varint error
        assert!(matches!(decode(buf), Err(ImprintError::InvalidVarInt)));

        // Given a value that's too large
        let buf = Bytes::from(vec![0x80, 0x80, 0x80, 0x80, 0x10]);

        // When decoding the too-large value
        // Then it should return an invalid varint error
        assert!(matches!(decode(buf), Err(ImprintError::InvalidVarInt)));
    }
}
