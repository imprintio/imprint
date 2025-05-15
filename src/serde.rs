use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::{
    error::ImprintError,
    types::{DirectoryEntry, Flags, Header, ImprintRecord, SchemaId, TypeCode, Value},
    varint,
    MAGIC, VERSION,
};

/// A trait for types that can be written to a byte buffer
pub trait Write {
    /// Write the value to the buffer in the Imprint format
    fn write(&self, buf: &mut BytesMut) -> Result<(), ImprintError>;
}

/// A trait for types that can be read from a byte buffer
pub trait Read: Sized {
    /// Read a value from the buffer, returning the value and number of bytes read
    fn read(bytes: Bytes) -> Result<(Self, usize), ImprintError>;
}

/// A trait for reading values with a known type code
pub trait ValueRead: Sized {
    /// Read a value from the buffer with a known type code, returning the value and number of bytes read
    fn read(type_code: TypeCode, bytes: Bytes) -> Result<(Self, usize), ImprintError>;
}

impl Write for Value {
    fn write(&self, buf: &mut BytesMut) -> Result<(), ImprintError> {
        match self {
            Self::Null => Ok(()),
            Self::Bool(v) => {
                buf.put_u8(if *v { 1 } else { 0 });
                Ok(())
            }
            Self::Int32(v) => {
                buf.put_i32_le(*v);
                Ok(())
            }
            Self::Int64(v) => {
                buf.put_i64_le(*v);
                Ok(())
            }
            Self::Float32(v) => {
                buf.put_f32_le(*v);
                Ok(())
            }
            Self::Float64(v) => {
                buf.put_f64_le(*v);
                Ok(())
            }
            Self::Bytes(v) => {
                varint::encode(v.len() as u32, buf);
                buf.put_slice(v);
                Ok(())
            }
            Self::String(v) => {
                let bytes = v.as_bytes();
                varint::encode(bytes.len() as u32, buf);
                buf.put_slice(bytes);
                Ok(())
            }
            Self::Array(v) => {
                if v.is_empty() {
                    return Err(ImprintError::SchemaError("empty array not allowed".into()));
                }
                let type_code = v[0].type_code();
                buf.put_u8(type_code as u8);
                varint::encode(v.len() as u32, buf);
                for value in v {
                    if value.type_code() != type_code {
                        return Err(ImprintError::SchemaError(
                            "array elements must have same type".into(),
                        ));
                    }
                    value.write(buf)?;
                }
                Ok(())
            }
            Self::Row(v) => v.write(buf),
        }
    }
}

impl ValueRead for Value {
    fn read(type_code: TypeCode, mut bytes: Bytes) -> Result<(Self, usize), ImprintError> {
        let mut bytes_read = 0;

        let value = match type_code {
            TypeCode::Null => Value::Null,
            TypeCode::Bool => {
                if !bytes.has_remaining() {
                    return Err(ImprintError::BufferUnderflow {
                        needed: 1,
                        available: 0,
                    });
                }
                let v = bytes.get_u8();
                bytes_read += 1;
                match v {
                    0 => Value::Bool(false),
                    1 => Value::Bool(true),
                    _ => return Err(ImprintError::SchemaError("invalid boolean value".into())),
                }
            }
            TypeCode::Int32 => {
                if bytes.remaining() < 4 {
                    return Err(ImprintError::BufferUnderflow {
                        needed: 4,
                        available: bytes.remaining(),
                    });
                }
                let v = bytes.get_i32_le();
                bytes_read += 4;
                Value::Int32(v)
            }
            TypeCode::Int64 => {
                if bytes.remaining() < 8 {
                    return Err(ImprintError::BufferUnderflow {
                        needed: 8,
                        available: bytes.remaining(),
                    });
                }
                let v = bytes.get_i64_le();
                bytes_read += 8;
                Value::Int64(v)
            }
            TypeCode::Float32 => {
                if bytes.remaining() < 4 {
                    return Err(ImprintError::BufferUnderflow {
                        needed: 4,
                        available: bytes.remaining(),
                    });
                }
                let v = bytes.get_f32_le();
                bytes_read += 4;
                Value::Float32(v)
            }
            TypeCode::Float64 => {
                if bytes.remaining() < 8 {
                    return Err(ImprintError::BufferUnderflow {
                        needed: 8,
                        available: bytes.remaining(),
                    });
                }
                let v = bytes.get_f64_le();
                bytes_read += 8;
                Value::Float64(v)
            }
            TypeCode::Bytes => {
                let (len, len_size) = varint::decode(bytes.clone())?;
                bytes.advance(len_size);
                bytes_read += len_size;

                if bytes.remaining() < len as usize {
                    return Err(ImprintError::BufferUnderflow {
                        needed: len as usize,
                        available: bytes.remaining(),
                    });
                }
                let mut v = vec![0; len as usize];
                bytes.copy_to_slice(&mut v);
                bytes_read += len as usize;
                Value::Bytes(v)
            }
            TypeCode::String => {
                let (len, len_size) = varint::decode(bytes.clone())?;
                bytes.advance(len_size);
                bytes_read += len_size;

                if bytes.remaining() < len as usize {
                    return Err(ImprintError::BufferUnderflow {
                        needed: len as usize,
                        available: bytes.remaining(),
                    });
                }
                let mut v = vec![0; len as usize];
                bytes.copy_to_slice(&mut v);
                bytes_read += len as usize;
                let s = String::from_utf8(v).map_err(|_| ImprintError::InvalidUtf8String)?;
                Value::String(s)
            }
            TypeCode::Array => {
                let element_type = TypeCode::try_from(bytes.get_u8())?;
                bytes_read += 1;

                let (len, len_size) = varint::decode(bytes.clone())?;
                bytes.advance(len_size);
                bytes_read += len_size;

                let mut values = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    let (value, value_size) = Self::read(element_type, bytes.clone())?;
                    bytes.advance(value_size);
                    bytes_read += value_size;
                    values.push(value);
                }
                Value::Array(values)
            }
            TypeCode::Row => {
                let (record, size) = ImprintRecord::read(bytes)?;
                bytes_read += size;
                Value::Row(Box::new(record))
            }
        };
        Ok((value, bytes_read))
    }
}

impl Write for DirectoryEntry {
    fn write(&self, buf: &mut BytesMut) -> Result<(), ImprintError> {
        buf.put_u32_le(self.id);
        buf.put_u8(self.type_code as u8);
        buf.put_u32_le(self.offset);
        Ok(())
    }
}

impl Read for DirectoryEntry {
    fn read(mut bytes: Bytes) -> Result<(Self, usize), ImprintError> {
        if bytes.remaining() < 9 {
            return Err(ImprintError::BufferUnderflow {
                needed: 9,
                available: bytes.remaining(),
            });
        }

        let id = bytes.get_u32_le();
        let type_code = TypeCode::try_from(bytes.get_u8())?;
        let offset = bytes.get_u32_le();

        Ok((Self { id, type_code, offset }, 9))
    }
}

impl Write for SchemaId {
    fn write(&self, buf: &mut BytesMut) -> Result<(), ImprintError> {
        buf.put_u32_le(self.fieldspace_id);
        buf.put_u32_le(self.schema_hash);
        Ok(())
    }
}

impl Read for SchemaId {
    fn read(mut bytes: Bytes) -> Result<(Self, usize), ImprintError> {
        if bytes.remaining() < 8 {
            return Err(ImprintError::BufferUnderflow {
                needed: 8,
                available: bytes.remaining(),
            });
        }

        let fieldspace_id = bytes.get_u32_le();
        let schema_hash = bytes.get_u32_le();

        Ok((Self { fieldspace_id, schema_hash }, 8))
    }
}

impl Write for Header {
    fn write(&self, buf: &mut BytesMut) -> Result<(), ImprintError> {
        buf.put_u8(MAGIC);
        buf.put_u8(VERSION);
        buf.put_u8(self.flags.0);
        self.schema_id.write(buf)?;
        Ok(())
    }
}

impl Read for Header {
    fn read(mut bytes: Bytes) -> Result<(Self, usize), ImprintError> {
        if bytes.remaining() < 11 {
            return Err(ImprintError::BufferUnderflow {
                needed: 11,
                available: bytes.remaining(),
            });
        }

        let magic = bytes.get_u8();
        if magic != MAGIC {
            return Err(ImprintError::InvalidMagic(magic));
        }

        let version = bytes.get_u8();
        if version != VERSION {
            return Err(ImprintError::UnsupportedVersion(version));
        }

        let flags = Flags::new(bytes.get_u8());
        let (schema_id, _) = SchemaId::read(bytes.clone())?;
        bytes.advance(8);

        Ok((Self { flags, schema_id }, 11))
    }
}

impl Write for ImprintRecord {
    fn write(&self, buf: &mut BytesMut) -> Result<(), ImprintError> {
        self.header.write(buf)?;

        if self.header.flags.has_field_directory() {
            varint::encode(self.directory.len() as u32, buf);
            for entry in &self.directory {
                entry.write(buf)?;
            }
        }

        buf.put_slice(&self.payload);

        Ok(())
    }
}

impl Read for ImprintRecord {
    fn read(mut bytes: Bytes) -> Result<(Self, usize), ImprintError> {
        let mut bytes_read = 0;

        let (header, header_size) = Header::read(bytes.clone())?;
        bytes.advance(header_size);
        bytes_read += header_size;

        let mut directory = Vec::new();
        if header.flags.has_field_directory() {
            let (count, count_size) = varint::decode(bytes.clone())?;
            bytes.advance(count_size);
            bytes_read += count_size;

            for _ in 0..count {
                let (entry, entry_size) = DirectoryEntry::read(bytes.clone())?;
                bytes.advance(entry_size);
                bytes_read += entry_size;
                directory.push(entry);
            }
        }

        let payload = bytes.slice(..);
        bytes_read = bytes.len();

        Ok((Self { header, directory, payload }, bytes_read))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_roundtrip_simple_record() {
        // Given a simple record with a few fields
        let mut buf = BytesMut::new();
        
        // Write header
        let header = Header {
            flags: Flags::new(Flags::FIELD_DIRECTORY),
            schema_id: SchemaId {
                fieldspace_id: 1,
                schema_hash: 0xdeadbeef,
            },
        };
        header.write(&mut buf).unwrap();

        // Write directory count
        varint::encode(2, &mut buf);

        // Write directory entries
        let entries = vec![
            DirectoryEntry {
                id: 1,
                type_code: TypeCode::Int32,
                offset: 0,
            },
            DirectoryEntry {
                id: 2,
                type_code: TypeCode::String,
                offset: 4,
            },
        ];
        for entry in &entries {
            entry.write(&mut buf).unwrap();
        }

        // Write values
        Value::Int32(42).write(&mut buf).unwrap();
        Value::String("hello".to_string()).write(&mut buf).unwrap();

        // When reading back
        let bytes = buf.freeze();
        let (record, _) = ImprintRecord::read(bytes).unwrap();

        // Then the record metadata should match
        assert_eq!(record.header.schema_id.fieldspace_id, 1);
        assert_eq!(record.header.schema_id.schema_hash, 0xdeadbeef);
        assert_eq!(record.header.flags.0, Flags::FIELD_DIRECTORY);
        assert_eq!(record.directory.len(), 2);

        // And we can read back the values
        assert_eq!(record.get_value(1).unwrap(), Some(Value::Int32(42)));
        assert_eq!(record.get_value(2).unwrap(), Some(Value::String("hello".to_string())));
        assert_eq!(record.get_value(3).unwrap(), None);
    }

    #[test]
    fn should_roundtrip_nested_record() {
        // Given a nested record
        let nested = ImprintRecord {
            header: Header {
                flags: Flags::new(Flags::FIELD_DIRECTORY),
                schema_id: SchemaId {
                    fieldspace_id: 2,
                    schema_hash: 0xcafebabe,
                },
            },
            directory: vec![
                DirectoryEntry {
                    id: 1,
                    type_code: TypeCode::Int32,
                    offset: 0,
                },
            ],
            payload: {
                let mut buf = BytesMut::new();
                Value::Int32(42).write(&mut buf).unwrap();
                buf.freeze()
            },
        };

        // When writing and reading back
        let mut buf = BytesMut::new();
        Value::Row(Box::new(nested)).write(&mut buf).unwrap();
        let bytes = buf.freeze();
        let (value, _) = Value::read(TypeCode::Row, bytes).unwrap();

        // Then it should match the original
        match value {
            Value::Row(record) => {
                assert_eq!(record.get_value(1).unwrap(), Some(Value::Int32(42)));
            }
            _ => panic!("Expected row value"),
        }
    }

    #[test]
    fn should_handle_error_cases() {
        // Given an invalid magic byte
        let mut buf = BytesMut::new();
        buf.put_u8(0x00); // Wrong magic
        buf.put_u8(VERSION);
        buf.put_u8(0x00);
        buf.put_u64_le(0);

        // When trying to read
        // Then it should return an invalid magic error
        assert!(matches!(
            ImprintRecord::read(buf.freeze()),
            Err(ImprintError::InvalidMagic(0x00))
        ));

        // Given an invalid version
        let mut buf = BytesMut::new();
        buf.put_u8(MAGIC);
        buf.put_u8(0xFF); // Wrong version
        buf.put_u8(0x00);
        buf.put_u64_le(0);

        // When trying to read
        // Then it should return an unsupported version error
        assert!(matches!(
            ImprintRecord::read(buf.freeze()),
            Err(ImprintError::UnsupportedVersion(0xFF))
        ));

        // Given a truncated buffer
        let mut buf = BytesMut::new();
        buf.put_u8(MAGIC);
        buf.put_u8(VERSION);

        // When trying to read
        // Then it should return a buffer underflow error
        assert!(matches!(
            ImprintRecord::read(buf.freeze()),
            Err(ImprintError::BufferUnderflow { .. })
        ));
    }
} 