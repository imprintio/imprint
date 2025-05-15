use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::{
    MAGIC, VERSION,
    error::ImprintError,
    types::{DirectoryEntry, Flags, Header, ImprintRecord, SchemaId, TypeCode, Value},
    varint,
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

        Ok((
            Self {
                id,
                type_code,
                offset,
            },
            9,
        ))
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

        Ok((
            Self {
                fieldspace_id,
                schema_hash,
            },
            8,
        ))
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

        Ok((
            Self {
                header,
                directory,
                payload,
            },
            bytes_read,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::writer::ImprintWriter;
    use proptest::prelude::*;
    use proptest::strategy::{BoxedStrategy, Strategy};
    use proptest::test_runner::TestRunner;

    // Helper function to generate primitive Values
    fn arb_primitive_value() -> BoxedStrategy<Value> {
        prop_oneof![
            Just(Value::Null),
            any::<bool>().prop_map(Value::Bool),
            any::<i32>().prop_map(Value::Int32),
            any::<i64>().prop_map(Value::Int64),
            any::<f32>().prop_map(Value::Float32),
            any::<f64>().prop_map(Value::Float64),
            prop::collection::vec(any::<u8>(), 0..100).prop_map(Value::Bytes),
            ".*".prop_map(Value::String)
        ]
        .boxed()
    }

    // Helper function to generate homogeneous arrays of a specific type
    fn arb_homogeneous_array(element_gen: BoxedStrategy<Value>) -> BoxedStrategy<Value> {
        prop::collection::vec(element_gen, 1..100)
            .prop_map(Value::Array)
            .boxed()
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

    #[test]
    fn test_roundtrip_nested_record() {
        // Create an inner record with an int32 and string field
        let mut inner_writer = ImprintWriter::new(SchemaId {
            fieldspace_id: 2,
            schema_hash: 0xcafebabe,
        })
        .unwrap();
        inner_writer.add_field(1, Value::Int32(42)).unwrap();
        inner_writer
            .add_field(2, Value::String("nested".to_string()))
            .unwrap();
        let inner_record = inner_writer.build().unwrap();

        // Create an outer record containing the inner record and an int64
        let mut outer_writer = ImprintWriter::new(SchemaId {
            fieldspace_id: 1,
            schema_hash: 0xdeadbeef,
        })
        .unwrap();
        outer_writer
            .add_field(1, Value::Row(Box::new(inner_record)))
            .unwrap();
        outer_writer.add_field(2, Value::Int64(123)).unwrap();
        let outer_record = outer_writer.build().unwrap();

        // When we serialize and deserialize the outer record
        let mut buf = BytesMut::new();
        outer_record.write(&mut buf).unwrap();
        let (deserialized_record, _) = ImprintRecord::read(buf.freeze()).unwrap();

        // Then the outer record metadata should be preserved
        assert_eq!(deserialized_record.header.schema_id.fieldspace_id, 1);
        assert_eq!(deserialized_record.header.schema_id.schema_hash, 0xdeadbeef);
        assert_eq!(deserialized_record.header.flags.0, Flags::FIELD_DIRECTORY);
        assert_eq!(deserialized_record.directory.len(), 2);

        // And the outer record values should match
        let got_row = deserialized_record.get_value(1).unwrap().unwrap();
        let got_int64 = deserialized_record.get_value(2).unwrap().unwrap();
        assert_eq!(got_int64, Value::Int64(123));

        // And the inner record should be preserved
        if let Value::Row(inner) = got_row {
            assert_eq!(inner.header.schema_id.fieldspace_id, 2);
            assert_eq!(inner.header.schema_id.schema_hash, 0xcafebabe);
            assert_eq!(inner.header.flags.0, Flags::FIELD_DIRECTORY);
            assert_eq!(inner.directory.len(), 2);

            let got_inner_int = inner.get_value(1).unwrap().unwrap();
            let got_inner_str = inner.get_value(2).unwrap().unwrap();

            assert_eq!(got_inner_int, Value::Int32(42));
            assert_eq!(got_inner_str, Value::String("nested".to_string()));
        } else {
            panic!("Expected Row value");
        }
    }

    proptest! {
        #[test]
        fn test_roundtrip_simple_record(
            null in Just(Value::Null),
            boolean in any::<bool>().prop_map(Value::Bool),
            int32 in any::<i32>().prop_map(Value::Int32),
            int64 in any::<i64>().prop_map(Value::Int64),
            float32 in any::<f32>().prop_map(Value::Float32),
            float64 in any::<f64>().prop_map(Value::Float64),
            bytes_val in prop::collection::vec(any::<u8>(), 1..100).prop_map(Value::Bytes),
            string in any::<String>().prop_map(Value::String)
        ) {
            let mut writer = ImprintWriter::new(SchemaId {
                fieldspace_id: 1,
                schema_hash: 0xdeadbeef,
            }).map_err(|e| TestCaseError::fail(e.to_string()))?;

            // Add all fields
            writer.add_field(1, null.clone()).map_err(|e| TestCaseError::fail(e.to_string()))?;
            writer.add_field(2, boolean.clone()).map_err(|e| TestCaseError::fail(e.to_string()))?;
            writer.add_field(3, int32.clone()).map_err(|e| TestCaseError::fail(e.to_string()))?;
            writer.add_field(4, int64.clone()).map_err(|e| TestCaseError::fail(e.to_string()))?;
            writer.add_field(5, float32.clone()).map_err(|e| TestCaseError::fail(e.to_string()))?;
            writer.add_field(6, float64.clone()).map_err(|e| TestCaseError::fail(e.to_string()))?;
            writer.add_field(7, bytes_val.clone()).map_err(|e| TestCaseError::fail(e.to_string()))?;
            writer.add_field(8, string.clone()).map_err(|e| TestCaseError::fail(e.to_string()))?;

            // Build and serialize
            let record = writer.build().map_err(|e| TestCaseError::fail(e.to_string()))?;
            let mut buf = BytesMut::new();
            record.write(&mut buf).map_err(|e| TestCaseError::fail(e.to_string()))?;

            // Deserialize and verify
            let (record, _) = ImprintRecord::read(buf.freeze()).map_err(|e| TestCaseError::fail(e.to_string()))?;

            // Verify metadata
            prop_assert_eq!(record.header.schema_id.fieldspace_id, 1);
            prop_assert_eq!(record.header.schema_id.schema_hash, 0xdeadbeef);
            prop_assert_eq!(record.header.flags.0, Flags::FIELD_DIRECTORY);
            prop_assert_eq!(record.directory.len(), 8);

            // Verify all values are preserved
            let got = record.get_value(1).map_err(|e| TestCaseError::fail(e.to_string()))?;
            prop_assert_eq!(got, Some(null));

            let got = record.get_value(2).map_err(|e| TestCaseError::fail(e.to_string()))?;
            prop_assert_eq!(got, Some(boolean));

            let got = record.get_value(3).map_err(|e| TestCaseError::fail(e.to_string()))?;
            prop_assert_eq!(got, Some(int32));

            let got = record.get_value(4).map_err(|e| TestCaseError::fail(e.to_string()))?;
            prop_assert_eq!(got, Some(int64));

            let got = record.get_value(5).map_err(|e| TestCaseError::fail(e.to_string()))?;
            prop_assert_eq!(got, Some(float32));

            let got = record.get_value(6).map_err(|e| TestCaseError::fail(e.to_string()))?;
            prop_assert_eq!(got, Some(float64));

            let got = record.get_value(7).map_err(|e| TestCaseError::fail(e.to_string()))?;
            prop_assert_eq!(got, Some(bytes_val));

            let got = record.get_value(8).map_err(|e| TestCaseError::fail(e.to_string()))?;
            prop_assert_eq!(got, Some(string));

            // Verify non-existent field returns None
            let got = record.get_value(9).map_err(|e| TestCaseError::fail(e.to_string()))?;
            prop_assert_eq!(got, None);
        }

        #[test]
        fn prop_roundtrip_arrays(base_value in arb_primitive_value()) {
            // Skip arrays and rows as base types
            prop_assume!(!matches!(base_value, Value::Array(_) | Value::Row(_)));

            // Create a strategy for arrays of this type
            let array_strategy = match base_value {
                Value::Null => Just(Value::Null).prop_map(|_| Value::Array(vec![Value::Null; 3])).boxed(),
                Value::Bool(_) => arb_homogeneous_array(any::<bool>().prop_map(Value::Bool).boxed()),
                Value::Int32(_) => arb_homogeneous_array(any::<i32>().prop_map(Value::Int32).boxed()),
                Value::Int64(_) => arb_homogeneous_array(any::<i64>().prop_map(Value::Int64).boxed()),
                Value::Float32(_) => arb_homogeneous_array(any::<f32>().prop_map(Value::Float32).boxed()),
                Value::Float64(_) => arb_homogeneous_array(any::<f64>().prop_map(Value::Float64).boxed()),
                Value::Bytes(_) => arb_homogeneous_array(prop::collection::vec(any::<u8>(), 0..100).prop_map(Value::Bytes).boxed()),
                Value::String(_) => arb_homogeneous_array(".*".prop_map(Value::String).boxed()),
                _ => panic!("Unsupported array type"),
            };

            // When generating an array
            let array = array_strategy
                .new_tree(&mut TestRunner::default())
                .map_err(|e| TestCaseError::fail(e.to_string()))?
                .current();

            // Create a record with the array
            let mut writer = ImprintWriter::new(SchemaId {
                fieldspace_id: 1,
                schema_hash: 0xdeadbeef,
            }).map_err(|e| TestCaseError::fail(e.to_string()))?;
            writer.add_field(1, array.clone()).map_err(|e| TestCaseError::fail(e.to_string()))?;

            // Build and serialize
            let record = writer.build().map_err(|e| TestCaseError::fail(e.to_string()))?;
            let mut buf = BytesMut::new();
            record.write(&mut buf).map_err(|e| TestCaseError::fail(e.to_string()))?;

            // Deserialize and verify
            let (record, _) = ImprintRecord::read(buf.freeze()).map_err(|e| TestCaseError::fail(e.to_string()))?;
            let got = record.get_value(1).map_err(|e| TestCaseError::fail(e.to_string()))?;
            prop_assert_eq!(got, Some(array));
        }
    }

    #[test]
    fn test_duplicate_field_id() {
        let mut writer = ImprintWriter::new(SchemaId {
            fieldspace_id: 1,
            schema_hash: 0xdeadbeef,
        })
        .unwrap();

        // Add duplicate field IDs
        writer.add_field(1, Value::Int32(42)).unwrap();
        writer.add_field(1, Value::Int32(43)).unwrap();

        // Build should succeed, last value wins
        let record = writer.build().unwrap();
        assert_eq!(record.directory.len(), 1);
        assert_eq!(record.get_value(1).unwrap(), Some(Value::Int32(43)));
    }
}
