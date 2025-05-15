use crate::{
    error::ImprintError,
    types::{DirectoryEntry, Header, ImprintRecord, SchemaId},
};
use bytes::{BufMut, BytesMut};

pub trait Project {
    fn project(&self, field_ids: &[u32]) -> Result<ImprintRecord, ImprintError>;
}
impl Project for ImprintRecord {
    fn project(&self, field_ids: &[u32]) -> Result<ImprintRecord, ImprintError> {
        // Create new directory entries for the projected fields
        let mut new_directory = Vec::with_capacity(field_ids.len());
        let mut new_payload = BytesMut::new();

        // Sort and deduplicate the field IDs for efficient matching with sorted directory
        let mut sorted_field_ids = field_ids.to_vec();
        sorted_field_ids.sort_unstable();
        sorted_field_ids.dedup();

        // Since both arrays are now sorted, we can do a single pass through both
        let mut dir_idx = 0;
        let mut field_idx = 0;

        // we do a linear scan through both, though we could consider a binary search
        // if the projection is sparse enough (maybe there is some heuristic we can
        // use to determine this)
        while dir_idx < self.directory.len() && field_idx < sorted_field_ids.len() {
            let entry = &self.directory[dir_idx];
            let field_id = sorted_field_ids[field_idx];

            match entry.id.cmp(&field_id) {
                std::cmp::Ordering::Equal => {
                    // Create new directory entry with updated offset
                    let new_entry = DirectoryEntry {
                        id: entry.id,
                        type_code: entry.type_code,
                        offset: new_payload.len() as u32,
                    };
                    new_directory.push(new_entry);

                    // Copy the bytes directly from the original payload - unwrap
                    // is safe because we got the field_id from the directory so
                    // it should exist in the original payload
                    new_payload.put_slice(&self.get_raw_bytes(field_id).unwrap());

                    dir_idx += 1;
                    field_idx += 1;
                }
                std::cmp::Ordering::Less => {
                    dir_idx += 1;
                }
                std::cmp::Ordering::Greater => {
                    field_idx += 1;
                }
            }
        }

        // Create the projected record with same header but new directory and payload
        Ok(ImprintRecord {
            header: Header {
                flags: self.header.flags,
                // TODO: we need to generate a new schema id for the projected record
                schema_id: SchemaId {
                    fieldspace_id: 0xdead,
                    schema_hash: 0xbeef,
                },
            },
            directory: new_directory,
            payload: new_payload.freeze(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ImprintWriter, Value};

    fn create_test_record() -> ImprintRecord {
        let mut writer = ImprintWriter::new(SchemaId {
            fieldspace_id: 1,
            schema_hash: 0xdeadbeef,
        })
        .unwrap();

        writer.add_field(1, Value::Int32(42)).unwrap();
        writer
            .add_field(3, Value::String("hello".to_string()))
            .unwrap();
        writer.add_field(5, Value::Bool(true)).unwrap();
        writer.add_field(7, Value::Bytes(vec![1, 2, 3])).unwrap();

        writer.build().unwrap()
    }

    #[test]
    fn should_project_subset_of_fields() {
        // Given a record with multiple fields
        let record = create_test_record();

        // When projecting a subset of fields
        let projected = record.project(&[1, 5]).unwrap();

        // Then only the requested fields should be present
        assert_eq!(projected.directory.len(), 2);
        assert_eq!(projected.get_value(1).unwrap(), Some(Value::Int32(42)));
        assert_eq!(projected.get_value(5).unwrap(), Some(Value::Bool(true)));

        // And non-requested fields should be absent
        assert_eq!(projected.get_value(3).unwrap(), None);
        assert_eq!(projected.get_value(7).unwrap(), None);
    }

    #[test]
    fn should_maintain_field_order_regardless_of_input() {
        // Given a record with multiple fields
        let record = create_test_record();

        // When projecting fields in arbitrary order
        let projected = record.project(&[7, 1, 5, 3]).unwrap();

        // Then all requested fields should be present with correct values
        assert_eq!(projected.directory.len(), 4);
        assert_eq!(projected.get_value(1).unwrap(), Some(Value::Int32(42)));
        assert_eq!(
            projected.get_value(3).unwrap(),
            Some(Value::String("hello".to_string()))
        );
        assert_eq!(projected.get_value(5).unwrap(), Some(Value::Bool(true)));
        assert_eq!(
            projected.get_value(7).unwrap(),
            Some(Value::Bytes(vec![1, 2, 3]))
        );

        // And directory should maintain sorted order
        let dir_ids: Vec<u32> = projected.directory.iter().map(|e| e.id).collect();
        assert!(
            dir_ids.windows(2).all(|w| w[0] < w[1]),
            "directory entries should be sorted by field id"
        );
    }

    #[test]
    fn should_handle_single_field_projection() {
        // Given a record with multiple fields
        let record = create_test_record();

        // When projecting a single field
        let projected = record.project(&[3]).unwrap();

        // Then only that field should be present
        assert_eq!(projected.directory.len(), 1);
        assert_eq!(
            projected.get_value(3).unwrap(),
            Some(Value::String("hello".to_string()))
        );
    }

    #[test]
    fn should_preserve_all_fields_when_projecting_all() {
        // Given a record with multiple fields
        let record = create_test_record();
        let all_fields: Vec<u32> = record.directory.iter().map(|e| e.id).collect();

        // When projecting all fields
        let projected = record.project(&all_fields).unwrap();

        // Then all fields should be present with matching values
        assert_eq!(projected.directory.len(), record.directory.len());
        for entry in &record.directory {
            assert_eq!(
                projected.get_value(entry.id).unwrap(),
                record.get_value(entry.id).unwrap(),
                "field {} should have matching value",
                entry.id
            );
        }
    }

    #[test]
    fn should_handle_empty_projection() {
        // Given a record with multiple fields
        let record = create_test_record();

        // When projecting no fields
        let projected = record.project(&[]).unwrap();

        // Then result should be empty but valid
        assert_eq!(projected.directory.len(), 0);
        assert!(projected.payload.is_empty());
    }

    #[test]
    fn should_ignore_nonexistent_fields() {
        // Given a record with multiple fields
        let record = create_test_record();

        // When projecting mix of existing and non-existing fields
        let projected = record.project(&[1, 99, 100]).unwrap();

        // Then only existing fields should be included
        assert_eq!(projected.directory.len(), 1);
        assert_eq!(projected.get_value(1).unwrap(), Some(Value::Int32(42)));
        assert_eq!(projected.get_value(99).unwrap(), None);
        assert_eq!(projected.get_value(100).unwrap(), None);
    }

    #[test]
    fn should_deduplicate_requested_fields() {
        // Given a record with multiple fields
        let record = create_test_record();

        // When projecting the same field multiple times
        let projected = record.project(&[1, 1, 1]).unwrap();

        // Then field should only appear once
        assert_eq!(projected.directory.len(), 1);
        assert_eq!(projected.get_value(1).unwrap(), Some(Value::Int32(42)));
    }

    #[test]
    fn should_handle_projection_from_empty_record() {
        // Given an empty record
        let writer = ImprintWriter::new(SchemaId {
            fieldspace_id: 1,
            schema_hash: 0xdeadbeef,
        })
        .unwrap();
        let empty_record = writer.build().unwrap();

        // When projecting any fields
        let projected = empty_record.project(&[1, 2, 3]).unwrap();

        // Then result should be empty but valid
        assert_eq!(projected.directory.len(), 0);
        assert!(projected.payload.is_empty());
    }

    #[test]
    fn should_preserve_exact_byte_representation() {
        // Given a record with multiple fields
        let record = create_test_record();
        let original_bytes = record.get_raw_bytes(3).unwrap();

        // When projecting a field
        let projected = record.project(&[3]).unwrap();

        // Then the byte representation should be exactly preserved
        let projected_bytes = projected.get_raw_bytes(3).unwrap();
        assert_eq!(
            original_bytes, projected_bytes,
            "byte representation should be identical"
        );
    }

    #[test]
    fn should_reduce_payload_size_when_projecting_subset() {
        // Given a record with multiple fields including some large values
        let mut writer = ImprintWriter::new(SchemaId {
            fieldspace_id: 1,
            schema_hash: 0xdeadbeef,
        })
        .unwrap();

        // Add a mix of small and large fields
        writer.add_field(1, Value::Int32(42)).unwrap(); // 4 bytes
        writer
            .add_field(2, Value::String("a".repeat(1000)))
            .unwrap(); // ~1000 bytes
        writer.add_field(3, Value::Int64(123)).unwrap(); // 8 bytes
        writer.add_field(4, Value::Bytes(vec![0; 500])).unwrap(); // 500 bytes
        let record = writer.build().unwrap();

        let original_payload_size = record.payload.len();

        // When projecting only the small fields
        let projected = record.project(&[1, 3]).unwrap();

        // Then the payload size should be significantly smaller
        assert!(
            projected.payload.len() < original_payload_size,
            "projected payload size ({}) should be less than original ({})",
            projected.payload.len(),
            original_payload_size
        );

        // And should be close to expected size for just the projected fields
        let expected_size = 4 + 8; // int32 + int64
        assert!(
            (projected.payload.len() as i64 - expected_size).abs() <= 2,
            "projected payload size ({}) should be close to expected size for int32 + int64 ({})",
            projected.payload.len(),
            expected_size
        );

        // And the values should still be correct
        assert_eq!(projected.get_value(1).unwrap(), Some(Value::Int32(42)));
        assert_eq!(projected.get_value(3).unwrap(), Some(Value::Int64(123)));
    }
}
