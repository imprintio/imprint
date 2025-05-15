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

#[derive(Debug, Clone, Copy, Default)]
pub struct MergeOptions {
    /// If true, duplicate fields from the second record will be filtered out of the payload
    /// If false, they will remain in the payload but won't be accessible via the directory
    pub filter_duplicate_payloads: bool,
}

pub trait Merge {
    /// Merge another record into this one, using default options.
    /// By default, duplicate fields from the second record will be kept in the payload
    /// but won't be accessible via the directory.
    fn merge(&self, other: &ImprintRecord) -> Result<ImprintRecord, ImprintError> {
        self.merge_with_opts(other, MergeOptions::default())
    }

    /// Merge another record into this one with specific options for handling duplicates.
    fn merge_with_opts(
        &self,
        other: &ImprintRecord,
        options: MergeOptions,
    ) -> Result<ImprintRecord, ImprintError>;
}

impl Merge for ImprintRecord {
    fn merge_with_opts(
        &self,
        other: &ImprintRecord,
        options: MergeOptions,
    ) -> Result<ImprintRecord, ImprintError> {
        // we just shrink the directory and payload to the exact size we need at the end of the
        // merge and allocate the largest possible sizes up front assuming that the records do
        // not have significant overlaping fields
        let mut new_directory = Vec::with_capacity(self.directory.len() + other.directory.len());
        let mut new_payload = BytesMut::with_capacity(self.payload.len() + other.payload.len());

        new_directory.extend_from_slice(&self.directory);
        new_payload.extend_from_slice(&self.payload);

        // Track field IDs from first record for deduplication
        let first_field_ids: std::collections::HashSet<u32> =
            self.directory.iter().map(|e| e.id).collect();

        let base_offset = new_payload.len() as u32;

        if options.filter_duplicate_payloads {
            // If filtering duplicates, we need to process each field individually
            let mut current_offset = 0u32;
            for entry in &other.directory {
                if first_field_ids.contains(&entry.id) {
                    continue;
                }

                let field_bytes = other.get_raw_bytes(entry.id).unwrap();

                // Add adjusted directory entry
                let new_entry = DirectoryEntry {
                    id: entry.id,
                    type_code: entry.type_code,
                    offset: base_offset + current_offset,
                };
                new_directory.push(new_entry);

                // Copy corresponding payload
                new_payload.extend_from_slice(field_bytes.as_ref());
                current_offset += field_bytes.len() as u32;
            }
        } else {
            // If not filtering duplicates, we can just append the entire payload
            new_payload.extend_from_slice(&other.payload);

            // Add all non-duplicate directory entries with adjusted offsets
            for entry in &other.directory {
                if !first_field_ids.contains(&entry.id) {
                    let new_entry = DirectoryEntry {
                        id: entry.id,
                        type_code: entry.type_code,
                        offset: base_offset + entry.offset,
                    };
                    new_directory.push(new_entry);
                }
            }
        }

        // Sort directory by field ID to maintain canonical form
        new_directory.sort_by_key(|e| e.id);

        // Shrink allocations to fit actual data
        new_directory.shrink_to_fit();
        let mut exact_payload = BytesMut::with_capacity(new_payload.len());
        exact_payload.extend_from_slice(&new_payload);

        Ok(ImprintRecord {
            header: Header {
                flags: self.header.flags,
                schema_id: self.header.schema_id,
            },
            directory: new_directory,
            payload: exact_payload.freeze(),
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

    #[test]
    fn should_merge_records_with_distinct_fields() {
        // Given two records with different fields
        let mut writer1 = ImprintWriter::new(SchemaId {
            fieldspace_id: 1,
            schema_hash: 0xdeadbeef,
        })
        .unwrap();
        writer1.add_field(1, Value::Int32(42)).unwrap();
        writer1
            .add_field(3, Value::String("hello".to_string()))
            .unwrap();
        let record1 = writer1.build().unwrap();

        let mut writer2 = ImprintWriter::new(SchemaId {
            fieldspace_id: 1,
            schema_hash: 0xcafebabe,
        })
        .unwrap();
        writer2.add_field(2, Value::Bool(true)).unwrap();
        writer2.add_field(4, Value::Int64(123)).unwrap();
        let record2 = writer2.build().unwrap();

        // When merging the records
        let merged = record1.merge(&record2).unwrap();

        // Then all fields should be present
        assert_eq!(merged.directory.len(), 4);
        assert_eq!(merged.get_value(1).unwrap(), Some(Value::Int32(42)));
        assert_eq!(merged.get_value(2).unwrap(), Some(Value::Bool(true)));
        assert_eq!(
            merged.get_value(3).unwrap(),
            Some(Value::String("hello".to_string()))
        );
        assert_eq!(merged.get_value(4).unwrap(), Some(Value::Int64(123)));
    }

    #[test]
    fn should_handle_duplicate_fields_keeping_first() {
        // Given two records with overlapping fields
        let mut writer1 = ImprintWriter::new(SchemaId {
            fieldspace_id: 1,
            schema_hash: 0xdeadbeef,
        })
        .unwrap();
        writer1.add_field(1, Value::Int32(42)).unwrap();
        writer1
            .add_field(2, Value::String("first".to_string()))
            .unwrap();
        let record1 = writer1.build().unwrap();

        let mut writer2 = ImprintWriter::new(SchemaId {
            fieldspace_id: 1,
            schema_hash: 0xcafebabe,
        })
        .unwrap();
        writer2
            .add_field(2, Value::String("second".to_string()))
            .unwrap();
        writer2.add_field(3, Value::Bool(true)).unwrap();
        let record2 = writer2.build().unwrap();

        // When merging with default options (keep zombie data)
        let merged = record1.merge(&record2).unwrap();

        // Then first occurrence of duplicate fields should be kept
        assert_eq!(merged.directory.len(), 3);
        assert_eq!(merged.get_value(1).unwrap(), Some(Value::Int32(42)));
        assert_eq!(
            merged.get_value(2).unwrap(),
            Some(Value::String("first".to_string()))
        );
        assert_eq!(merged.get_value(3).unwrap(), Some(Value::Bool(true)));

        // And payload should be larger due to zombie data
        let filtered_merged = record1
            .merge_with_opts(
                &record2,
                MergeOptions {
                    filter_duplicate_payloads: true,
                },
            )
            .unwrap();
        assert!(merged.payload.len() > filtered_merged.payload.len());
    }

    #[test]
    fn should_preserve_schema_id_from_first_record() {
        // Given two records with different schema IDs
        let schema1 = SchemaId {
            fieldspace_id: 1,
            schema_hash: 0xdeadbeef,
        };
        let mut writer1 = ImprintWriter::new(schema1).unwrap();
        writer1.add_field(1, Value::Int32(42)).unwrap();
        let record1 = writer1.build().unwrap();

        let schema2 = SchemaId {
            fieldspace_id: 1,
            schema_hash: 0xcafebabe,
        };
        let mut writer2 = ImprintWriter::new(schema2).unwrap();
        writer2.add_field(2, Value::Bool(true)).unwrap();
        let record2 = writer2.build().unwrap();

        // When merging the records
        let merged = record1.merge(&record2).unwrap();

        // Then schema ID from first record should be preserved
        assert_eq!(merged.header.schema_id, schema1);
    }
}
