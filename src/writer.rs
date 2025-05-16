use bytes::BytesMut;
use std::collections::BTreeMap;

use crate::{
    error::ImprintError,
    serde::Write,
    types::{DirectoryEntry, Flags, Header, ImprintRecord, SchemaId, Value},
};

/// A writer for constructing ImprintRecords by adding fields sequentially.
pub struct ImprintWriter {
    schema_id: SchemaId,
    fields: BTreeMap<u32, Value>, // keep fields in sorted order
}

impl ImprintWriter {
    /// Creates a new ImprintWriter with the given schema ID.
    pub fn new(schema_id: SchemaId) -> Result<Self, ImprintError> {
        Ok(Self {
            schema_id,
            fields: BTreeMap::new(),
        })
    }

    /// Adds a field to the record being built.
    pub fn add_field(&mut self, id: u32, value: Value) -> Result<(), ImprintError> {
        self.fields.insert(id, value);
        Ok(())
    }

    /// Consumes the writer and builds an ImprintRecord.
    pub fn build(self) -> Result<ImprintRecord, ImprintError> {
        let mut directory = Vec::with_capacity(self.fields.len());
        let mut payload = BytesMut::new();

        for (&id, value) in &self.fields {
            directory.push(DirectoryEntry {
                id,
                type_code: value.type_code(),
                offset: payload.len() as u32,
            });
            value.write(&mut payload)?;
        }

        let header = Header {
            flags: Flags::new(Flags::FIELD_DIRECTORY),
            schema_id: self.schema_id,
            payload_size: payload.len() as u32,
        };

        Ok(ImprintRecord {
            header,
            directory,
            payload: payload.freeze(),
        })
    }
}
