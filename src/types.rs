use bytes::Bytes;
use crate::error::ImprintError;
use crate::serde::ValueRead;

/// Magic byte that starts every Imprint record (ASCII 'I')
pub const MAGIC: u8 = 0x49;
/// Current version of the Imprint format
pub const VERSION: u8 = 0x01;

/// Flags that control how to deserialize the record
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Flags(pub(crate) u8);

impl Flags {
    /// Whether a field directory is present in the record
    pub const FIELD_DIRECTORY: u8 = 0x01;
    
    pub fn new(flags: u8) -> Self {
        Self(flags)
    }

    pub fn has_field_directory(&self) -> bool {
        self.0 & Self::FIELD_DIRECTORY != 0
    }
}

/// Type codes for field values
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TypeCode {
    Null = 0x0,
    Bool = 0x1,
    Int32 = 0x2,
    Int64 = 0x3,
    Float32 = 0x4,
    Float64 = 0x5,
    Bytes = 0x6,
    String = 0x7,
    Array = 0x8,
    Row = 0x9,
}

impl TypeCode {
    pub fn fixed_width(&self) -> Option<usize> {
        match self {
            Self::Bool => Some(1),
            Self::Int32 | Self::Float32 => Some(4),
            Self::Int64 | Self::Float64 => Some(8),
            _ => None,
        }
    }
}

impl TryFrom<u8> for TypeCode {
    type Error = ImprintError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x0 => Ok(Self::Null),
            0x1 => Ok(Self::Bool),
            0x2 => Ok(Self::Int32),
            0x3 => Ok(Self::Int64),
            0x4 => Ok(Self::Float32),
            0x5 => Ok(Self::Float64),
            0x6 => Ok(Self::Bytes),
            0x7 => Ok(Self::String),
            0x8 => Ok(Self::Array),
            0x9 => Ok(Self::Row),
            _ => Err(ImprintError::InvalidFieldType(value)),
        }
    }
}

/// A value that can be stored in an Imprint record
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Bytes(Vec<u8>),
    String(String),
    Array(Vec<Value>),
    Row(Box<ImprintRecord>),
}

impl Value {
    pub fn type_code(&self) -> TypeCode {
        match self {
            Self::Null => TypeCode::Null,
            Self::Bool(_) => TypeCode::Bool,
            Self::Int32(_) => TypeCode::Int32,
            Self::Int64(_) => TypeCode::Int64,
            Self::Float32(_) => TypeCode::Float32,
            Self::Float64(_) => TypeCode::Float64,
            Self::Bytes(_) => TypeCode::Bytes,
            Self::String(_) => TypeCode::String,
            Self::Array(_) => TypeCode::Array,
            Self::Row(_) => TypeCode::Row,
        }
    }
}

/// A directory entry describing a single field in an Imprint record.
/// Each entry has a fixed size of 9 bytes.
#[derive(Debug, Clone, PartialEq)]
pub struct DirectoryEntry {
    /// Uniquely assigned identifier within a fieldspace (4 bytes)
    pub id: u32,
    /// Field type identifier (1 byte)
    pub type_code: TypeCode,
    /// Byte position of the value relative to the payload (4 bytes)
    pub offset: u32,
}

/// A schema identifier consisting of a fieldspace ID and schema hash
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchemaId {
    pub fieldspace_id: u32,
    pub schema_hash: u32,
}

/// The header of an Imprint record
#[derive(Debug, Clone, PartialEq)]
pub struct Header {
    pub flags: Flags,
    pub schema_id: SchemaId,
}

/// An Imprint record containing a header, optional field directory, and payload
#[derive(Debug, Clone, PartialEq)]
pub struct ImprintRecord {
    pub header: Header,
    pub directory: Vec<DirectoryEntry>,
    pub payload: Bytes,
}

impl ImprintRecord {
    /// Get a value by field ID, deserializing it on demand
    pub fn get_value(&self, field_id: u32) -> Result<Option<Value>, ImprintError> {
        // TODO: directory should be a map instead of a vector
        let entry = self.directory.iter().find(|e| e.id == field_id);
        match entry {
            Some(entry) => {
                let value_bytes = self.payload.slice(entry.offset as usize..);
                let (value, _) = Value::read(entry.type_code, value_bytes)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Get the raw bytes for a field without deserializing
    pub fn get_raw_bytes(&self, field_id: u32) -> Option<Bytes> {
        let entry = self.directory.iter().find(|e| e.id == field_id)?;
        let start = entry.offset as usize;
        let next_offset = self.directory.iter()
            .find(|e| e.id > field_id)
            .map(|e| e.offset as usize)
            .unwrap_or(self.payload.len());
        Some(self.payload.slice(start..next_offset))
    }
} 