use crate::error::ImprintError;
use crate::serde::ValueRead;
use bytes::Bytes;

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

/// All the atomic values we consider “primitive”
#[derive(Debug, Clone, PartialEq)]
pub enum PrimitiveValue {
    Null,
    Bool(bool),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Bytes(Vec<u8>),
    String(String),
}

/// All the composite values that contain (and compose) other Values
#[derive(Debug, Clone, PartialEq)]
pub enum ComplexValue {
    Array(Vec<Value>),
    Row(Box<ImprintRecord>),
}

/// The top-level Value type
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Primitive(PrimitiveValue),
    Complex(ComplexValue),
}

impl Value {
    pub fn type_code(&self) -> TypeCode {
        match self {
            Self::Primitive(PrimitiveValue::Null) => TypeCode::Null,
            Self::Primitive(PrimitiveValue::Bool(_)) => TypeCode::Bool,
            Self::Primitive(PrimitiveValue::Int32(_)) => TypeCode::Int32,
            Self::Primitive(PrimitiveValue::Int64(_)) => TypeCode::Int64,
            Self::Primitive(PrimitiveValue::Float32(_)) => TypeCode::Float32,
            Self::Primitive(PrimitiveValue::Float64(_)) => TypeCode::Float64,
            Self::Primitive(PrimitiveValue::Bytes(_)) => TypeCode::Bytes,
            Self::Primitive(PrimitiveValue::String(_)) => TypeCode::String,
            Self::Complex(ComplexValue::Array(_)) => TypeCode::Array,
            Self::Complex(ComplexValue::Row(_)) => TypeCode::Row,
        }
    }

    pub fn is_primitive(&self) -> bool {
        matches!(self, Value::Primitive(_))
    }
    pub fn is_complex(&self) -> bool {
        matches!(self, Value::Complex(_))
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Value::Primitive(PrimitiveValue::Bool(b))
    }
}

impl From<i32> for Value {
    fn from(i: i32) -> Self {
        Value::Primitive(PrimitiveValue::Int32(i))
    }
}

impl From<i64> for Value {
    fn from(i: i64) -> Self {
        Value::Primitive(PrimitiveValue::Int64(i))
    }
}

impl From<f32> for Value {
    fn from(f: f32) -> Self {
        Value::Primitive(PrimitiveValue::Float32(f))
    }
}

impl From<f64> for Value {
    fn from(f: f64) -> Self {
        Value::Primitive(PrimitiveValue::Float64(f))
    }
}

impl From<Vec<u8>> for Value {
    fn from(b: Vec<u8>) -> Self {
        Value::Primitive(PrimitiveValue::Bytes(b))
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::Primitive(PrimitiveValue::String(s))
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::Primitive(PrimitiveValue::String(s.to_string()))
    }
}

// Any collection of values that can be converted to an array
impl<T: Into<Value>> From<Vec<T>> for Value {
    fn from(v: Vec<T>) -> Self {
        // Map each value to its Value::Primitive
        let values = v.into_iter().map(|v| v.into()).collect();
        Value::Complex(ComplexValue::Array(values))
    }
}

impl From<Box<ImprintRecord>> for Value {
    fn from(r: Box<ImprintRecord>) -> Self {
        Value::Complex(ComplexValue::Row(r))
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
    pub(crate) header: Header,
    pub(crate) directory: Vec<DirectoryEntry>,
    pub(crate) payload: Bytes,
}

impl ImprintRecord {
    /// Get a value by field ID, deserializing it on demand
    pub fn get_value(&self, field_id: u32) -> Result<Option<Value>, ImprintError> {
        match self.directory.binary_search_by_key(&field_id, |e| e.id) {
            Ok(idx) => {
                let entry = &self.directory[idx];
                let value_bytes = self.payload.slice(entry.offset as usize..);
                let (value, _) = Value::read(entry.type_code, value_bytes)?;
                Ok(Some(value))
            }
            Err(_) => Ok(None),
        }
    }

    /// Get the raw bytes for a field without deserializing
    pub fn get_raw_bytes(&self, field_id: u32) -> Option<Bytes> {
        let idx = self
            .directory
            .binary_search_by_key(&field_id, |e| e.id)
            .ok()?;
        let entry = &self.directory[idx];
        let start = entry.offset as usize;
        let next_offset = self.directory[idx + 1..]
            .first()
            .map(|e| e.offset as usize)
            .unwrap_or(self.payload.len());
        Some(self.payload.slice(start..next_offset))
    }
}
