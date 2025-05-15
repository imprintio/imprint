mod error;
mod serde;
mod types;
mod varint;

pub use error::ImprintError;
pub use serde::{Read, Write};
pub use types::{
    DirectoryEntry, Flags, Header, ImprintRecord, MAGIC, SchemaId, TypeCode, VERSION, Value,
};
pub use varint::{decode as decode_varint, encode as encode_varint};

/// Result type for Imprint operations
pub type Result<T> = std::result::Result<T, error::ImprintError>;
