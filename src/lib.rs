mod error;
mod ops;
mod serde;
mod types;
mod varint;
mod writer;

pub use error::ImprintError;
pub use ops::Project;
pub use serde::{Read, Write};
pub use types::{
    DirectoryEntry, Flags, Header, ImprintRecord, MAGIC, SchemaId, TypeCode, VERSION, Value,
};
pub use varint::{decode as decode_varint, encode as encode_varint};
pub use writer::ImprintWriter;

/// Result type for Imprint operations
pub type Result<T> = std::result::Result<T, error::ImprintError>;
