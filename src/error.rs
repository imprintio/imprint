use thiserror::Error;

#[derive(Error, Debug)]
pub enum ImprintError {
    #[error("invalid magic byte: expected 0x49, got {0:#x}")]
    InvalidMagic(u8),

    #[error("unsupported version: {0}")]
    UnsupportedVersion(u8),

    #[error("invalid field type: {0}")]
    InvalidFieldType(u8),

    #[error("invalid varint encoding")]
    InvalidVarInt,

    #[error("field not found: {0}")]
    FieldNotFound(u32),

    #[error("invalid utf8 in string field")]
    InvalidUtf8String,

    #[error("buffer underflow: needed {needed} bytes, had {available}")]
    BufferUnderflow { needed: usize, available: usize },

    #[error("schema error: {0}")]
    SchemaError(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}
