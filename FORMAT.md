# Imprint Binary Format Specification

This document describes the binary format used by Imprint for serialization and deserialization, with ASCII art diagrams to illustrate the byte layout.

## Record Structure Overview

```text
+--------+-------------------------+---------+
| Header | Field Directory (opt.)  | Payload |
+--------+-------------------------+---------+
```

## Header Format (15 bytes)

```text
Byte:  0       1       2       3-6             7-10            11-14
     +-------+-------+-------+----------------+----------------+----------------+
     | Magic | Ver.  | Flags | Fieldspace ID  | Schema Hash    | Payload Size   |
     | 'I'   | 0x01  | 0x??  | (LE u32)       | (LE u32)       | (LE u32)       |
     +-------+-------+-------+----------------+----------------+----------------+
```

Flags:
- `0x01`: Field directory is present

## Field Directory

```text
     +---------------------+---------------------+---------------------+
     | Count (varint)      | Entry 1             | Entry 2             | ...
     | (1-5 bytes)         | (9 bytes)           | (9 bytes)           |
     +---------------------+---------------------+---------------------+
```

Each directory entry (9 bytes):

```text
     +----------------+-------+----------------+
     | Field ID       | Type  | Field Offset   |
     | (LE u32)       | Code  | (LE u32)       |
     +----------------+-------+----------------+
      Bytes 0-3        Byte 4  Bytes 5-8
```

## Type Codes

| Type Code | Value | Description |
|-----------|-------|-------------|
| `0x0`     | Null  | No data     |
| `0x1`     | Bool  | Boolean     |
| `0x2`     | Int32 | 32-bit signed integer |
| `0x3`     | Int64 | 64-bit signed integer |
| `0x4`     | Float32 | 32-bit floating point |
| `0x5`     | Float64 | 64-bit floating point |
| `0x6`     | Bytes | Byte array |
| `0x7`     | String | UTF-8 encoded string |
| `0x8`     | Array | Array of values |
| `0x9`     | Map | Key-value mapping |
| `0xA`     | Row | Nested Imprint record |
| `0xB-0xFF` | Reserved | Future types |

## Type Serialization Formats

### Fixed-Width Types

#### Null (`0x0`)
```text
(No bytes)
```

#### Bool (`0x1`)
```text
Byte:  0
     +-------+
     | Value |
     | 0/1   |
     +-------+
```

#### Int32 (`0x2`)
```text
Byte:  0       1       2       3
     +-------+-------+-------+-------+
     | LSB                     MSB   |
     | (Little-endian i32)           |
     +-------+-------+-------+-------+
```

#### Int64 (`0x3`)
```text
Byte:  0       1       2       3       4       5       6       7
     +-------+-------+-------+-------+-------+-------+-------+-------+
     | LSB                                                     MSB   |
     | (Little-endian i64)                                           |
     +-------+-------+-------+-------+-------+-------+-------+-------+
```

#### Float32 (`0x4`)
```text
Byte:  0       1       2       3
     +-------+-------+-------+-------+
     | (IEEE 754 32-bit float)       |
     | (Little-endian)               |
     +-------+-------+-------+-------+
```

#### Float64 (`0x5`)
```text
Byte:  0       1       2       3       4       5       6       7
     +-------+-------+-------+-------+-------+-------+-------+-------+
     | (IEEE 754 64-bit float)                                       |
     | (Little-endian)                                               |
     +-------+-------+-------+-------+-------+-------+-------+-------+
```

### Variable-Width Types

#### Bytes (`0x6`)
```text
     +---------------------+---------------------------------------+
     | Length (varint)     | Raw Bytes Content                     |
     | (1-5 bytes)         | (Length bytes)                        |
     +---------------------+---------------------------------------+
```

#### String (`0x7`)
```text
     +---------------------+---------------------------------------+
     | Length (varint)     | UTF-8 Encoded String                  |
     | (1-5 bytes)         | (Length bytes)                        |
     +---------------------+---------------------------------------+
```

#### Array (`0x8`)
```text
     +---------------------+-------+--------------------------------+
     | Length (varint)     | Elem. | Element 1 | Element 2 | ...    |
     | (1-5 bytes)         | Type  | (format depends on type)       |
     +---------------------+-------+--------------------------------+
                            ^
                            Only present if Length > 0
```

#### Map (`0x9`)
```text
     +---------------------+-------+-------+------------------------+
     | Length (varint)     | Key   | Value | Key 1   | Value 1 |... |
     | (1-5 bytes)         | Type  | Type  | (format depends on respective types) |
     +---------------------+-------+-------+------------------------+
                            ^       ^       ^
                            Only present if Length > 0
```

Valid map key types:
- Int32 (`0x2`)
- Int64 (`0x3`)
- Bytes (`0x6`)
- String (`0x7`)

#### Row (`0xA`)
```text
     +----------------+-------------------------+---------------+
     | Header (15B)   | Field Directory (opt.)  | Payload       |
     +----------------+-------------------------+---------------+
     (Complete Imprint record - recursive structure)
```

## Varint Encoding

```text
MSB: Most Significant Bit - indicates if more bytes follow:
  1 = more bytes follow
  0 = this is the last byte

Single-byte example (1-127):
Byte:  0
     +----------------+
     |0|   7 bits     |
     +----------------+
      ^ MSB = 0 (end)

Multi-byte example:
Byte:  0             1             2  
     +----------------+----------------+----------------+
     |1|   7 bits     |1|   7 bits     |0|   7 bits     |
     +----------------+----------------+----------------+
      ^ MSB = 1         ^ MSB = 1         ^ MSB = 0
      more bytes        more bytes        last byte
```

Examples:
- 1: `00000001` (`0x01`)
- 127: `01111111` (`0x7F`)
- 128: `10000000 00000001` (`0x80 0x01`)
- 16,383: `11111111 01111111` (`0xFF 0x7F`)
- 16,384: `10000000 10000000 00000001` (`0x80 0x80 0x01`)

Also see [LEB128 encoding](https://en.wikipedia.org/wiki/LEB128) for more details.

## Complete Record Example

```text
+---------------------------------------------------------------------------+
| HEADER:                                                                   |
|  0      1      2      3-6           7-10          11-14                   |
| +------+------+------+-------------+-------------+-------------+          |
| | 0x49 | 0x01 | 0x01 | Fieldspace  | Schema Hash | Payload     |          |
| | 'I'  | Ver. | Flg. | ID          | (LE u32)    | Size        |          |
| +------+------+------+-------------+-------------+-------------+          |
|                                                                           |
| FIELD DIRECTORY (present because Flag 0x01 is set):                       |
| +-----------------+----------------------------------------+              |
| | Count (varint)  | Directory Entries (Count × 9 bytes)    |              |
| +-----------------+----------------------------------------+              |
|                                                                           |
| PAYLOAD (contains encoded field values):                                  |
| +----------------------------------------------------------------+        |
| | Field Value 1 | Field Value 2 | ... (format depends on types)  |        |
| +----------------------------------------------------------------+        |
+---------------------------------------------------------------------------+
```

## Field Access

Field values can be accessed in two ways:
1. By deserialization on demand (`get_value`)
2. As raw bytes without deserialization (`get_raw_bytes`)

Fields are located using binary search on field IDs in the directory.
