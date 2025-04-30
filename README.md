# Imprint: Serialization Format for Incremental Data Manipulation

Imprint is a binary row serialization format built for stream processing
workloads, particularly those involving **incremental joins** and
**denormalization** across heterogeneous data sources. It combines the
flexibility of schemaless formats like JSON with the safety and performance of
schema-aware formats like Avro or Protobuf.

Imprint is optimized for a common pattern in stream processing: data is
serialized once and deserialized many times. This format prioritizes
(de)serialization speed and schema composability, even if that means accepting
larger record sizes.

## Core Principles & Motivation

Imprint as a serialization format is opinionated about its core principles. Specifically,
the goal is to allow efficient row-level data manipulation and easy debugging:

| Feature                   | Description                                                                   |
|---------------------------|-------------------------------------------------------------------------------|
| **Message Composition**   | Combining records with different schemas can be done without reserialization  |
| **Message Decomposition** | Projecting a subset of fields can be done without full deserialization        |
| **Field Addressable**     | Each field can be deserialized without deserializing the entire record        |
| **Schemaless Reads**      | Messages can be read without access to the schema that wrote the record       |

Existing formats are typically either optimized for RPCs (JSON/Protobuf) or
efficient in-memory representation (Flatbuffer). AVRO's reader/writer schema
support is perhaps the best example of a row-oriented format designed for data
manipulation and evolution but it is extremely inefficient in its use for the
most common streaming operations.

### Comparison with Existing Formats

See the table below more detailed comparison of existing formats:

| Feature                       | Imprint | JSON | Avro | Protobuf | Flatbuffer |
| ----------------------------- | ------- | ---- | ---- | -------- | ---------- |
| Message Composition           | ✅      | ⚠️    |❌    | ❌        | ❌         |
| Message Decomposition         | ✅      | ❌    |❌    | ❌        | ✅         |
| Field Addressable             | ✅      | ❌    |❌    | ❌        | ✅         |
| Compact Binary Format         | ⚠️      | ❌    |✅    | ✅        | ✅         |
| Native Schema Evolution       | ✅      | ⚠️    |✅    | ✅        | ❌         |
| Schema-less Reads             | ⚠️      | ✅    |❌    | ❌        | ❌         |

Digging deeper into AVRO and Protobuf, which are the existing dominators in the
stream processing space, this table explains a bit more behind why the limitations
of each system is as it is:

| Capability | Avro | Protobuf | Imprint |
|------------|------|-----------|----------|
| Random field access | Sequential scan (O(record size)) | Tag stream scan (O(record size)) | Offset directory → O(log N) lookup |
| Compose / merge rows without re‑encoding | ❌ (must re‑serialize with merged schema) | ❌ (decode + re‑encode) | ✅ pointer‑math on directories & value areas |
| Self‑contained row (no external schema) | ⚠️ Container files embed writer schema; single rows often rely on registry | ⚠️ Code‑generated classes needed | ✅ type‑code + length in directory ⇒ schema‑less reads |
| Field order stability | Fixed at write‑time | Any order / repeats | Canonical sort by field_id ⇒ deterministic bytes |
| On‑wire overhead | Smallest (no directory) | Very small (1 VarInt tag/field) | Slightly larger (directory ≈ 3‑5 bytes/field) |

Take‑away: Imprint pays a few bytes per field to unlock deserialization-free joins,
per‑field projection, and schema‑less tooling—capabilities that matter most
in realtiem data manipulation topologies where each record may be routed,
filtered, or merged dozens of times.

## Binary Format Structure

The Imprint row is a self‑describing binary blob.  Every row carries the
minimum metadata required to (de)serialize any individual field without
interpreting the rest of the record.  The layout is deliberately simple so that
it can be manipulated with constant‑time pointer arithmetic for projection and
composition operations.

```
+--------------------------------------------+
| Magic | Version | Flags | Schema Hash / ID |
+--------------------------------------------+
|        VarInt: Field Count (N)             |
+--------------------------------------------+
|  N x DIRECTORY ENTRY (sorted by field id)  |
+--------------------------------------------+
|                 PAYLOAD                    |
+--------------------------------------------+
```

### Header Format

| Offset | Size | Field   | Notes                                               |
|--------|------|---------|-----------------------------------------------------|
| 0      | 1    | Magic   | ASCII `0x49` ("I") to guard against misparsing      |
| 1      | 1    | Version | Currently `0x01`. Allows for wire-format evolution  |
| 2      | 1    | Flags   | See below                                           |
| 3      | 8    | Schema  | 32-bit fieldspace id + 32-bit schema hash           |

The flags are a reserved bitset that indicate how to deserialize the rest of the
record:

| Bit | Name                   | Meaning                              |
|-----|------------------------|--------------------------------------|
| 0   | `FLAG_FIELD_DIRECTORY` | Whether a field directory is present |
| 1-7 | _reserved_             | Must be `0` in v1                    |

Schemas in Imprint have two components: 

1. a fieldspace that contains all the possible fields for schemas within a
  fieldspace, and
2. the schema itself, which represents which fields in the fieldspace are
  present in the record itself

The Field Directory (see below) is a binary format definition of a schema,
and is typically cached (keyed by the schema id) to avoid deserializing it
for repeated reads of records that contain the same fields.

### Field Directory

The field directory contains `N` directory entries that describe a single field
in an Imprint record. The field directory is sorted by the field id, which is a 
uniquely assigned integer unique within a fieldspace (best practices for designing
Imprint schemas is discussed below). Sorting by `field_id` gives deterministic
serialisations—identical logical rows produce byte‑for‑byte equal blobs, which
makes hashing and deduplication cheap.

Each entry has the following format:

```
+------------------------------+
| id | type | offset | length? |
+------------------------------+
```

| Field    | Encoding | Description                                        |
|----------|----------|----------------------------------------------------|
| `id`     | VarInt   | Uniquely assigned identifier within a fieldspace   |
| `type`   | `u8`     | Field type identifier, see below                   | 
| `offset` | `u32`    | Byte position of the value relative to the payload |
| `length` | VarInt   | Length, only for variable-width types              |

### Payload Encoding

| `type_code` | Type       | Encoding details                     |
| ----------: | ---------- | ------------------------------------ |
|         0x0 | `null`     | No payload; `length` = 0             |
|         0x1 | `bool`     | 1 byte `0x00` / `0x01`               |
|         0x2 | `int32`    | 4-byte signed int32                  |
|         0x3 | `int64`    | 8-byte signed int64                  |
|         0x4 | `float32`  | IEEE‑754 little‑endian bytes         |
|         0x5 | `float64`  | IEEE‑754 little‑endian bytes         |
|         0x6 | `bytes`    | `length` + payload                   |
|         0x7 | `string`   | UTF‑8, `length` + payload            |
|         0x8 | `array`    | `type_code` + `size` + payload       |
|         0x9 | `row`      | Nested Imprint row (recursive joins) |
|      10–127 | *reserved* | Future primitives / logical types    |

## Algorithms for Various Data Operations

To help illustrate the impact of Imprint's data format on common incremental,
row-oriented use cases we outline a few of the algorithms used for the most
common set of operations.

### Projection (Field Subset)

Projection can be done without deserializing any of the payload. After
parsing the header and the field directory, the byte slices within the
payload can be directly referenced and appended to a new buffer.

```
header = parse header
fields = parse field directory

new_schema = []
new_payload = []
for field in fields:
  if field is in projection:
    new_schema.append(field)
    new_payload.append(payload.bytes[field.offset:field.offset + field.length])
```

### Composition (Join / Merge)

Merging two Imprint rows with compatible schemas (there are no fields
with the same name and different types) can be done by concatenating 
the field directories and the payloads, modifying the directory for
trailing messages by incrementing the offset by the length of the 
payload of A:

```
new_payload = A.payload || B.payload
new_directory = A.dir ∪ (B.dir offset+|A|)
new_fieldcount = A.N + B.N
```

If the field directories are not disjoint, the directory will only include
the directory entry for the first field. This means the order of composition 
matters as the payload in the second record will be ignored (or, optionally,
the second payload can be modified to remove the discarded value to save 
space).