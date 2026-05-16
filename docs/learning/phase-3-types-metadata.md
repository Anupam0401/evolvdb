# Phase 3: Type System & Metadata (M6-M7)

**Goal**: Understand how EvolvDB represents typed data and manages table metadata.

**Prerequisites**: Phase 0-2 (Foundations + Storage Engine)

---

## Overview: From Bytes to Meaning

The storage engine deals with **raw bytes**. But databases understand **typed data**:
- Integer: 42
- String: "Alice"
- Boolean: true

This phase bridges the gap between physical bytes and logical data.

**Key components**:
```
Type System       → What types exist (INT, VARCHAR, etc.)
Schema            → What columns are in a table
Tuple             → A logical row with typed values
RowCodec          → Convert between Tuple and bytes
Catalog           → Persistent metadata about tables
Table             → High-level abstraction over HeapFile
```

---

## M6: Type System

**Module**: `evolvdb-types`

### Supported Types

```java
enum Type {
    INT,        // 4 bytes, 32-bit signed integer
    BIGINT,     // 8 bytes, 64-bit signed integer
    BOOLEAN,    // 1 byte (0 or 1)
    FLOAT,      // 4 bytes, IEEE-754 single precision
    VARCHAR,    // Variable-length string with max length
    STRING      // Variable-length string (unbounded in declaration)
}
```

### Type Properties

#### Fixed-Width Types

**INT** (4 bytes):
- Range: -2,147,483,648 to 2,147,483,647
- Java type: `int` or `Integer`
- Binary: Little-endian 4 bytes

**BIGINT** (8 bytes):
- Range: -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807
- Java type: `long` or `Long`
- Binary: Little-endian 8 bytes

**BOOLEAN** (1 byte):
- Values: `true` (1) or `false` (0)
- Java type: `boolean` or `Boolean`
- Binary: 1 byte (0 or 1)

**FLOAT** (4 bytes):
- IEEE-754 single precision
- Java type: `float` or `Float`
- Binary: Little-endian 4 bytes

#### Variable-Width Types

**VARCHAR(N)**:
- Max **character** length: N
- Max **byte** length: Depends on UTF-8 encoding
- Java type: `String`
- Binary: `[u16 byteLength][UTF-8 bytes]`

**STRING**:
- Unbounded in schema declaration
- Same binary representation as VARCHAR
- Max byte length: 65,535 (u16 limit)

**Why both VARCHAR and STRING?**
- VARCHAR: SQL standard with explicit length
- STRING: Convenience type (common in modern systems)

### ColumnMeta

A **column definition**:

```java
record ColumnMeta(
    String name,
    Type type,
    Integer length  // Only for VARCHAR, null otherwise
) {}
```

**Examples**:
```java
new ColumnMeta("id", Type.INT, null)
new ColumnMeta("name", Type.VARCHAR, 50)
new ColumnMeta("age", Type.INT, null)
new ColumnMeta("active", Type.BOOLEAN, null)
```

**Constraints**:
- Name must be non-empty
- VARCHAR requires non-null, positive length
- Other types must have null length

### Schema

A **list of columns** with uniqueness constraints:

```java
class Schema {
    List<ColumnMeta> columns;
    
    Schema(List<ColumnMeta> columns) {
        // Validate: no duplicate names (case-insensitive)
        // Validate: VARCHAR has length, others don't
    }
    
    ColumnMeta getColumn(String name);  // Case-insensitive
    ColumnMeta getColumn(int index);
    int indexOf(String name);
    int columnCount();
}
```

**Key properties**:
- Column names are **case-insensitive** ("ID" == "id" == "Id")
- Column order matters (affects binary encoding)
- Immutable after creation

**Example**:
```java
Schema schema = new Schema(List.of(
    new ColumnMeta("id", Type.INT, null),
    new ColumnMeta("name", Type.VARCHAR, 50),
    new ColumnMeta("age", Type.INT, null)
));
```

This defines a table structure like:
```sql
CREATE TABLE example (
    id INT,
    name VARCHAR(50),
    age INT
)
```

---

## M7: Tuple - Logical Rows

**Module**: `evolvdb-types`

### What Is a Tuple?

A **tuple** is an immutable row with typed values bound to a schema.

```java
class Tuple {
    Schema schema;
    List<Object> values;
    
    Tuple(Schema schema, List<Object> values) {
        // Validate: values.size() == schema.columnCount()
        // Validate: each value matches column type
        // Validate: VARCHAR values don't exceed max length
    }
    
    Object get(int index);
    Object get(String columnName);
    Schema getSchema();
}
```

### Type Validation

On construction, Tuple validates each value:

**INT**:
- Java type must be `Integer`
- Example: `42`, `-10`

**BIGINT**:
- Java type must be `Long`
- Example: `1000000000000L`

**BOOLEAN**:
- Java type must be `Boolean`
- Example: `true`, `false`

**FLOAT**:
- Java type must be `Float`
- Example: `3.14f`, `-2.5f`

**VARCHAR(N)** or **STRING**:
- Java type must be `String`
- For VARCHAR: character length ≤ N
- Byte length (UTF-8) ≤ 65,535

**NULL values**: Not supported in M1-M11 (planned for M13)

### Example Usage

```java
Schema schema = new Schema(List.of(
    new ColumnMeta("id", Type.INT, null),
    new ColumnMeta("name", Type.VARCHAR, 50),
    new ColumnMeta("age", Type.INT, null)
));

Tuple tuple = new Tuple(schema, List.of(
    1,           // id
    "Alice",     // name
    30           // age
));

int id = (Integer) tuple.get("id");
String name = (String) tuple.get("name");
```

**Why immutable?**
- Thread safety
- Simpler reasoning (no unexpected mutations)
- Functional programming style

---

## RowCodec: Binary Encoding

**Module**: `evolvdb-types`  
**Class**: `io.github.anupam.evolvdb.types.RowCodec`

### Purpose

Convert between:
- **Logical**: `Tuple` (Java objects)
- **Physical**: `byte[]` (binary for storage)

### Binary Format

**Fixed-width types first** (predictable offsets), **variable-width types last**:

```
[INT fields...][BIGINT fields...][BOOLEAN fields...][FLOAT fields...]
[VARCHAR/STRING fields...]
```

**Each variable-width field**:
```
[u16 byteLength][UTF-8 bytes]
```

**Endianness**: Little-endian (x86 standard)

### Example Encoding

Schema: `(id INT, name VARCHAR(50), age INT)`
Tuple: `(1, "Alice", 30)`

**Binary layout**:
```
Bytes 0-3:   00 00 00 01           (id = 1, little-endian)
Bytes 4-7:   00 00 00 1E           (age = 30)
Bytes 8-9:   00 05                 (name byte length = 5)
Bytes 10-14: 41 6C 69 63 65        ("Alice" in UTF-8)
```

**Total**: 15 bytes

### API

```java
class RowCodec {
    static byte[] encode(Schema schema, Tuple tuple);
    static Tuple decode(Schema schema, byte[] bytes);
}
```

**Constraints**:
- Tuple schema must match encoding schema exactly
- Decoding with wrong schema = garbage data (no validation possible)

### Why This Encoding?

**Advantages**:
1. ✅ Compact (no field names, no delimiters)
2. ✅ Fast decoding (fixed-width fields have predictable offsets)
3. ✅ Type-safe (schema enforces structure)

**Disadvantages**:
1. ❌ Schema-dependent (can't decode without schema)
2. ❌ No forward/backward compatibility (schema change = re-encode all data)
3. ❌ No NULL support (yet)

**Comparison to alternatives**:

| Format | Size | Speed | Flexibility |
|--------|------|-------|-------------|
| EvolvDB | Compact | Fast | Low |
| JSON | Large | Slow | High |
| Protobuf | Compact | Fast | Medium |
| Avro | Compact | Fast | Medium |

EvolvDB prioritizes **simplicity and speed** over flexibility.

---

## M6: Catalog - Persistent Metadata

**Module**: `evolvdb-catalog`

### What Problem Does It Solve?

Tables persist across database restarts. We need to remember:
- Table names
- Table schemas
- Where table data is stored (FileId)

**Catalog** is the "database of databases" - metadata about your data.

### TableId

```java
record TableId(long id) {}
```

A **unique identifier** for each table. Auto-incremented on creation.

### TableMeta

**Metadata for one table**:

```java
record TableMeta(
    TableId tableId,
    String name,
    Schema schema,
    FileId fileId
) {}
```

**Example**:
```java
new TableMeta(
    new TableId(1),
    "users",
    schema,
    new FileId("users")
)
```

**Interpretation**: Table "users" with ID=1, stored in file "users.evolv"

### CatalogManager

**Class**: `io.github.anupam.evolvdb.catalog.CatalogManager`

**Persistent storage**: Uses a special HeapFile named `__catalog__`

**Operations**:
```java
class CatalogManager {
    TableId createTable(String name, Schema schema);
    Optional<TableMeta> getTable(String name);
    Optional<TableMeta> getTable(TableId id);
    void dropTable(TableId id);
    List<TableMeta> listTables();
}
```

### Persistence Strategy: Append-Only Log

The catalog file is an **append-only log** of operations:

```
[UPSERT users]
[UPSERT orders]
[DROP users]
[UPSERT users]   ← Recreated
```

**On startup**: Scan log, apply operations to build in-memory map.

**Why append-only?**
- Simple (no complex updates)
- Durable (just append, no risk of corruption)
- Sequential writes (fast)

### TableMetaCodec: Binary Format

**Version 1 format**:

**UPSERT operation**:
```
[u16 version=1][u8 kind=1][u64 tableId]
[u16 nameLen][name bytes]
[u16 columnCount]
For each column:
    [u16 colNameLen][colName bytes]
    [u8 typeOrdinal]
    [i32 varcharLen or -1]
[u16 fileNameLen][fileName bytes]
```

**DROP operation**:
```
[u16 version=1][u8 kind=2][u64 tableId]
```

**Why versioned?**
- Future changes to format (version 2, 3, ...)
- Old versions can be decoded
- Forward compatibility

### Lifecycle

**Startup**:
```
1. Open HeapFile(__catalog__)
2. Scan all records
3. For each record:
   - Decode operation
   - If UPSERT: Add/update table in maps
   - If DROP: Remove table from maps
4. Ready to serve queries
```

**Create table**:
```
1. Validate: name not taken
2. Allocate new TableId
3. Encode UPSERT operation
4. Append to catalog HeapFile
5. Update in-memory maps
6. Return TableId
```

**Drop table**:
```
1. Validate: table exists
2. Encode DROP operation
3. Append to catalog HeapFile
4. Remove from in-memory maps
5. Optionally: delete table's data file
```

### Catalog as System Table

The catalog is itself stored as a HeapFile:
- Uses same storage engine (DiskManager, BufferPool, SlottedPageFormat)
- Subject to same crashes, recovery issues
- Future: Replicate catalog or use WAL for durability

---

## Table Abstraction

**Module**: `evolvdb-catalog`  
**Class**: `io.github.anupam.evolvdb.catalog.Table`

### Purpose

Wrap HeapFile with **type-aware APIs**:

```java
class Table {
    TableMeta meta;
    HeapFile heapFile;
    
    RecordId insert(Tuple tuple);
    Tuple read(RecordId rid);
    RecordId update(RecordId rid, Tuple tuple);
    void delete(RecordId rid);
    Iterable<Tuple> scanTuples();
}
```

**Key difference from HeapFile**:
- HeapFile: `insert(byte[])`
- Table: `insert(Tuple)`

Table handles encoding/decoding via RowCodec.

### Insert Flow

```java
RecordId Table.insert(Tuple tuple) {
    byte[] bytes = RowCodec.encode(meta.schema(), tuple);
    return heapFile.insert(bytes);
}
```

### Read Flow

```java
Tuple Table.read(RecordId rid) {
    byte[] bytes = heapFile.read(rid);
    return RowCodec.decode(meta.schema(), bytes);
}
```

### Scan Flow

```java
Iterable<Tuple> Table.scanTuples() {
    return StreamSupport.stream(heapFile.scan().spliterator(), false)
        .map(bytes -> RowCodec.decode(meta.schema(), bytes))
        .collect(Collectors.toList());
}
```

### Update Flow

```java
RecordId Table.update(RecordId rid, Tuple newTuple) {
    byte[] newBytes = RowCodec.encode(meta.schema(), newTuple);
    return heapFile.update(rid, newBytes);
}
```

**Remember**: RecordId may change if record relocates.

---

## Complete Example: Creating and Using a Table

```java
// 1. Create database
DbConfig config = new DbConfig(Paths.get("/data"), 4096, 100);
DiskManager disk = new NioDiskManager(config);
BufferPool buffer = new DefaultBufferPool(disk, 100, new LruEvictionPolicy());
CatalogManager catalog = new CatalogManager(disk, buffer, new SlottedPageFormat());

// 2. Define schema
Schema schema = new Schema(List.of(
    new ColumnMeta("id", Type.INT, null),
    new ColumnMeta("name", Type.VARCHAR, 50),
    new ColumnMeta("age", Type.INT, null)
));

// 3. Create table
TableId tableId = catalog.createTable("users", schema);

// 4. Open table
Table table = catalog.openTable("users");

// 5. Insert data
Tuple alice = new Tuple(schema, List.of(1, "Alice", 30));
Tuple bob = new Tuple(schema, List.of(2, "Bob", 25));

RecordId rid1 = table.insert(alice);
RecordId rid2 = table.insert(bob);

// 6. Read data
Tuple retrieved = table.read(rid1);
System.out.println(retrieved.get("name"));  // "Alice"

// 7. Scan all
for (Tuple tuple : table.scanTuples()) {
    System.out.println(tuple.get("name"));
}

// 8. Update
Tuple alice31 = new Tuple(schema, List.of(1, "Alice", 31));
RecordId newRid = table.update(rid1, alice31);

// 9. Delete
table.delete(rid2);

// 10. Close
catalog.close();
buffer.flushAll();
disk.close();
```

---

## Data Flow: Logical to Physical

Let's trace an insert end-to-end:

```
Client code
    ↓
Table.insert(tuple)
    ↓
RowCodec.encode(schema, tuple) → bytes
    ↓
HeapFile.insert(bytes)
    ↓
For each page:
    BufferPool.getPage(pageId)
        ↓
    (If miss: DiskManager.readPage)
        ↓
    PageFormat.freeSpace(page)
        ↓
    If space: PageFormat.insert(page, bytes)
        ↓
    BufferPool.unpin(dirty=true)
        ↓
    Return RecordId
```

**Reverse direction (read)**:

```
RecordId (e.g., PageId=5, slot=2)
    ↓
Table.read(rid)
    ↓
HeapFile.read(rid)
    ↓
BufferPool.getPage(PageId=5)
    ↓
PageFormat.read(page, slot=2) → bytes
    ↓
BufferPool.unpin()
    ↓
RowCodec.decode(schema, bytes) → tuple
    ↓
Return to client
```

---

## Why This Layer Matters

### Abstraction Benefits

Without this layer, every query would need to:
1. Know binary encoding format
2. Manually decode bytes to types
3. Track table schemas manually
4. Handle schema mismatches

**With this layer**:
- Queries work with `Tuple` objects (type-safe)
- Schema is enforced automatically
- Catalog handles metadata persistence
- Clean separation: storage vs. logical data

### Extensibility

**Type system is pluggable**:
- Add new types (DATE, TIMESTAMP, DECIMAL)
- Schema evolution (add/drop columns)
- Alternative encoding (columnar, compressed)

**Catalog is versioned**:
- Version 2 codec can support new features
- Old data still readable

---

## Key Takeaways

### Type System

- **Fixed-width** types: INT, BIGINT, BOOLEAN, FLOAT
- **Variable-width** types: VARCHAR, STRING
- Schema enforces structure
- Tuple provides type-safe row abstraction

### Encoding

- RowCodec converts Tuple ↔ bytes
- Little-endian, compact binary format
- Fixed-width fields first (fast access)
- Variable-width fields last (sequential read)

### Catalog

- Persistent metadata in `__catalog__` HeapFile
- Append-only log (UPSERT/DROP operations)
- Versioned encoding (future-proof)
- In-memory maps for fast lookups

### Table

- High-level abstraction over HeapFile
- Automatic encoding/decoding
- Type-safe APIs (insert/read/update/delete/scan)

---

## Self-Test Questions

1. **Why does VARCHAR need a length parameter but STRING doesn't?**
   - VARCHAR is SQL standard with explicit limit; STRING is convenience type

2. **What happens if you decode bytes with the wrong schema?**
   - Garbage data (no way to detect mismatch)

3. **Why is Tuple immutable?**
   - Thread safety, simpler reasoning

4. **How does catalog survive database restart?**
   - Stored in HeapFile, scanned on startup

5. **Why append-only log instead of updating catalog entries in place?**
   - Simpler, safer (no risk of partial writes), fast sequential writes

6. **What is the maximum size of a VARCHAR field in bytes?**
   - 65,535 (u16 length prefix limit)

---

## Next Steps

You now understand how EvolvDB represents typed data and manages metadata. In **Phase 4**, we'll explore the SQL frontend:
- Tokenizer and lexer
- Parser (recursive descent)
- Abstract Syntax Tree (AST)
- SQL validation

**Continue to**: [`phase-4-sql-frontend.md`](./phase-4-sql-frontend.md)
