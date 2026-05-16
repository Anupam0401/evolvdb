# Phase 0-1: Foundations & Mental Model

**Goal**: Build intuition about what databases do and where EvolvDB fits. No prior database knowledge required.

---

## Phase 0: Orientation

### What Problem Does a Database Solve?

Imagine you're building an application that needs to store user data. You could:

1. **Just use files**: Write data to `users.txt`
   - ❌ Problem: How do you find a specific user quickly?
   - ❌ Problem: What if two processes write simultaneously?
   - ❌ Problem: What if power fails mid-write?

2. **Use variables in memory**
   - ❌ Problem: All data lost when program stops
   - ❌ Problem: Limited by RAM

**A database solves these problems**:
- ✅ **Persistent storage** that survives crashes
- ✅ **Fast lookups** via indexing (not yet in EvolvDB, but designed for it)
- ✅ **Concurrent access** control (planned for EvolvDB)
- ✅ **Query language** (SQL) to ask questions about data
- ✅ **Crash recovery** (planned for EvolvDB via WAL)

### What Does "Building a Database from Scratch" Mean?

Most applications use databases like PostgreSQL or MySQL. EvolvDB **reimplements** the internals of such systems without using external database libraries.

**EvolvDB implements**:
- Physical storage management (pages, files)
- Memory caching (buffer pool)
- Data structures (heap files, slotted pages)
- SQL parsing and execution
- Query optimization

**EvolvDB does NOT implement** (yet):
- Indexes (B+Tree planned)
- Transactions (ACID planned)
- Network protocol (no client-server)
- Advanced SQL features (window functions, CTEs)

### Where Does EvolvDB Fit?

**Comparison Matrix**:

| Feature | PostgreSQL | EvolvDB (M1-M11) | Toy Example |
|---------|-----------|------------------|-------------|
| Storage Engine | ✅ Production | ✅ Complete | ❌ |
| SQL Parsing | ✅ Full ANSI | ✅ Subset | ❌ |
| Query Optimizer | ✅ Advanced | ✅ Volcano-style | ❌ |
| Indexes | ✅ B+Tree, Hash, GiST | ❌ (M16 planned) | ❌ |
| Transactions | ✅ MVCC | ❌ (M17 planned) | ❌ |
| WAL/Recovery | ✅ ARIES-based | ❌ (M18 planned) | ❌ |
| Network Protocol | ✅ Wire protocol | ❌ | ❌ |

**EvolvDB is**: An educational database with production-quality architecture for the features it implements.

---

## Phase 1: Absolute Foundations

### First Principles: What Is Data?

**Data** is just information you want to remember and retrieve later.

Examples:
- User records: `{id: 1, name: "Alice", age: 30}`
- Order records: `{orderId: 42, userId: 1, amount: 99.99}`

### What Is Persistence?

**Persistence** means data survives after the program ends.

**Why files alone aren't enough**:
```
users.txt:
1,Alice,30
2,Bob,25
3,Charlie,35
```

Problems:
1. **Finding data is slow**: Need to scan entire file to find user #2
2. **Updates are awkward**: Can't easily change Bob's age without rewriting file
3. **No structure**: What if name contains commas?
4. **Concurrent access**: Two processes could corrupt the file
5. **Crashes**: Partial write = corrupted data

**What databases add**:
- **Fixed-size pages**: Split file into uniform chunks (e.g., 4KB pages)
- **Structured layout**: Binary format with metadata
- **Buffer caching**: Keep hot pages in memory
- **Write-ahead logging**: Log changes before applying (for crash recovery)

### Why Pages?

Think of a book:
- Pages are fixed-size units (easier to manage than variable-size chunks)
- You can jump to any page by number
- Operating systems read/write in blocks anyway

**EvolvDB uses 4KB pages** (configurable):
- Operating system page size is typically 4KB
- Good balance: not too small (overhead), not too large (waste)

### The Three-Layer Storage Model

Every database has roughly these layers:

```
┌─────────────────────────────────────┐
│   SQL / Query Processing            │  ← "What" you want
├─────────────────────────────────────┤
│   Logical Data Management           │  ← Tables, rows, types
├─────────────────────────────────────┤
│   Physical Storage                  │  ← "How" it's stored
└─────────────────────────────────────┘
```

**EvolvDB's layers**:

1. **Physical Storage** (M1-M5)
   - DiskManager: Read/write pages
   - BufferPool: Cache pages in memory
   - Page Format: Layout records within pages
   - HeapFile: Manage records across multiple pages

2. **Logical Management** (M6-M7)
   - Types: INT, VARCHAR, etc.
   - Schema: Define table structure
   - Catalog: Persistent metadata
   - Tuple: Logical row abstraction

3. **Query Processing** (M8-M11)
   - Parser: SQL → AST
   - Planner: AST → Logical Plan
   - Optimizer: Logical → Best Physical Plan
   - Executor: Physical Plan → Results

---

## Mental Model: The Big Picture Flow

Let's trace a simple query from start to finish:

```sql
SELECT name FROM users WHERE age > 25
```

**High-level journey**:

```
   SQL String
      ↓
   Parser (M8)
      ↓
   AST (Abstract Syntax Tree)
      ↓
   Validator (M8)
      ↓
   Binder (M9) ← uses Catalog
      ↓
   Logical Plan (M9)
      ↓
   Logical Rewriter (M11)
      ↓
   Volcano Optimizer (M11)
      ↓
   Physical Plan (M10-M11)
      ↓
   Execution Operators (M10)
      ↓
   Table/HeapFile (M4, M7)
      ↓
   BufferPool (M2)
      ↓
   DiskManager (M1)
      ↓
   Disk Files (.evolv)
```

**Key insight**: Each layer has a clear responsibility and doesn't know about layers below their immediate dependency.

---

## Why This Architecture?

### Separation of Concerns

**Bad design**: One giant class that parses SQL, finds data, and returns results.

**Good design** (EvolvDB):
- Parser only cares about syntax
- Planner only cares about logical operations
- Executor only cares about algorithm efficiency
- Storage only cares about bytes on disk

**Benefits**:
- **Testability**: Test each layer independently
- **Extensibility**: Add new operators without touching storage
- **Maintainability**: Clear module boundaries

### Abstraction Layers

Each layer presents a **simpler interface** to the layer above:

- **DiskManager**: "Give me page 42 of file 'users'"
- **BufferPool**: "Give me page 42, I'll cache it for you"
- **HeapFile**: "Give me all records in 'users'"
- **Table**: "Give me all tuples (logical rows) in 'users'"
- **Executor**: "Give me the next row matching my predicate"

---

## Core Abstractions in EvolvDB

### 1. Identifiers (Addressing Scheme)

```java
FileId(String name)                    // "users"
PageId(FileId file, int pageNo)        // File "users", page 5
RecordId(PageId page, short slot)      // Page 5, slot 3
```

**Why three levels?**
- **FileId**: Logical name (humans think in tables)
- **PageId**: Physical location (OS reads pages)
- **RecordId**: Record location (stable identifier even if record moves within page)

### 2. Page

A `Page` is a wrapper around a `ByteBuffer`:
```java
class Page {
    PageId id;
    ByteBuffer buffer;  // 4KB by default
}
```

**Why ByteBuffer?**
- Direct memory allocation (off-heap)
- Efficient I/O via NIO
- Manual layout control

### 3. Operators (Execution Model)

Every query operator follows this interface:
```java
interface PhysicalOperator {
    void open();              // Initialize
    Tuple next();            // Get next row (null = done)
    void close();            // Cleanup
    Schema schema();         // Output schema
}
```

**This is the Volcano Model** (pull-based iteration). More in Phase 6.

---

## What Makes EvolvDB Non-Trivial?

### Not a Toy

❌ **Toy database**:
- In-memory only
- No SQL parsing
- Single data structure (e.g., HashMap)
- No optimization

✅ **EvolvDB**:
- Persistent storage with proper page management
- Full SQL parser with AST
- Query optimizer with cost model
- Multiple join algorithms
- Clean separation of concerns

### Production-Quality Patterns

EvolvDB uses **real database techniques**:

1. **Slotted pages** (used in PostgreSQL, MySQL)
2. **Buffer pool with LRU** (standard caching)
3. **Volcano iterator model** (standard execution)
4. **Cost-based optimization** (Volcano-style memo)
5. **Versioned metadata codec** (forward compatibility)

### What's Intentionally Missing?

These are **planned**, not absent due to poor design:

- **Indexes**: M16 has full B+Tree design ready
- **Transactions**: M17 architecture planned (2PL or MVCC)
- **WAL**: M18 recovery design documented
- **NULL support**: M13 planned (three-valued logic)

The foundation is **designed to support these features** without major refactoring.

---

## How to Think About EvolvDB

### Metaphor: A Manufacturing Pipeline

```
Raw SQL       →   [Parser]    →   AST
AST           →   [Planner]   →   Logical Plan
Logical Plan  →   [Optimizer] →   Physical Plan
Physical Plan →   [Executor]  →   Results
```

Each stage:
- Takes input from previous stage
- Transforms it
- Passes to next stage
- Doesn't backtrack (mostly)

### Metaphor: Nested Russian Dolls

Each layer wraps the one below:

```
Table (logical rows)
  wraps HeapFile (binary records)
    wraps BufferPool (cached pages)
      wraps DiskManager (raw I/O)
        wraps FileSystem (OS)
```

Higher layers **don't know** how lower layers work, only their interface.

---

## Key Design Principles

### 1. Strategy Pattern

Multiple implementations of the same interface:
- `EvictionPolicy`: `LruEvictionPolicy`, future: `ClockEvictionPolicy`
- `PageFormat`: `SlottedPageFormat`, future: `FixedPageFormat`
- `PhysicalPlan`: Multiple join algorithms with same interface

### 2. Visitor Pattern

Traverse tree structures (AST, Logical Plans):
```java
interface LogicalPlanVisitor {
    void visit(LogicalScan node);
    void visit(LogicalFilter node);
    void visit(LogicalJoin node);
    // ...
}
```

### 3. Template Method

Fixed lifecycle, variable steps:
```java
class PhysicalOperator {
    final void execute() {
        open();
        while (Tuple t = next()) {
            // process
        }
        close();
    }
}
```

### 4. Dependency Inversion

High-level modules depend on abstractions, not concrete implementations:
- `HeapFile` depends on `PageFormat` interface, not `SlottedPageFormat`
- `BufferPool` depends on `DiskManager` interface, not `NioDiskManager`

---

## Module Map

EvolvDB is organized into Gradle modules:

### Storage Layer
- `evolvdb-storage-disk`: DiskManager (page I/O)
- `evolvdb-storage-buffer`: BufferPool (caching)
- `evolvdb-storage-page`: Page abstractions and formats
- `evolvdb-storage-record`: HeapFile (multi-page records)

### Type System
- `evolvdb-types`: Type, Schema, Tuple, RowCodec

### Metadata
- `evolvdb-catalog`: CatalogManager, TableMeta

### Query Processing
- `evolvdb-sql`: Parser, AST, Validator
- `evolvdb-planner`: Logical plans, Binder, rules
- `evolvdb-exec`: Physical operators, Executor, Optimizer

### Infrastructure
- `evolvdb-common`: Exceptions, utilities
- `evolvdb-config`: Configuration (page size, buffer pool size)
- `evolvdb-core`: Database facade (composition root)
- `evolvdb-cli`: Command-line interface

---

## Mental Checkpoint

Before moving to Phase 2, make sure you understand:

✅ **Why databases exist** (persistence + structure + performance)  
✅ **Pages are the unit of I/O** (fixed-size chunks)  
✅ **Layers of abstraction** (physical → logical → query processing)  
✅ **Big picture flow** (SQL → Parser → Planner → Optimizer → Executor → Storage)  
✅ **EvolvDB is non-trivial** (production patterns, extensible design)  
✅ **What's missing is intentional** (planned features, not architectural gaps)  

---

## Quick Self-Test

1. **Why can't we just use plain text files?**
   - Hint: Performance, structure, concurrent access, crash safety

2. **What are the three main layers of EvolvDB?**
   - Hint: Physical storage, logical management, query processing

3. **What is a PageId?**
   - Hint: FileId + page number

4. **What does the Optimizer do?**
   - Hint: Logical plan → Physical plan (choosing best algorithms)

5. **Why do we need a Catalog?**
   - Hint: Store metadata about tables (schemas, types)

---

## Next Steps

You now have the mental model. In **Phase 2**, we'll dive deep into the storage engine:
- How pages are read and written (DiskManager)
- How pages are cached (BufferPool)
- How records are laid out in pages (Slotted Page Format)
- How records span multiple pages (HeapFile)

**Continue to**: [`phase-2-storage-engine.md`](./phase-2-storage-engine.md)
