# Phase 9: High-Level Design (HLD)

**Goal**: Understand the system architecture, component boundaries, and data flow.

**Prerequisites**: Phase 0-8 (Complete understanding of all components)

---

## System Architecture Overview

EvolvDB is organized in **layers** with clear separation of concerns:

```
┌─────────────────────────────────────────────────────┐
│              SQL Frontend (M8)                       │
│  Parser → AST → Validator                           │
└─────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────┐
│           Query Planning (M9)                        │
│  Binder → Analyzer → Logical Planner                │
└─────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────┐
│         Query Optimization (M11)                     │
│  Logical Rewriter → Volcano Optimizer                │
└─────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────┐
│          Query Execution (M10)                       │
│  Physical Operators (Volcano Model)                  │
└─────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────┐
│         Catalog & Type System (M6-M7)               │
│  TableMeta → Schema → Tuple → Table                 │
└─────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────┐
│         Storage Engine (M1-M5)                       │
│  HeapFile → BufferPool → DiskManager                │
└─────────────────────────────────────────────────────┘
```

---

## Layer 1: Storage Foundation

### Responsibility
Manage physical storage of data on disk.

### Components

**DiskManager** (`evolvdb-storage-disk`)
- Allocate pages in files
- Read/write pages by PageId
- Sync durability

**BufferPool** (`evolvdb-storage-buffer`)
- Cache pages in memory
- LRU eviction policy
- Pin/unpin semantics
- Dirty page tracking

**PageFormat** (`evolvdb-storage-page`)
- Slotted page layout
- Variable-length records
- Tombstones for deletes
- Compaction on demand

**HeapFile** (`evolvdb-storage-record`)
- Multi-page record storage
- Insert/read/update/delete
- Sequential scan iterator

### Boundaries

**Knows about**: Pages, bytes, files, caching  
**Doesn't know about**: Types, schemas, SQL, queries  

**Provides to upper layers**: Byte-level record storage with RecordId addressing

---

## Layer 2: Type System & Metadata

### Responsibility
Bridge between bytes and typed data.

### Components

**Type System** (`evolvdb-types`)
- Type enum (INT, VARCHAR, etc.)
- ColumnMeta (name, type, length)
- Schema (list of columns)

**Tuple** (`evolvdb-types`)
- Immutable typed row
- Bound to schema
- Type validation

**RowCodec** (`evolvdb-types`)
- Binary encoding/decoding
- Little-endian format
- Fixed-width first, variable-width last

**Catalog** (`evolvdb-catalog`)
- CatalogManager (persistent metadata)
- TableMeta (id, name, schema, fileId)
- Append-only log with versioned codec

**Table** (`evolvdb-catalog`)
- High-level abstraction over HeapFile
- Tuple-oriented APIs
- Automatic encoding/decoding

### Boundaries

**Knows about**: Storage layer (HeapFile)  
**Doesn't know about**: SQL, queries, optimization  

**Provides to upper layers**: Typed table abstraction with schema enforcement

---

## Layer 3: SQL Frontend

### Responsibility
Parse SQL text into structured AST.

### Components

**Tokenizer** (`evolvdb-sql`)
- Lexical analysis
- Token stream with positions
- Keywords, operators, identifiers, literals

**Parser** (`evolvdb-sql`)
- Recursive descent parsing
- Build AST from tokens
- Operator precedence hierarchy

**AST Nodes** (`evolvdb-sql`)
- Statements (CREATE, INSERT, SELECT, etc.)
- Expressions (Binary, Comparison, Logical, etc.)
- Visitor pattern support

**Validator** (`evolvdb-sql`)
- Catalog-aware checks
- Table/column existence
- Type compatibility
- Early error detection

### Boundaries

**Knows about**: Catalog (for validation)  
**Doesn't know about**: Physical storage, execution  

**Provides to upper layers**: Validated AST ready for planning

---

## Layer 4: Query Planning

### Responsibility
Convert AST to logical query plan.

### Components

**Binder** (`evolvdb-planner`)
- Name resolution
- Table lookups in catalog
- Qualified column references
- Ambiguity detection

**Analyzer** (`evolvdb-planner`)
- Type inference
- Expression typing
- Type promotion rules
- Schema propagation

**Logical Plan Nodes** (`evolvdb-planner`)
- LogicalScan, LogicalFilter, LogicalProject
- LogicalJoin, LogicalAggregate, LogicalInsert
- Tree structure with schemas

**Rule Engine** (`evolvdb-planner`)
- Equivalence-preserving transformations
- Predicate simplification
- Projection/filter reordering
- Fixed-point iteration

### Boundaries

**Knows about**: Catalog, Type system  
**Doesn't know about**: Physical algorithms, execution  

**Provides to upper layers**: Logical plan describing "what" to compute

---

## Layer 5: Query Optimization

### Responsibility
Choose best physical execution plan.

### Components

**Logical Rewriter** (`evolvdb-exec/optimizer`)
- Predicate pushdown
- Projection pruning
- Join reordering
- Pre-optimization transformations

**Volcano Optimizer** (`evolvdb-exec/optimizer`)
- Bottom-up optimization
- Memo structure (simplified)
- Cost-based selection
- Physical rule application

**Cost Model** (`evolvdb-exec/optimizer`)
- Estimate (rows, CPU, I/O)
- Default statistics (no real stats in M11)
- Selectivity assumptions
- Algorithm cost formulas

**Physical Rules** (`evolvdb-exec/optimizer`)
- Generate alternatives per logical node
- Join algorithms (NLJ, HashJoin, SortMerge)
- Join commutativity

### Boundaries

**Knows about**: Logical plans, execution layer  
**Doesn't know about**: SQL syntax, storage details  

**Provides to upper layers**: Best physical plan with algorithm choices

---

## Layer 6: Query Execution

### Responsibility
Execute physical plan and return results.

### Components

**Physical Operators** (`evolvdb-exec/op`)
- SeqScanExec, FilterExec, ProjectExec
- NestedLoopJoinExec, HashJoinExec, SortMergeJoinExec
- AggregateExec, InsertExec
- Volcano iterator interface (open/next/close)

**Expression Evaluator** (`evolvdb-exec/expr`)
- Runtime expression evaluation
- Tuple context binding
- Type coercion
- Short-circuit logic

**ExecContext** (`evolvdb-exec`)
- Execution context
- CatalogManager reference
- Optimizer enable/disable flag
- Future: Transaction context

### Boundaries

**Knows about**: Catalog, Table, Tuple  
**Doesn't know about**: SQL parsing, logical planning  

**Provides to users**: Result tuples via iterator interface

---

## Cross-Cutting Concerns

### Configuration

**DbConfig** (`evolvdb-config`)
- Page size (default 4KB)
- Buffer pool size (default 100 pages)
- Data directory path
- Centralized configuration

### Error Handling

**Exceptions** (`evolvdb-common`)
- EvolvDbException (base)
- StorageException, ValidationException, ExecutionException
- Position tracking for SQL errors

### Utilities

**Common** (`evolvdb-common`)
- Shared utilities
- Precondition checks
- Assertion helpers

---

## Data Flow: Query Execution

```
User SQL Query
    ↓
┌─────────────────────────┐
│ SQL Parser              │
│ (Tokenize, Parse)       │
└─────────────────────────┘
    ↓ AST
┌─────────────────────────┐
│ Validator               │
│ (Catalog checks)        │
└─────────────────────────┘
    ↓ Validated AST
┌─────────────────────────┐
│ Binder & Analyzer       │
│ (Resolve names, types)  │
└─────────────────────────┘
    ↓ Logical Plan
┌─────────────────────────┐
│ Logical Rewriter        │
│ (Pushdowns, pruning)    │
└─────────────────────────┘
    ↓ Optimized Logical
┌─────────────────────────┐
│ Volcano Optimizer       │
│ (Algorithm selection)   │
└─────────────────────────┘
    ↓ Physical Plan
┌─────────────────────────┐
│ Operator Execution      │
│ (Volcano model)         │
└─────────────────────────┘
    ↓ Access data
┌─────────────────────────┐
│ Table → HeapFile        │
│ (Tuple ↔ bytes)         │
└─────────────────────────┘
    ↓ Read pages
┌─────────────────────────┐
│ BufferPool              │
│ (Cache, pin/unpin)      │
└─────────────────────────┘
    ↓ Page I/O
┌─────────────────────────┐
│ DiskManager             │
│ (Read/write pages)      │
└─────────────────────────┘
    ↓
Physical Files (.evolv)
```

---

## Data Flow: DDL Operations

### CREATE TABLE

```
SQL: "CREATE TABLE users (...)"
    ↓
Parser → CreateTable AST
    ↓
Validator → Check columns valid
    ↓
CatalogManager.createTable(name, schema)
    ↓
1. Allocate TableId
2. Create TableMeta
3. Encode to bytes (TableMetaCodec)
4. Append to __catalog__ HeapFile
5. Update in-memory maps
```

### INSERT

```
SQL: "INSERT INTO users VALUES (...)"
    ↓
Parser → Insert AST
    ↓
Validator → Check table exists, types match
    ↓
Planner → LogicalInsert
    ↓
Optimizer → InsertPlan (no alternatives)
    ↓
InsertExec.open()
    ↓
For each row:
    1. Create Tuple
    2. Table.insert(tuple)
        ↓
    3. RowCodec.encode(tuple) → bytes
        ↓
    4. HeapFile.insert(bytes)
        ↓
    5. Find page with space (or allocate new)
        ↓
    6. BufferPool.getPage(pageId, forUpdate=true)
        ↓
    7. PageFormat.insert(page, bytes) → RecordId
        ↓
    8. BufferPool.unpin(pageId, dirty=true)
```

---

## Module Dependencies

```
┌─────────────────────────────────────────────────────┐
│                   evolvdb-cli                        │
│              (Command-line interface)                │
└─────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────┐
│                   evolvdb-core                       │
│              (Database facade)                       │
└─────────────────────────────────────────────────────┘
          ↓                              ↓
┌──────────────────────┐      ┌──────────────────────┐
│   evolvdb-exec       │      │  evolvdb-catalog     │
│ (Optimizer, Ops)     │←────→│  (CatalogManager)    │
└──────────────────────┘      └──────────────────────┘
          ↓                              ↓
┌──────────────────────┐      ┌──────────────────────┐
│   evolvdb-planner    │      │   evolvdb-types      │
│ (Logical plans)      │←────→│  (Schema, Tuple)     │
└──────────────────────┘      └──────────────────────┘
          ↓                              ↓
┌──────────────────────┐      ┌──────────────────────┐
│     evolvdb-sql      │      │evolvdb-storage-record│
│  (Parser, AST)       │      │    (HeapFile)        │
└──────────────────────┘      └──────────────────────┘
                                         ↓
                              ┌──────────────────────┐
                              │evolvdb-storage-buffer│
                              │   (BufferPool)       │
                              └──────────────────────┘
                                         ↓
                              ┌──────────────────────┐
                              │evolvdb-storage-page  │
                              │  (PageFormat)        │
                              └──────────────────────┘
                                         ↓
                              ┌──────────────────────┐
                              │evolvdb-storage-disk  │
                              │   (DiskManager)      │
                              └──────────────────────┘
          ↓                              ↓
┌──────────────────────┐      ┌──────────────────────┐
│   evolvdb-config     │      │   evolvdb-common     │
│    (DbConfig)        │      │   (Exceptions)       │
└──────────────────────┘      └──────────────────────┘
```

**Dependency rules**:
- Higher layers depend on lower layers
- Never circular dependencies
- Common/Config are leaf dependencies

---

## Composition Root: Database Facade

**Class**: `io.github.anupam.evolvdb.core.Database`

**Purpose**: Single entry point that wires all components together.

```java
class Database {
    DbConfig config;
    DiskManager diskManager;
    BufferPool bufferPool;
    CatalogManager catalogManager;
    
    Database(Path dataDir) {
        this.config = new DbConfig(dataDir, 4096, 100);
        this.diskManager = new NioDiskManager(config);
        this.bufferPool = new DefaultBufferPool(diskManager, 100, new LruEvictionPolicy());
        this.catalogManager = new CatalogManager(diskManager, bufferPool, new SlottedPageFormat());
    }
    
    void executeQuery(String sql) {
        // Parse
        Statement stmt = SqlParser.parse(sql);
        
        // Validate
        AstValidator validator = new AstValidator(catalogManager);
        validator.validate(stmt);
        
        // Plan
        Binder binder = new Binder(catalogManager);
        LogicalPlan logicalPlan = binder.bind(stmt);
        
        // Optimize
        ExecContext ctx = new ExecContext(catalogManager, useOptimizer=true);
        PhysicalPlan physicalPlan = VolcanoOptimizer.optimize(logicalPlan, ctx);
        
        // Execute
        PhysicalOperator operator = physicalPlan.create(ctx);
        operator.open();
        try {
            while (true) {
                Tuple tuple = operator.next();
                if (tuple == null) break;
                System.out.println(tuple);
            }
        } finally {
            operator.close();
        }
    }
    
    CatalogManager catalog() {
        return catalogManager;
    }
    
    void close() {
        bufferPool.flushAll();
        diskManager.close();
    }
}
```

---

## Concurrency Model (M1-M11)

### Current State: Coarse-Grained Locking

**DiskManager**: Synchronized per FileChannel  
**BufferPool**: Synchronized methods  
**CatalogManager**: Synchronized methods  

**Result**: Single-threaded execution (safe but not concurrent)

### Future (M17): Fine-Grained Locking

**Page-level latches**: Protect individual pages  
**Lock manager**: 2PL for record-level locks  
**Transaction manager**: ACID guarantees  

---

## Failure Handling (M1-M11)

### What Happens on Crash?

**Uncommitted changes**: Lost (no WAL)  
**Dirty pages**: Not flushed → data loss  
**Catalog**: May be inconsistent  

**Recovery**: Restart with last durable state

### Future (M18): WAL & Recovery

**Write-Ahead Log**: All changes logged before applied  
**LSN in pages**: Track durable state  
**ARIES recovery**: Analyze, redo, undo  

---

## Performance Characteristics

### Query Latency

**Simple query** (SELECT with filter):
- Parsing: ~2ms
- Planning: ~3ms
- Execution: ~5ms + I/O
- **Total**: ~10ms + I/O

**Complex query** (multi-way join):
- Parsing: ~5ms
- Planning: ~10ms
- Optimization: ~20ms
- Execution: Variable (depends on data size and join algorithms)

### Throughput

**M1-M11**: Limited by single-threaded execution  
**Future (M23)**: Parallel query execution

### Scalability

**Vertical**: Limited by buffer pool size and single-threaded execution  
**Horizontal**: Not designed for distributed execution (single-node database)

---

## Design Principles Applied

### 1. Separation of Concerns

Each layer has one clear responsibility:
- Storage: Manage bytes
- Types: Enforce schemas
- Planning: Determine what to compute
- Execution: Actually compute it

### 2. Dependency Inversion

High-level modules depend on abstractions:
- BufferPool depends on `DiskManager` interface
- HeapFile depends on `PageFormat` interface
- Operators depend on `PhysicalOperator` interface

### 3. Open/Closed Principle

System is open for extension:
- New eviction policies (without changing BufferPool)
- New join algorithms (without changing optimizer)
- New operators (without changing executor)

### 4. Single Responsibility

Each class has one reason to change:
- DiskManager: Only if I/O strategy changes
- RowCodec: Only if encoding format changes
- Binder: Only if name resolution rules change

---

## Key Architectural Decisions

### 1. Volcano Model for Execution

**Decision**: Pull-based iterators  
**Pros**: Simple, composable, low memory  
**Cons**: Function call overhead, poor cache locality  
**Alternatives**: Push-based, vectorized, compiled  

### 2. Slotted Pages for Storage

**Decision**: Variable-length records with slot directory  
**Pros**: Handles variable data, stable RecordIds  
**Cons**: Fragmentation, compaction overhead  
**Alternatives**: Fixed-size records, log-structured  

### 3. Tuple-at-a-Time Processing

**Decision**: One tuple flows through pipeline  
**Pros**: Low memory, simple operators  
**Cons**: High overhead for large scans  
**Alternatives**: Vectorized (batches), compiled (fused loops)  

### 4. Bottom-Up Optimization

**Decision**: Optimize children first, then parent  
**Pros**: Simple, predictable cost propagation  
**Cons**: May miss global opportunities  
**Alternatives**: Top-down (Volcano/Cascades full)  

### 5. Modular Design with Gradle

**Decision**: Multi-module Gradle project  
**Pros**: Clear boundaries, independent compilation, testability  
**Cons**: More setup, inter-module coordination  
**Alternatives**: Monolithic, package-based separation  

---

## Summary: The Big Picture

EvolvDB is a **layered architecture** where:

1. **Storage** provides durable byte storage with caching
2. **Type System** adds schema enforcement and typed data
3. **SQL Frontend** parses text into structured AST
4. **Planning** converts AST to logical query plan
5. **Optimization** chooses best physical algorithms
6. **Execution** runs the plan and returns results

Each layer is **independently testable** and **loosely coupled**.

The system demonstrates **real database techniques** (slotted pages, Volcano model, cost-based optimization) with **clean code** (SOLID, design patterns, clear boundaries).

---

## Next Steps

You now understand the high-level architecture. In **Phase 10**, we'll dive into low-level design:
- Key classes and interfaces
- Design patterns applied
- Important methods and algorithms
- Code structure and organization

**Continue to**: [`phase-10-lld.md`](./phase-10-lld.md)
