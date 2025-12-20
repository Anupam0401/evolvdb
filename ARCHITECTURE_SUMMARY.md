# EvolvDB Architecture Summary

## Executive Summary

EvolvDB is a **fully functional educational SQL database** built from scratch in Java 21. It demonstrates clean architecture, SOLID principles, and modern database internals. As of M11 completion, it has:

- **Complete storage engine** with NIO-based disk I/O, LRU buffer pool, and variable-length record storage
- **Full SQL pipeline** from parsing through optimized execution
- **Advanced query optimizer** with Volcano-style enumeration, cost-based selection, and three join algorithms
- **Production-ready features**: persistent catalog, type system, expression evaluation, aggregations

**What's Missing**: Indexes (B+Tree), transactions (2PL/MVCC), durability (WAL), and advanced SQL features.

## From Smallest to Largest: Architecture Walkthrough

### 1. Foundation: Identifiers (Records)

The smallest building blocks are simple record types:

```java
FileId(String name)              // Logical file name: "users"
PageId(FileId fileId, int pageNo) // Physical page address
RecordId(PageId pageId, short slot) // Record within a page
```

These form the addressing scheme for all data in the system.

### 2. Storage Layer: Bottom-Up

**DiskManager** (M1)
- Manages physical files on disk
- Allocates pages at file tail (zero-filled)
- Reads/writes exactly one page by PageId
- Uses Java NIO FileChannel for I/O
- Thread-safe with coarse-grained synchronization

**BufferPool** (M2)
- Caches pages in memory (default: 100 pages)
- Pin/unpin semantics protect in-use pages
- LRU eviction policy (Strategy pattern)
- Tracks dirty pages, flushes on eviction
- Frame structure: `{PageId, ByteBuffer, pinCount, dirty}`

**PageFormat** (M3)
- Defines record layout within pages
- SlottedPageFormat implementation:
  - Header: pageType, LSN, slotCount, freeStartOffset
  - Payload grows upward, slot directory grows downward
  - Each slot: `{offset, length}` (negative length = tombstone)
  - Supports variable-length records, in-place updates, compaction

**HeapFile** (M4)
- Multi-page file abstraction
- Insert: scans pages for space, allocates new if needed
- Read: pins page, delegates to PageFormat
- Delete: marks slot as tombstone
- Update: in-place if fits, else relocate (delete + insert)

**Scan & Update** (M5)
- Iterator over all live records across pages
- Skips tombstones automatically
- Update semantics: preserves RecordId when possible

### 3. Type System & Metadata (M6-M7)

**Type System**
```
Type enum: INT(4), BIGINT(8), BOOLEAN(1), FLOAT(4), VARCHAR(n), STRING
ColumnMeta: {name, Type, length?}
Schema: List<ColumnMeta> with unique names (case-insensitive)
```

**Tuple**
- Immutable row bound to Schema
- Type validation on construction (no nulls yet)
- Values stored as List<Object>

**RowCodec**
- Binary encoding: little-endian, fixed-width first, var-width with u16 length prefix
- Encode: Tuple → byte[]
- Decode: byte[] + Schema → Tuple

**Catalog**
- CatalogManager: persistent metadata in system HeapFile (`__catalog__`)
- TableMeta: {TableId, name, Schema, FileId}
- Versioned encoding: UPSERT and DROP operations
- Scan-based recovery on startup

**Table**
- Wrapper over HeapFile with Schema
- insert(Tuple), read(RecordId), update(RecordId, Tuple), scanTuples()
- Handles Tuple ↔ bytes conversion via RowCodec

### 4. SQL Front-End (M8)

**Parser**
- Recursive descent parser
- Tokenizer: lexical analysis with position tracking
- Grammar: CREATE TABLE, DROP TABLE, INSERT, SELECT (single table + joins via comma-separated FROM)
- AST nodes: CreateTable, DropTable, Insert, Select, Expr (binary, comparison, logical, literals)

**Validator**
- AstValidator: catalog-aware checks
- Verifies table existence, column references, type compatibility
- Catches duplicate columns, invalid VARCHAR lengths

### 5. Logical Planning (M9)

**Binder/Analyzer**
- Resolves names against Catalog
- Type inference: ColumnRef → column type, literals → INT/BIGINT/BOOLEAN/STRING, arithmetic → promotion
- Builds logical plan tree:
  - LogicalScan(tableName, alias)
  - LogicalFilter(child, predicate)
  - LogicalProject(child, items)
  - LogicalJoin(left, right, type, condition) - extracts join conditions from WHERE
  - LogicalAggregate(child, groupBy, aggregates)
  - LogicalInsert(table, columns, rows)

**Rule Engine**
- Visitor pattern for plan traversal
- Rules: PredicateSimplification, PushProjectBelowFilter, RemoveRedundantProject
- Fixed-point iteration per node

### 6. Physical Planning & Optimizer (M10-M11)

**Logical Rewriter**
- Pre-optimization logical transformations:
  - Predicate pushdown across joins
  - Projection pruning (eliminates unused columns)
  - Join reordering (left-deep to bushy trees)

**Volcano Optimizer**
- Bottom-up optimization with memoization
- **Memo structure**:
  - Group: set of logically equivalent expressions
  - GroupExpr: one expression with child group references
  - Join commutativity: auto-generates commuted joins for INNER JOIN
- **Cost Model**: estimates (rowCount, cpu, io)
  - SeqScan: 1000 rows default (no stats yet)
  - Filter: 0.1 selectivity
  - NestedLoopJoin: O(n*m) with 0.25 join selectivity
  - HashJoin: O(n+m) linear in sum of inputs
  - SortMergeJoin: O(n log n + m log m)
- **Physical Rules**: generate alternatives per logical node
  - Scan → SeqScanPlan
  - Filter → FilterPlan
  - Project → ProjectPlan
  - Join → [NestedLoopJoinPlan, HashJoinPlan, SortMergeJoinPlan]
  - Aggregate → AggregatePlan
  - Insert → InsertPlan
- **Cost-based selection**: picks lowest-cost alternative (tie-breaker favors HashJoin > SortMergeJoin > NestedLoopJoin)

**Physical Operators (Volcano Model)**
- Interface: open(), next() → Tuple, close(), schema()
- Pull-based iterator model: parent calls next() until null
- **SeqScanExec**: scans Table.scanTuples()
- **FilterExec**: evaluates predicate, passes matching tuples
- **ProjectExec**: computes projection expressions, builds new tuple
- **NestedLoopJoinExec**: double loop, buffered right side, O(n*m)
- **HashJoinExec**: builds hash table on right, probes with left, O(n+m) for equi-join
- **SortMergeJoinExec**: sorts both sides, merge with cursors, O(n log n + m log m)
- **AggregateExec**: in-memory hash map, supports COUNT/SUM/AVG/MIN/MAX, GROUP BY
- **InsertExec**: inserts rows via Catalog.openTable().insert()

**Expression Evaluator**
- Runtime evaluation of AST expressions
- Literals, ColumnRef (by name or qualified name), binary ops, comparisons, logical ops
- Type coercion: INT → BIGINT → FLOAT for arithmetic

### 7. Execution Flow

**End-to-End Query Path**:
1. SQL string → Parser → AST
2. AST → Validator (catalog checks) → Validated AST
3. Validated AST → Binder → Logical Plan
4. Logical Plan → LogicalRewriter → Optimized Logical Plan
5. Optimized Logical Plan → VolcanoOptimizer → Best Physical Plan
6. Best Physical Plan → create() → Physical Operator Tree
7. Operator Tree → open() → next() loop → close() → Results

**Example: SELECT name FROM users WHERE age > 25**
```
Physical Tree:
  ProjectExec(name)
    └── FilterExec(age > 25)
          └── SeqScanExec(users)

Execution:
1. Client calls ProjectExec.open()
   - Calls FilterExec.open()
     - Calls SeqScanExec.open()
       - Opens HeapFile.scan() iterator
2. Client calls ProjectExec.next() in loop:
   - ProjectExec calls FilterExec.next()
     - FilterExec calls SeqScanExec.next()
       - SeqScanExec fetches tuple from HeapFile
     - FilterExec evaluates "age > 25"
       - If false, loop back to SeqScanExec.next()
       - If true, return tuple to ProjectExec
   - ProjectExec extracts "name" column, builds new tuple
   - Returns projected tuple to client
3. Client calls ProjectExec.close()
   - Cascades close() down tree
```

## Design Patterns Used

- **Strategy**: EvictionPolicy, PageFormat, CostModel
- **Visitor**: AST traversal, LogicalPlan traversal
- **Iterator**: Volcano operators (open/next/close)
- **Factory**: TableMeta creation, Physical plan creation
- **Builder**: SQL AST construction
- **Facade**: Database (composition root), Table
- **Template**: Operator lifecycle (open/next/close)
- **RAII**: Pin/unpin via try-with-resources pattern

## SOLID Principles Adherence

- **Single Responsibility**: Each class has one clear job (e.g., DiskManager only does page I/O)
- **Open/Closed**: New eviction policies, page formats, rules can be added without modifying core code
- **Liskov Substitution**: Any EvictionPolicy can replace LruEvictionPolicy
- **Interface Segregation**: Interfaces expose only necessary methods (e.g., PhysicalOperator)
- **Dependency Inversion**: High-level modules depend on abstractions (BufferPool → DiskManager interface)

## Performance Characteristics

**Storage**:
- Page size: 4KB default (configurable)
- Buffer pool: 100 pages default (~400KB memory)
- No free-space map: O(n) page scan for insert
- No prefetching or async I/O

**Query Execution**:
- No parallelism: single-threaded execution
- No pipelining: each operator is blocking
- Hash join: in-memory only (no spill-to-disk)
- Aggregation: in-memory only (no external sort)
- No query result caching

**Optimizer**:
- No statistics: uses default row counts (1000)
- No cardinality estimation: fixed selectivity (0.1 for filter, 0.25 for join)
- Memo structure: supports bushy plans and join commutativity
- Cost model: simplistic but extensible

## What Makes This Database Educational Yet Impressive

### Strengths
1. **Complete vertical slice**: From disk I/O to SQL queries working end-to-end
2. **Modern optimizer**: Volcano-style with memo, cost model, multiple join algorithms
3. **Clean architecture**: Well-factored modules, clear separation of concerns
4. **Comprehensive tests**: Unit tests, integration tests, E2E tests all passing
5. **Good documentation**: Detailed docs for each milestone with HLD/LLD

### Limitations (by design for M1-M11)
1. **No persistence guarantees**: No WAL, crashes lose uncommitted data
2. **No concurrency control**: Single-threaded, no transactions
3. **No indexes**: Sequential scans only
4. **Limited SQL**: Basic SELECT/INSERT/CREATE/DROP, no UPDATE/DELETE/JOIN syntax
5. **No nulls**: Type system doesn't support NULL values yet

## Recommended Next Steps

### Priority 1: UPDATE and DELETE statements (Low-hanging fruit)
- Already have `HeapFile.update()` and `HeapFile.delete()`
- Need SQL parsing for UPDATE/DELETE
- Need LogicalUpdate and LogicalDelete plan nodes
- Need UpdateExec and DeleteExec operators
- **Effort**: ~2-3 days
- **Value**: Makes database actually usable for CRUD operations

### Priority 2: NULL support (Foundation for advanced features)
- Add NULL marker in RowCodec (e.g., null bitmap)
- Update Tuple validation to allow nulls
- Update expression evaluator for NULL semantics (SQL three-valued logic)
- **Effort**: ~2-3 days
- **Value**: Required for outer joins, default values, many SQL features

### Priority 3: B+Tree Indexing (M12)
- Most impactful for query performance
- Natural next milestone after optimizer
- **Components needed**:
  - BPlusTree class (search, insert, delete, split, merge)
  - Index page format (internal nodes, leaf nodes)
  - IndexMeta in catalog
  - IndexScan operator
  - Index selection rules in optimizer
- **Effort**: ~2-3 weeks
- **Value**: 10-1000x speedup for point queries and range scans

### Priority 4: Transactions (M13)
- Critical for data integrity
- **Components needed**:
  - TransactionManager (begin/commit/abort)
  - LockManager (2PL with deadlock detection)
  - Transaction context through operators
  - Versioned tuples or undo log
- **Effort**: ~3-4 weeks
- **Value**: ACID guarantees, multi-user safety

### Priority 5: WAL & Recovery (M14)
- Required for durability
- **Components needed**:
  - LogManager (append WAL records)
  - LSN in pages
  - RecoveryManager (ARIES: analyze, redo, undo)
  - Checkpoint mechanism
- **Effort**: ~3-4 weeks
- **Value**: Crash recovery, durability guarantee

## Testing Strategy Recommendations

For new features, follow the existing pattern:

1. **Unit tests**: Test individual components in isolation (e.g., BPlusTree split/merge)
2. **Integration tests**: Test component interactions (e.g., IndexScan with BufferPool)
3. **E2E tests**: Full SQL queries exercising new features
4. **Property-based tests**: Randomized operations maintaining invariants (e.g., B+Tree structure)
5. **Concurrency tests**: Multi-threaded harness for transactions (when implemented)

## Conclusion

EvolvDB successfully demonstrates **end-to-end database implementation** from physical storage through optimized query execution. The codebase is well-structured, tested, and documented. It's production-ready for the features implemented (M1-M11) but needs indexes, transactions, and WAL for real-world use.

The most valuable next step depends on goals:
- **Learning**: Implement B+Tree indexing (teaches tree algorithms, index structures)
- **Usability**: Add UPDATE/DELETE (makes CRUD complete)
- **Production-readiness**: Add transactions + WAL (enables multi-user safety and durability)

**Estimated time to production-ready**: ~6-8 weeks for M12-M14 (indexes, transactions, WAL).
