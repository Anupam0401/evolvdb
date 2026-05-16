# Phase 11: What's Missing & Why

**Goal**: Understand planned features, why they're important, and how they would integrate.

**Prerequisites**: Phase 0-10 (Complete system understanding)

---

## Overview: Production Gaps

EvolvDB M1-M12 implements a **complete vertical slice** of a database including basic CRUD operations, but several critical production features are missing:

| Feature | Status | Priority | Complexity |
|---------|--------|----------|------------|
| **UPDATE/DELETE** | ✅ M12 (Complete) | High | Low |
| **NULL Literal Parsing** | ✅ Fixed (parser recognizes NULL keyword) | High | Low |
| **Unified execute(sql)** | ✅ Fixed (Database.execute + REPL) | High | Medium |
| **NULL Semantics** | M13 (Planned) | High | Medium |
| **ORDER BY/LIMIT** | M15 (Planned) | Medium | Low |
| **B+Tree Indexes** | M16 (Planned) | Critical | High |
| **Transactions** | M17 (Planned) | Critical | Very High |
| **WAL & Recovery** | M18 (Planned) | Critical | Very High |
| **Subqueries** | M21 (Planned) | Medium | Medium |
| **Window Functions** | M22 (Planned) | Low | High |

### Bug Fixes Applied (Post-M12)

| Bug | Root Cause | Fix |
|-----|-----------|-----|
| HeapFile catches `IllegalStateException` | Generic exception swallowed real bugs | Introduced `PageFullException`; HeapFile catches only that |
| NULL parsed as column reference | `NULL` not in keyword map or parser | Added `NULL` token type, keyword mapping, and parser case |
| UpdateExec/DeleteExec bypass Volcano | Operators extracted filter from logical plan, rescanned table | Refactored to use child operator via `SeqScanWithRidExec` + `lastRecordId()` |
| MultiAggregateTest disabled | `SumAgg` field named `f` caused shadowing with `instanceof Float` check | Renamed to `isFloat`, `floatSum`, `intSum`; test re-enabled and passing |
| No unified SQL entry point | CLI hardcoded; no `execute()` API | Added `Database.execute(String sql)` + interactive CLI REPL |

---

## ✅ M12: UPDATE & DELETE Statements (COMPLETED)

### ✅ Already Implemented

**SQL (fully supported)**:
```sql
UPDATE users SET age = 31 WHERE id = 1;
UPDATE users SET age = age + 1, name = 'Alice' WHERE active = true;
DELETE FROM users WHERE age < 18;
DELETE FROM orders;  -- Delete all rows
```

**Status**: Complete implementation with comprehensive tests

### What Was Implemented

#### 1. ✅ SQL Parser

**Implemented grammar**:
```
updateStmt := UPDATE ident SET assignment (',' assignment)* [WHERE expr]
assignment := ident '=' expr

deleteStmt := DELETE FROM ident [WHERE expr]
```

**Location**: `evolvdb-sql/src/main/java/io/github/anupam/evolvdb/sql/parser/SqlParser.java`

#### 2. ✅ AST Nodes

**Implemented classes**:
- `Update.java` - UPDATE statement AST
- `Delete.java` - DELETE statement AST

**Location**: `evolvdb-sql/src/main/java/io/github/anupam/evolvdb/sql/ast/`

#### 3. ✅ Logical Plans

**Implemented classes**:
- `LogicalUpdate` - Logical update with assignments and child plan
- `LogicalDelete` - Logical delete with child plan

**Location**: `evolvdb-planner/src/main/java/io/github/anupam/evolvdb/planner/logical/`

#### 4. ✅ Physical Operators

**Implemented classes**:
- `UpdateExec.java` — Pulls matching tuples from child operator via Volcano model, applies SET assignments, calls `table.update(rid, newTuple)`
- `DeleteExec.java` — Pulls matching tuples, collects RecordIds, then deletes in batch
- `SeqScanWithRidExec.java` — Scan operator that tracks RecordIds, used as the leaf for UPDATE/DELETE child plans

**Key implementation details**:
- Uses the Volcano pull model: child operator chain (e.g., `FilterExec` → `SeqScanWithRidExec`) produces tuples with associated RecordIds
- `PhysicalOperator.lastRecordId()` default method propagates RecordIds through intermediate operators like `FilterExec`
- `PhysicalPlanner.planDmlChild()` ensures scan leaves use `SeqScanWithRidExec` instead of `SeqScanExec`
- Returns result tuple with updated/deleted count

**Location**: `evolvdb-exec/src/main/java/io/github/anupam/evolvdb/exec/op/`

#### 5. ✅ Tests

**Comprehensive test coverage**:
- `UpdateDeleteParserTest.java` — Parser tests for UPDATE/DELETE SQL (including NULL literal assignment)
- `UpdateDeleteExecTest.java` — End-to-end execution tests

**Location**: Test directories in respective modules

#### 6. ✅ Unified Execute API

**`Database.execute(String sql)`** — Single entry point wiring the full pipeline:
- Handles DDL (CREATE TABLE, DROP TABLE) directly via CatalogManager
- Handles DML (SELECT, INSERT, UPDATE, DELETE) through: Parser → Validator → Analyzer → PhysicalPlanner → Volcano execution
- Returns `QueryResult` with schema + rows (for queries) or a status message (for DDL)
- CLI refactored into an interactive SQL REPL

**Location**: `evolvdb-core/src/main/java/io/github/anupam/evolvdb/core/Database.java`

### Implementation Notes

**RecordId handling**: 
- RecordIds propagate through the Volcano operator chain via `PhysicalOperator.lastRecordId()`
- Updates may relocate records (HeapFile returns new RecordId)
- Currently no index updates (indexes not yet implemented)
- Future M16 will need to track RecordId changes for index maintenance

**Performance**:
- Sequential scan for WHERE clause (no index support yet)
- Batch deletes to avoid concurrent modification issues

### Why This Matters

✅ **CRUD completeness** achieved — database now supports all basic operations  
✅ **Volcano integrity** — UPDATE/DELETE participate properly in the pull-based pipeline  
✅ **Real usability** — unified `execute(sql)` API and interactive REPL  
✅ **Foundation for transactions** — UPDATE/DELETE will integrate with M17 transaction support

---

## M13: NULL Support

### What's Missing

**SQL**:
```sql
CREATE TABLE users (id INT, name VARCHAR(50), age INT);
INSERT INTO users VALUES (1, 'Alice', NULL);
SELECT * FROM users WHERE age IS NULL;
SELECT * FROM users WHERE age IS NOT NULL;
```

**Current**: NULLs not supported, any NULL value causes exception

### Why It Matters

**Real-world data**: Missing data is common (optional fields, incomplete forms)

**SQL compliance**: NULL is fundamental to SQL (three-valued logic)

**Use cases**:
- Optional profile fields
- Data migration (partial records)
- Outer joins (future M20)

### How It Would Work

#### 1. Storage: Null Bitmap

**Add to RowCodec encoding**:
```
[Null bitmap (1 bit per column, rounded up to bytes)]
[Fixed-width non-null values]
[Variable-width non-null values]
```

**Example**: 3 columns → 1 byte bitmap
- Bit 0: Column 0 is NULL?
- Bit 1: Column 1 is NULL?
- Bit 2: Column 2 is NULL?

**Encoding**:
```java
byte[] encode(Schema schema, Tuple tuple) {
    int columnCount = schema.columnCount();
    int bitmapBytes = (columnCount + 7) / 8;
    
    byte[] bitmap = new byte[bitmapBytes];
    for (int i = 0; i < columnCount; i++) {
        if (tuple.get(i) == null) {
            bitmap[i / 8] |= (1 << (i % 8));
        }
    }
    
    ByteBuffer buf = ...;
    buf.put(bitmap);
    
    // Encode only non-null values
    for (int i = 0; i < columnCount; i++) {
        if (tuple.get(i) != null) {
            encodeValue(buf, schema.getColumn(i).type(), tuple.get(i));
        }
    }
    
    return buf.array();
}
```

#### 2. Three-Valued Logic

**SQL logic with NULLs**:
```
TRUE AND NULL   → NULL
FALSE AND NULL  → FALSE
TRUE OR NULL    → TRUE
FALSE OR NULL   → NULL
NULL = NULL     → NULL (not TRUE!)
NULL IS NULL    → TRUE
```

**Expression evaluation**:
```java
Object evalComparison(Object left, Object right, CompOp op) {
    if (left == null || right == null) {
        return null;  // Unknown
    }
    return compare(left, right, op);
}

Object evalAnd(Object left, Object right) {
    if (Boolean.FALSE.equals(left) || Boolean.FALSE.equals(right)) {
        return false;  // Short-circuit
    }
    if (left == null || right == null) {
        return null;  // Unknown
    }
    return Boolean.TRUE.equals(left) && Boolean.TRUE.equals(right);
}
```

#### 3. Filter Semantics

**WHERE clause**: Only rows where predicate is TRUE (not NULL or FALSE)

```java
Tuple next() {
    while (true) {
        Tuple tuple = child.next();
        if (tuple == null) return null;
        
        Object result = evaluator.eval(predicate, tuple);
        if (Boolean.TRUE.equals(result)) {  // Not just "true", but TRUE
            return tuple;
        }
        // NULL or FALSE: skip row
    }
}
```

#### 4. Aggregate Handling

**COUNT(*)**: Count all rows including NULLs
**COUNT(expr)**: Count only non-NULL values
**SUM/AVG/MIN/MAX**: Ignore NULL values

```java
void updateAggregate(AggregateCall agg, Tuple tuple) {
    if (agg.isCountStar()) {
        count++;
    } else {
        Object value = evaluator.eval(agg.expr(), tuple);
        if (value != null) {  // Ignore NULLs
            count++;
            sum += (Number) value;
        }
    }
}
```

### Challenges

**Storage overhead**: 1 bit per column (minimal)
**Logic complexity**: Three-valued logic is counterintuitive
**Performance**: NULL checks in hot paths

### Estimated Effort

**4-5 days** for complete implementation with tests

---

## M16: B+Tree Indexes

### What's Missing

**SQL**:
```sql
CREATE INDEX idx_age ON users(age);
SELECT * FROM users WHERE age > 30;  -- Uses index
```

**Current**: All queries use sequential scan (O(n))

### Why It Matters

**Performance**: 10-1000x speedup for point queries and range scans

**Example**:
- Without index: Scan 1M rows to find age=30 → 1M comparisons
- With index: B+Tree lookup → ~log(1M) ≈ 20 comparisons

**Use cases**:
- Primary key lookups
- Foreign key joins
- Range queries (age BETWEEN 20 AND 30)

### How It Would Work

#### 1. B+Tree Structure

**Internal nodes**: Keys + child pointers
**Leaf nodes**: Keys + RecordIds (pointing to heap file)
**Linked leaves**: For range scans

```
                [50]
               /    \
        [25]            [75]
       /    \          /    \
[10,20]  [30,40]  [60,70]  [80,90]
  ↓        ↓        ↓        ↓
RecordIds in heap file
```

**Properties**:
- All leaves at same depth
- Each node 50-100% full (B=order)
- Sorted keys for binary search

#### 2. Index Page Layout

**Internal node page**:
```
[Header: pageType=INDEX_INTERNAL, keyCount, ...]
[Key₀, ChildPtr₀, Key₁, ChildPtr₁, ..., KeyN, ChildPtrN]
```

**Leaf node page**:
```
[Header: pageType=INDEX_LEAF, keyCount, nextLeafPtr]
[Key₀, RecordId₀, Key₁, RecordId₁, ..., KeyN, RecordIdN]
```

#### 3. IndexMeta in Catalog

```java
record IndexMeta(
    IndexId indexId,
    String name,
    TableId tableId,
    List<String> columns,  // Multi-column indexes
    FileId fileId,
    PageId rootPageId
) {}
```

**Persistent in catalog**:
```
CatalogManager.createIndex(tableName, indexName, columns)
→ Allocate IndexId
→ Build B+Tree from existing data
→ Store IndexMeta in __catalog__
```

#### 4. IndexScan Operator

```java
class IndexScanExec extends PhysicalOperator {
    BPlusTree index;
    Expr predicate;  // e.g., age > 30
    
    void open() {
        if (predicate is point query) {
            // index.search(key) → single RecordId
            cursor = index.pointQuery(extractKey(predicate));
        } else if (predicate is range) {
            // index.range(low, high) → cursor over range
            cursor = index.rangeQuery(extractLow(), extractHigh());
        }
    }
    
    Tuple next() {
        RecordId rid = cursor.next();
        if (rid == null) return null;
        return table.read(rid);
    }
}
```

#### 5. Optimizer Integration

**Index selection rule**:
```java
if (logical plan is Scan + Filter) {
    Expr predicate = filter.predicate();
    List<IndexMeta> indexes = catalog.getIndexes(scan.tableName());
    
    for (IndexMeta index : indexes) {
        if (predicateUsesIndex(predicate, index.columns())) {
            alternatives.add(new IndexScanPlan(index, predicate));
        }
    }
}
```

**Cost comparison**:
```java
// SeqScan: O(n)
Cost seqScanCost = new Cost(tableRows, tableRows * CPU, tableRows / tuplesPerPage);

// IndexScan: O(log n) + result size
Cost indexScanCost = new Cost(
    tableRows * selectivity,
    (Math.log(tableRows) + tableRows * selectivity) * CPU,
    Math.log(tableRows) + tableRows * selectivity / tuplesPerPage
);
```

**Winner**: IndexScan for selective queries (selectivity < 10%)

#### 6. Index Maintenance

**Insert**: Add key+RecordId to B+Tree (may cause splits)
**Delete**: Remove key from B+Tree (may cause merges)
**Update**: Delete old + Insert new (if indexed column changes)

```java
class Table {
    void insert(Tuple tuple) {
        RecordId rid = heapFile.insert(encode(tuple));
        
        // Update all indexes
        for (IndexMeta indexMeta : catalog.getIndexes(tableId)) {
            BPlusTree index = openIndex(indexMeta);
            Object key = extractKey(tuple, indexMeta.columns());
            index.insert(key, rid);
        }
    }
}
```

### Challenges

**B+Tree complexity**: Split/merge algorithms are tricky
**Concurrency**: Index latching during modifications (future M17)
**Index choice**: Multi-index selection (which index to use?)
**Covering indexes**: Index-only scans (don't access heap file)

### Estimated Effort

**2-3 weeks** for complete implementation with split/merge logic and tests

---

## M17: Transactions & Concurrency

### What's Missing

**SQL**:
```sql
BEGIN;
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
UPDATE accounts SET balance = balance + 100 WHERE id = 2;
COMMIT;
```

**Current**: No isolation, concurrent access unsafe, no rollback

### Why It Matters

**ACID properties**:
- **Atomicity**: All-or-nothing execution
- **Consistency**: Invariants preserved
- **Isolation**: Concurrent transactions don't interfere
- **Durability**: Committed data survives crashes (M18)

**Use cases**:
- Bank transfers (atomic)
- Multi-user systems (isolation)
- Error recovery (rollback)

### How It Would Work

#### 1. Transaction Manager

```java
interface TransactionManager {
    Transaction begin();
    void commit(Transaction txn);
    void abort(Transaction txn);
}

class Transaction {
    TransactionId txnId;
    IsolationLevel level;
    Set<PageId> readSet;   // For validation
    Set<PageId> writeSet;  // For rollback
}
```

#### 2. Lock Manager (2PL)

**Two-Phase Locking**:
- **Growing phase**: Acquire locks, never release
- **Shrinking phase**: Release locks, never acquire

```java
enum LockMode {
    SHARED,      // Read lock (multiple readers)
    EXCLUSIVE    // Write lock (single writer)
}

interface LockManager {
    void lock(Transaction txn, RecordId rid, LockMode mode);
    void unlock(Transaction txn, RecordId rid);
    void releaseAll(Transaction txn);
}
```

**Lock compatibility**:
```
       S    X
S      ✓    ✗
X      ✗    ✗
```

**Deadlock detection**: Wait-for graph, abort youngest transaction

#### 3. MVCC Alternative

**Multi-Version Concurrency Control**:
- Each tuple has multiple versions
- Readers see snapshot (don't block writers)
- Writers create new versions

```java
class TupleVersion {
    byte[] data;
    TransactionId xmin;  // Created by
    TransactionId xmax;  // Deleted by
    
    boolean visibleTo(Transaction txn) {
        return xmin.committed() && xmin < txn.startId 
            && (xmax == null || !xmax.committed() || xmax >= txn.startId);
    }
}
```

**Trade-offs**:
- **2PL**: Simpler, but blocking
- **MVCC**: Complex, but better concurrency

#### 4. Operator Integration

**Thread transaction context through operators**:

```java
class ExecContext {
    CatalogManager catalog;
    Transaction transaction;  // Added
}

class SeqScanExec {
    void open() {
        iterator = table.scanTuples(ctx.transaction());  // Pass txn
    }
}

class Table {
    Tuple read(RecordId rid, Transaction txn) {
        lockManager.lock(txn, rid, LockMode.SHARED);
        return heapFile.read(rid);
    }
    
    void insert(Tuple tuple, Transaction txn) {
        RecordId rid = heapFile.insert(encode(tuple));
        lockManager.lock(txn, rid, LockMode.EXCLUSIVE);
    }
}
```

#### 5. Rollback Support

**Undo log**:
```java
interface UndoLog {
    void logInsert(Transaction txn, RecordId rid);
    void logUpdate(Transaction txn, RecordId rid, byte[] oldData);
    void logDelete(Transaction txn, RecordId rid, byte[] oldData);
    
    void undo(Transaction txn);  // Called on abort
}
```

**Abort process**:
```
1. Read undo log for transaction
2. For each operation in reverse:
   - Insert → Delete the record
   - Update → Restore old data
   - Delete → Reinsert old data
3. Release all locks
4. Mark transaction as aborted
```

### Challenges

**Deadlock handling**: Detection + resolution strategy
**Lock granularity**: Record vs page vs table locks
**Performance**: Lock overhead on every access
**Complexity**: Transactions touch every component

### Estimated Effort

**3-4 weeks** for 2PL with deadlock detection and undo logs

---

## M18: Write-Ahead Logging & Recovery

### What's Missing

**Crash scenario**:
```
1. Transaction commits
2. Dirty pages not yet flushed
3. Power failure
4. Data lost ❌
```

**Current**: No durability guarantee

### Why It Matters

**Durability**: Committed data must survive crashes

**Recovery**: Rebuild consistent state after crash

**Use cases**:
- Server crashes
- Power failures
- OS crashes

### How It Would Work

#### 1. WAL Structure

**Log record types**:
```java
interface LogRecord {
    LSN lsn();            // Log Sequence Number
    TransactionId txnId();
    
    void redo();          // Replay during recovery
    void undo();          // Rollback during recovery
}

class InsertLogRecord implements LogRecord {
    RecordId rid;
    byte[] data;
}

class UpdateLogRecord implements LogRecord {
    RecordId rid;
    byte[] oldData;
    byte[] newData;
}

class CommitLogRecord implements LogRecord {
    // No data, just marks commit point
}
```

**WAL file**: Sequential append-only log
```
[LSN=1: BEGIN txn=100]
[LSN=2: UPDATE rid=..., old=..., new=..., txn=100]
[LSN=3: INSERT rid=..., data=..., txn=100]
[LSN=4: COMMIT txn=100]
[LSN=5: BEGIN txn=101]
...
```

#### 2. WAL Protocol

**Write-Ahead Logging rule**:
1. Log record must be on disk **before** data page
2. All log records for transaction must be on disk **before** commit returns

```java
class Table {
    void insert(Tuple tuple, Transaction txn) {
        byte[] data = encode(tuple);
        
        // 1. Write log record
        LSN lsn = logManager.append(new InsertLogRecord(txn, data));
        
        // 2. Flush log (sync to disk)
        logManager.flush(lsn);
        
        // 3. Apply change to page (in BufferPool, not yet on disk)
        RecordId rid = heapFile.insert(data);
        
        // 4. Update page LSN
        page.setLSN(lsn);
    }
}
```

#### 3. Page LSN

**Each page tracks its LSN**:
```java
class Page {
    long pageLSN;  // LSN of last log record that modified this page
}
```

**Flush rule**: Can only flush page if `pageLSN <= durableLSN`
- Ensures log flushed before page

#### 4. Checkpointing

**Periodic checkpoint** to bound recovery time:
```
1. Pause new transactions
2. Flush all dirty pages
3. Write CHECKPOINT record to log
4. Resume transactions
```

**Checkpoint record**:
```java
class CheckpointRecord {
    LSN checkpointLSN;
    Set<TransactionId> activeTransactions;
    Map<PageId, LSN> dirtyPages;
}
```

#### 5. ARIES Recovery

**Three phases**:

**Analysis**:
```
1. Scan log from last checkpoint
2. Build active transaction table
3. Build dirty page table
```

**Redo**:
```
1. Replay all changes from first dirty page
2. Brings database to pre-crash state
3. Includes uncommitted changes
```

**Undo**:
```
1. Roll back uncommitted transactions
2. Use undo log records
3. Restores consistency
```

**Algorithm**:
```java
void recover() {
    // Analysis
    LSN checkpointLSN = findLastCheckpoint();
    Set<TransactionId> activeTxns = new HashSet<>();
    Set<PageId> dirtyPages = new HashSet<>();
    
    for (LogRecord record : scanLog(checkpointLSN)) {
        if (record is BEGIN) {
            activeTxns.add(record.txnId());
        } else if (record is COMMIT) {
            activeTxns.remove(record.txnId());
        } else {
            dirtyPages.add(record.pageId());
        }
    }
    
    // Redo
    LSN redoStart = dirtyPages.stream()
        .map(pid -> getPageLSN(pid))
        .min()
        .orElse(checkpointLSN);
    
    for (LogRecord record : scanLog(redoStart)) {
        if (needsRedo(record)) {
            record.redo();
        }
    }
    
    // Undo
    for (TransactionId txnId : activeTxns) {
        List<LogRecord> txnLog = getTransactionLog(txnId);
        for (LogRecord record : reverse(txnLog)) {
            record.undo();
        }
        logManager.append(new AbortLogRecord(txnId));
    }
}
```

### Challenges

**Log I/O overhead**: Every write generates log record
**Recovery time**: Proportional to log size (checkpoints help)
**Concurrency**: Log must be append-only (single writer)
**Complexity**: ARIES is intricate with many corner cases

### Estimated Effort

**3-4 weeks** for complete WAL + ARIES recovery with tests

---

## Feature Integration Roadmap

### Dependencies

```
✅ M12 (UPDATE/DELETE) - COMPLETE
  ↓
M13 (NULL Support)
  ↓
M15 (ORDER BY/LIMIT)
  ↓
M16 (Indexes) ← Performance critical
  ↓
M17 (Transactions) ← Correctness critical
  ↓
M18 (WAL) ← Durability critical
```

**Rationale**:
1. **✅ M12**: SQL completeness (CRUD operations) - DONE
2. **M13**: NULL support (foundation for OUTER JOINs)
3. **M15**: Query result control (ORDER BY, LIMIT, OFFSET)
4. **M16**: Performance (10-1000x for selective queries)
5. **M17**: Multi-user safety (ACID properties)
6. **M18**: Crash recovery (durability guarantee)

### Timeline to Production

**✅ Phase 1a** (M12): Complete
- UPDATE/DELETE implemented
- **Outcome**: Basic CRUD operations working

**Phase 1b** (M13-M15): 1-2 weeks
- NULLs, ORDER BY/LIMIT
- **Outcome**: Fully functional single-user database

**Phase 2** (M16-M18): 7-10 weeks ⚠️ **Critical**
- Indexes, Transactions, WAL
- **Outcome**: Production-ready multi-user database

**Total**: ~2-2.5 months to production-ready (down from 3 months with M12 complete)

---

## Summary: What Makes EvolvDB "Almost Production"

### Already Implemented ✅

- Complete storage engine (pages, caching, heap files)
- Type system with schema enforcement
- SQL parsing and validation (including UPDATE/DELETE)
- Query planning (logical and physical)
- Query optimization (Volcano-style with cost model)
- Multiple join algorithms
- Aggregations
- **Full CRUD**: INSERT, SELECT, UPDATE, DELETE ✅

### Missing for Production ❌

- **Indexes** → Without: 1000x slower queries
- **Transactions** → Without: Data corruption possible
- **WAL** → Without: Data loss on crash
- **NULLs** → Without: Can't model optional data
- **ORDER BY/LIMIT** → Without: Can't control result order/size

### The Gap

EvolvDB is a **complete educational database** with **production-quality architecture** for what it implements.

The gap to production is **not architectural** (design is extensible), but **implementation effort** (~3 months for critical features).

---

## Next Steps

You now understand what's missing and why. In **Phase 12**, we'll prepare for interviews:
- 30-second pitch
- Common questions with answers
- How to explain your project confidently
- Technical deep-dives
- Trade-off discussions

**Continue to**: [`phase-12-interview-prep.md`](./phase-12-interview-prep.md)
