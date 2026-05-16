# Phase 8: End-to-End Query Walkthrough

**Goal**: Trace a complete query execution from SQL to results, seeing every component in action.

**Prerequisites**: Phase 0-7 (All previous phases)

---

## The Query

We'll trace this query through the entire system:

```sql
SELECT name FROM users WHERE age > 30;
```

**Setup**:
```sql
CREATE TABLE users (id INT, name VARCHAR(50), age INT);
INSERT INTO users VALUES (1, 'Alice', 35), (2, 'Bob', 25), (3, 'Charlie', 40);
```

**Expected result**:
```
name
-------
Alice
Charlie
```

---

## Phase 1: SQL Parsing (M8)

### Input
```
"SELECT name FROM users WHERE age > 30"
```

### Tokenization

**Tokenizer** breaks into tokens:

```java
Token(SELECT, "SELECT", 0)
Token(IDENTIFIER, "name", 7)
Token(FROM, "FROM", 12)
Token(IDENTIFIER, "users", 17)
Token(WHERE, "WHERE", 23)
Token(IDENTIFIER, "age", 29)
Token(GT, ">", 33)
Token(NUMBER, "30", 35)
Token(EOF, "", 37)
```

**Module**: `evolvdb-sql`  
**Class**: `io.github.anupam.evolvdb.sql.parser.Tokenizer`

### Parsing

**Parser** builds AST:

```java
Select {
  selectItems: [
    SelectItem(
      expr: ColumnRef(qualifier=null, name="name"),
      alias: null
    )
  ],
  tables: [
    TableRef(name="users", alias=null)
  ],
  where: ComparisonExpr(
    left: ColumnRef(qualifier=null, name="age"),
    op: GT,
    right: Literal(30, Type.INT)
  )
}
```

**Module**: `evolvdb-sql`  
**Class**: `io.github.anupam.evolvdb.sql.parser.SqlParser`

**Key method**: `parseSelectStmt()`

### Validation

**AstValidator** checks:

1. ✅ Table "users" exists in catalog
2. ✅ Column "name" exists in users schema
3. ✅ Column "age" exists in users schema
4. ✅ Comparison type compatible (INT > INT)

**Module**: `evolvdb-sql`  
**Class**: `io.github.anupam.evolvdb.sql.validate.AstValidator`

---

## Phase 2: Logical Planning (M9)

### Binding (Name Resolution)

**Binder** resolves names:

```java
// Look up table in catalog
TableMeta usersMeta = catalog.getTable("users").get();
Schema usersSchema = usersMeta.schema();
// Schema: (id:INT, name:VARCHAR(50), age:INT)

// Resolve column "name" → users.name (VARCHAR)
// Resolve column "age" → users.age (INT)
```

**Module**: `evolvdb-planner`  
**Class**: `io.github.anupam.evolvdb.planner.analyzer.Binder`

### Type Inference

**Analyzer** infers expression types:

```java
// ColumnRef("age") → Type.INT (from schema)
// Literal(30) → Type.INT
// ComparisonExpr(INT > INT) → Type.BOOLEAN ✅
```

**Module**: `evolvdb-planner`  
**Class**: `io.github.anupam.evolvdb.planner.analyzer.Analyzer`

### Initial Logical Plan

```
LogicalProject([name])
  ↓
LogicalFilter(age > 30)
  ↓
LogicalScan(users)
```

**Output schemas**:
- Scan: (id:INT, name:VARCHAR, age:INT)
- Filter: (id:INT, name:VARCHAR, age:INT) - same as child
- Project: (name:VARCHAR)

---

## Phase 3: Logical Optimization (M9)

### Rule Engine

**Attempt PushProjectBelowFilter**:
- Check: Does filter reference columns removed by projection?
- Filter uses "age", projection keeps only "name" → **Not safe**
- Rule doesn't apply

**Attempt RemoveRedundantProject**:
- Check: Is projection identity (all columns, same order)?
- No, projection selects only "name" → **Not redundant**
- Rule doesn't apply

**Attempt PredicateSimplification**:
- Check: Can predicate be simplified?
- "age > 30" is already simple → **No changes**

**Result**: Logical plan unchanged

**Module**: `evolvdb-planner`  
**Package**: `io.github.anupam.evolvdb.planner.rules`

---

## Phase 4: Physical Planning (M10-M11)

### Logical Rewriter (Pre-Optimization)

No multi-table joins, so:
- No predicate pushdown across joins
- No join reordering

**Plan remains**:
```
LogicalProject([name])
  ↓
LogicalFilter(age > 30)
  ↓
LogicalScan(users)
```

**Module**: `evolvdb-exec`  
**Class**: `io.github.anupam.evolvdb.exec.optimizer.LogicalRewriter`

### Volcano Optimizer

**Bottom-up optimization**:

#### 1. Optimize LogicalScan

**Generate alternatives**:
- SeqScanPlan(users)

**Cost estimate**:
```
Rows: 3 (actual, but optimizer assumes 1000 without stats)
CPU: 3 × CPU_PER_TUPLE
I/O: 1 page (table fits in one page)
```

**Winner**: SeqScanPlan (only option)

#### 2. Optimize LogicalFilter

**Child**: SeqScanPlan(users) - 3 rows

**Generate alternatives**:
- FilterPlan(predicate: age > 30, child: SeqScanPlan)

**Cost estimate**:
```
Rows: 3 × 0.1 = 0.3 (assumes 10% selectivity)
CPU: child.cpu + (3 × CPU_PER_PREDICATE)
I/O: child.io (same, no additional I/O)
```

**Winner**: FilterPlan (only option)

#### 3. Optimize LogicalProject

**Child**: FilterPlan - 0.3 rows (estimated)

**Generate alternatives**:
- ProjectPlan(exprs: [name], child: FilterPlan)

**Cost estimate**:
```
Rows: 0.3 (same as child)
CPU: child.cpu + (0.3 × 1 × CPU_PER_EXPR)
I/O: child.io (same)
```

**Winner**: ProjectPlan (only option)

### Final Physical Plan

```
ProjectPlan([name])
  ↓
FilterPlan(age > 30)
  ↓
SeqScanPlan(users)
```

**Module**: `evolvdb-exec`  
**Class**: `io.github.anupam.evolvdb.exec.optimizer.VolcanoOptimizer`

---

## Phase 5: Operator Creation

### Build Operator Tree

**PhysicalPlan.create(execContext)** builds operators:

```java
ExecContext ctx = new ExecContext(catalogManager);

// Create SeqScanExec
Table usersTable = catalog.openTable("users");
SeqScanExec scanOp = new SeqScanExec(usersTable, "users", null);

// Create FilterExec
Expr predicate = ...; // age > 30
FilterExec filterOp = new FilterExec(scanOp, predicate, ctx);

// Create ProjectExec
List<Expr> projections = List.of(ColumnRef("name"));
Schema outputSchema = new Schema(List.of(new ColumnMeta("name", Type.VARCHAR, 50)));
ProjectExec projectOp = new ProjectExec(filterOp, projections, outputSchema, ctx);
```

**Module**: `evolvdb-exec`  
**Package**: `io.github.anupam.evolvdb.exec.op`

---

## Phase 6: Execution (Volcano Model)

### Initialization: open()

```java
projectOp.open();
  → filterOp.open();
    → scanOp.open();
      → iterator = usersTable.scanTuples().iterator();
```

**What happens**:
1. Opens users table (HeapFile)
2. Creates iterator over all records
3. Prepares operator tree for execution

### First Tuple: next()

```java
Tuple result1 = projectOp.next();
```

**Execution trace**:

```
1. ProjectExec.next() called

2. Calls FilterExec.next()
   
3. FilterExec enters loop:
   - Calls SeqScanExec.next()
   
4. SeqScanExec.next():
   - Reads first record from iterator
   - HeapFile.scan() → iterator.next()
   - Pins page 0 via BufferPool
   - PageFormat.read(page, slot=0)
   - RowCodec.decode(bytes) → Tuple(1, "Alice", 35)
   - Returns Tuple(1, "Alice", 35)
   
5. FilterExec has tuple (1, "Alice", 35):
   - ExprEvaluator.eval(age > 30, tuple)
   - Extract age: tuple.get("age") → 35
   - Evaluate: 35 > 30 → true ✅
   - Returns Tuple(1, "Alice", 35)
   
6. ProjectExec has tuple (1, "Alice", 35):
   - ExprEvaluator.eval(name, tuple)
   - Extract name: tuple.get("name") → "Alice"
   - Build new tuple: Tuple("Alice")
   - Returns Tuple("Alice")
```

**Storage layer calls**:
```
BufferPool.getPage(PageId(users, 0), forUpdate=false)
  → Cache miss
  → DiskManager.readPage(PageId(users, 0), buffer)
    → FileChannel.read(buffer, offset=0)
  → Return Page
```

**Result**: `Tuple("Alice")`

### Second Tuple: next()

```java
Tuple result2 = projectOp.next();
```

**Execution trace**:

```
1. ProjectExec.next() called

2. Calls FilterExec.next()
   
3. FilterExec enters loop:
   - Calls SeqScanExec.next()
   
4. SeqScanExec.next():
   - Reads second record from iterator
   - Page 0 already pinned/cached
   - PageFormat.read(page, slot=1)
   - RowCodec.decode(bytes) → Tuple(2, "Bob", 25)
   - Returns Tuple(2, "Bob", 25)
   
5. FilterExec has tuple (2, "Bob", 25):
   - ExprEvaluator.eval(age > 30, tuple)
   - Extract age: tuple.get("age") → 25
   - Evaluate: 25 > 30 → false ❌
   - **Loops back to step 3** (no return)
   
3. FilterExec continues loop:
   - Calls SeqScanExec.next() again
   
4. SeqScanExec.next():
   - Reads third record from iterator
   - PageFormat.read(page, slot=2)
   - RowCodec.decode(bytes) → Tuple(3, "Charlie", 40)
   - Returns Tuple(3, "Charlie", 40)
   
5. FilterExec has tuple (3, "Charlie", 40):
   - ExprEvaluator.eval(age > 30, tuple)
   - Extract age: tuple.get("age") → 40
   - Evaluate: 40 > 30 → true ✅
   - Returns Tuple(3, "Charlie", 40)
   
6. ProjectExec has tuple (3, "Charlie", 40):
   - ExprEvaluator.eval(name, tuple)
   - Extract name: tuple.get("name") → "Charlie"
   - Build new tuple: Tuple("Charlie")
   - Returns Tuple("Charlie")
```

**Key insight**: Bob's tuple was **filtered out** without bubbling up to ProjectExec.

**Result**: `Tuple("Charlie")`

### Third Tuple: next()

```java
Tuple result3 = projectOp.next();
```

**Execution trace**:

```
1. ProjectExec.next() called

2. Calls FilterExec.next()
   
3. FilterExec enters loop:
   - Calls SeqScanExec.next()
   
4. SeqScanExec.next():
   - Iterator exhausted (no more records)
   - Returns null
   
5. FilterExec receives null:
   - Returns null (propagate exhaustion)
   
6. ProjectExec receives null:
   - Returns null (query done)
```

**Result**: `null` (no more tuples)

### Cleanup: close()

```java
projectOp.close();
  → filterOp.close();
    → scanOp.close();
      → BufferPool.unpin(PageId(users, 0), dirty=false)
```

**What happens**:
1. Unpins page from BufferPool (can be evicted now)
2. Releases any resources
3. Operator tree ready for garbage collection

---

## Phase 7: Storage Layer Detail

### HeapFile.scan() Implementation

**Module**: `evolvdb-storage-record`  
**Class**: `io.github.anupam.evolvdb.storage.record.HeapFile`

```java
class HeapFileIterator implements Iterator<Tuple> {
    int currentPageNo = 0;
    int currentSlot = 0;
    int totalPages;
    
    Tuple next() {
        while (currentPageNo < totalPages) {
            PageId pageId = new PageId(fileId, currentPageNo);
            Page page = bufferPool.getPage(pageId, false);
            
            try {
                int slotCount = pageFormat.slotCount(page);
                while (currentSlot < slotCount) {
                    if (pageFormat.isLive(page, currentSlot)) {
                        byte[] bytes = pageFormat.read(page, recordId);
                        Tuple tuple = RowCodec.decode(schema, bytes);
                        currentSlot++;
                        return tuple;
                    }
                    currentSlot++;  // Skip tombstone
                }
            } finally {
                bufferPool.unpin(pageId, false);
            }
            
            currentPageNo++;
            currentSlot = 0;
        }
        return null;  // Exhausted
    }
}
```

**Key points**:
1. Iterates pages sequentially (0 to totalPages-1)
2. Within each page, iterates slots (0 to slotCount-1)
3. Skips tombstones (deleted records)
4. Pins page, reads record, unpins page
5. BufferPool caches pages for subsequent reads

### BufferPool Interaction

**First page read** (cache miss):
```
1. bufferPool.getPage(PageId(users, 0), false)
2. Check frames map: not present
3. Allocate new frame (pool not full, no eviction needed)
4. diskManager.readPage(PageId(users, 0), buffer)
5. Store in frames map
6. Set pinCount = 1
7. Return Page
```

**Second read** (cache hit):
```
1. bufferPool.getPage(PageId(users, 0), false)
2. Check frames map: present ✅
3. Increment pinCount (now 2)
4. Update LRU (mark as recently used)
5. Return cached Page
```

**Unpin**:
```
1. bufferPool.unpin(PageId(users, 0), false)
2. Decrement pinCount (now 1)
3. Keep in cache (pinCount > 0)
```

### DiskManager.readPage()

**Module**: `evolvdb-storage-disk`  
**Class**: `io.github.anupam.evolvdb.storage.disk.NioDiskManager`

```java
void readPage(PageId pageId, ByteBuffer dst) {
    FileChannel channel = getOrOpenChannel(pageId.fileId());
    long offset = pageId.pageNo() * pageSize;
    
    synchronized (channel) {
        channel.position(offset);
        int bytesRead = channel.read(dst);
        if (bytesRead != pageSize) {
            throw new IOException("Incomplete read");
        }
    }
}
```

**What happens**:
1. Open file channel for "users.evolv"
2. Calculate byte offset: page 0 = offset 0
3. Position channel to offset
4. Read exactly 4096 bytes into buffer
5. Return (buffer now contains page data)

---

## Complete Execution Timeline

### Millisecond-by-millisecond (hypothetical)

```
T=0ms: Query arrives: "SELECT name FROM users WHERE age > 30"

T=1ms: Tokenization complete (9 tokens)

T=2ms: Parsing complete (AST built)

T=3ms: Validation complete (catalog checks passed)

T=4ms: Binding complete (names resolved)

T=5ms: Logical plan built

T=6ms: Logical optimization complete (no changes)

T=7ms: Physical optimization complete (SeqScan → Filter → Project)

T=8ms: Operator tree created

T=9ms: open() called, HeapFile iterator initialized

T=10ms: First next() called
  → BufferPool cache miss
  → DiskManager reads page 0 (5ms disk I/O)

T=15ms: Page in memory, decode Tuple(1, "Alice", 35)
  → Filter evaluates: 35 > 30 = true
  → Project extracts: "Alice"
  → Returns Tuple("Alice")

T=16ms: Second next() called
  → BufferPool cache hit (fast!)
  → Decode Tuple(2, "Bob", 25)
  → Filter evaluates: 25 > 30 = false
  → Loop continues
  → Decode Tuple(3, "Charlie", 40)
  → Filter evaluates: 40 > 30 = true
  → Project extracts: "Charlie"
  → Returns Tuple("Charlie")

T=17ms: Third next() called
  → Iterator exhausted
  → Returns null

T=18ms: close() called, page unpinned

T=19ms: Results returned to client

Total: ~19ms
```

**Breakdown**:
- Parsing/Planning: ~9ms (CPU-bound)
- Disk I/O: ~5ms (one page read)
- Execution: ~5ms (CPU-bound)

**Note**: With hot buffer pool (page already cached), total time drops to ~14ms.

---

## Layers Involved (Summary)

### All Modules Used

1. **evolvdb-sql**: Parsing, AST, validation
2. **evolvdb-planner**: Binding, logical plans, rules
3. **evolvdb-exec**: Optimizer, physical operators, expression evaluation
4. **evolvdb-catalog**: CatalogManager, TableMeta, Table
5. **evolvdb-types**: Schema, Tuple, RowCodec
6. **evolvdb-storage-record**: HeapFile, RecordManager
7. **evolvdb-storage-buffer**: BufferPool, LruEvictionPolicy
8. **evolvdb-storage-page**: SlottedPageFormat
9. **evolvdb-storage-disk**: NioDiskManager
10. **evolvdb-config**: DbConfig

**Every single component** of EvolvDB participates in this query!

---

## Key Insights

### Pull-Based Execution

**Higher layers pull from lower layers**:
- Client pulls from ProjectExec
- ProjectExec pulls from FilterExec
- FilterExec pulls from SeqScanExec
- SeqScanExec pulls from Table/HeapFile
- HeapFile pulls from BufferPool
- BufferPool pulls from DiskManager

**Data flows upward**, **control flows downward**.

### Tuple-at-a-Time

Only **one tuple in memory** at a time (for pipelined operators).

**Exception**: Blocking operators (joins, aggregates) materialize intermediate results.

### Lazy Evaluation

FilterExec doesn't pre-filter all tuples. It filters **on-demand** as ProjectExec requests them.

**Benefit**: If client stops early (e.g., LIMIT), work is saved.

### Layer Independence

Each layer knows only its immediate dependencies:
- Operators don't know about pages
- HeapFile doesn't know about SQL
- BufferPool doesn't know about tuples

**Benefit**: Can change implementations independently (e.g., add columnar storage without changing operators).

---

## Performance Analysis

### What Took Time?

**Disk I/O**: 5ms (reading page 0)
- Most expensive operation
- Would be worse for multi-page tables

**CPU work**: ~9ms total
- Parsing: ~2ms
- Planning/optimization: ~5ms
- Execution: ~2ms

### Where Could We Optimize?

**Add index on age** (M16):
```
IndexScan(age > 30) instead of SeqScan + Filter
```
**Benefit**: Only read matching rows, skip Bob's record entirely

**Cache plan** (not implemented):
- Skip parsing/planning for repeated queries
- ~9ms saved per execution

**Vectorized execution** (not implemented):
- Process batches of tuples, not one-at-a-time
- Better CPU cache utilization

---

## Self-Test Questions

1. **At what point does the query actually touch disk?**
   - During SeqScanExec.next() when BufferPool has cache miss

2. **Why wasn't Bob's tuple returned?**
   - FilterExec evaluated age (25) > 30 = false, looped to next tuple

3. **How many times was page 0 pinned?**
   - Multiple times (once per tuple read), but BufferPool cached after first read

4. **What would happen if we removed the WHERE clause?**
   - FilterExec wouldn't exist, all 3 tuples returned

5. **What would happen if users table had 1 million rows?**
   - Same process, but many more iterations and pages read

6. **Why is the plan built bottom-up but executed top-down?**
   - Planning: Optimize children first (cost depends on children)
   - Execution: Parent pulls from child (Volcano model)

---

## Next Steps

You've seen a complete query execution. In **Phase 9-10**, we'll examine the architecture:
- High-Level Design: System components and boundaries
- Low-Level Design: Key classes and patterns

**Continue to**: [`phase-9-hld.md`](./phase-9-hld.md)
