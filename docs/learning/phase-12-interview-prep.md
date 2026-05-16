# Phase 12: Interview Preparation

**Goal**: Prepare polished explanations for technical interviews.

**Prerequisites**: Phase 0-11 (Complete mastery)

---

## The 30-Second Elevator Pitch

**Scenario**: "Tell me about your database project."

**Response**:

"I built EvolvDB, a Postgres-inspired SQL database from scratch in Java. It implements the complete stack from disk I/O to query optimization—including a storage engine with buffer pool caching, full SQL support with CRUD operations, a Volcano-style query optimizer with multiple join algorithms, and complete query execution. The architecture is modular and follows SOLID principles, with about 15,000 lines of well-tested code across 13 Gradle modules. It supports SELECT, INSERT, UPDATE, DELETE with WHERE clauses, JOINs, and GROUP BY aggregations. It's educational but uses production techniques like slotted pages, cost-based optimization, and hash joins."

**Key points**:
- ✅ Built from scratch (not using existing DB libraries)
- ✅ Complete vertical slice (storage to SQL)
- ✅ Modern techniques (Volcano optimizer, multiple join algorithms)
- ✅ Clean architecture (SOLID, modular)
- ✅ Substantial project (15K+ LOC)

---

## The 2-Minute Overview

**Scenario**: "Walk me through the architecture."

**Response**:

"EvolvDB has six main layers:

**Storage Foundation**: At the bottom, I have a DiskManager that handles page I/O using Java NIO, a BufferPool with LRU eviction that caches pages in memory, and a SlottedPageFormat for variable-length records. HeapFiles manage records across multiple pages.

**Type System**: Above that, I have a type system with schema enforcement—INT, VARCHAR, BOOLEAN, etc. Tuples are immutable typed rows, and RowCodec handles binary encoding in little-endian format. The catalog persists table metadata in an append-only log.

**SQL Frontend**: The parser uses recursive descent to build an AST from SQL text. A validator does catalog-aware semantic checking before planning.

**Query Planning**: The binder resolves names against the catalog, and the analyzer infers types. This produces a logical plan using relational algebra operators—Scan, Filter, Project, Join, Aggregate.

**Optimization**: I implemented a Volcano-style optimizer that applies logical transformations like predicate pushdown and projection pruning, then uses a cost model to choose between physical algorithms. For joins, it can pick nested loop, hash join, or sort-merge join based on estimated costs.

**Execution**: Physical operators follow the Volcano iterator model—open, next, close. Data flows tuple-at-a-time through the pipeline from table scans up through filters, projections, joins, and aggregates.

The whole system is modular with clear boundaries—each layer knows only its immediate dependencies, making it testable and extensible."

**Time**: ~90 seconds at normal pace

---

## Common Interview Questions

### Q1: "Why build a database from scratch?"

**Answer**:

"I wanted to deeply understand database internals rather than just use them. Reading about B+Trees or query optimizers is one thing, but implementing them teaches you the edge cases and trade-offs. For example, I learned why buffer pools need pin counts the hard way when my first version had race conditions. I also wanted to demonstrate systems programming skills—dealing with bytes, caching, concurrent access—which aren't visible in typical application development."

### Q2: "What was the hardest part?"

**Answer**:

"The query optimizer was the most challenging. It's easy to build a naive planner that just scans and filters, but a real optimizer needs to consider multiple join orders, different algorithms, and estimate costs without real statistics. 

I implemented a simplified Volcano optimizer that explores alternatives bottom-up. For example, given a three-table join, it considers different join orders and whether to use nested loops versus hash join for each. The cost model estimates row counts, CPU operations, and I/O to pick the best plan. Getting the cost formulas right was tricky—I initially underestimated hash join overhead and it always won, even when wrong.

The second hardest was the slotted page format with compaction. Managing variable-length records with deletions means fragmentation, and compaction has to update all slot offsets while the page is still pinned."

### Q3: "How does your query optimizer work?"

**Answer**:

"It's a Volcano-style optimizer with three phases:

**Phase 1 - Logical Rewriting**: I apply equivalence-preserving transformations like predicate pushdown—moving filters down through joins to reduce intermediate results—and projection pruning to eliminate unused columns early.

**Phase 2 - Physical Planning**: Bottom-up optimization. For each logical node, I generate physical alternatives. For joins, that's three algorithms: nested loop which is O(n×m), hash join which is O(n+m) but requires an equi-join, and sort-merge which is O(n log n) but produces sorted output.

**Phase 3 - Cost-Based Selection**: I use a cost model that estimates row counts, CPU operations, and I/O for each alternative. For example, hash join reads each side once (n + m pages) while nested loop reads the right side once per left tuple (n × m pages). The optimizer picks the lowest total cost.

It's simplified compared to production databases—I don't have real statistics yet, just defaults like '1000 rows per table' and '10% filter selectivity'—but it demonstrates the core ideas."

### Q4: "What design patterns did you use?"

**Answer**:

"Seven main patterns:

**Strategy** for pluggable algorithms—different eviction policies for the buffer pool, different page formats, different join algorithms. Each implements an interface so I can swap them without changing clients.

**Visitor** for tree traversal—AST validation, logical plan optimization, and expression evaluation all use visitors. This keeps operations separate from tree structure.

**Iterator** for the Volcano model—every operator implements open/next/close. This enables composable pipelines and lazy evaluation.

**Factory** for object creation—RecordManager creates HeapFiles, the physical planner creates operators from plans.

**Template Method** for operator lifecycle—the PhysicalOperator base class defines the execution pattern, subclasses implement specific behaviors.

**Facade** to hide complexity—the Database class is the single entry point that wires everything together; the Table class wraps HeapFile with typed APIs.

**Builder** for complex construction—Schema building with validation, AST node construction in the parser.

These aren't arbitrary—each solves a real problem in the architecture."

### Q5: "How would you add transactions?"

**Answer**:

"I'd add three components:

**TransactionManager** to handle begin/commit/abort. Each transaction gets a unique ID and tracks its read and write sets.

**LockManager** implementing two-phase locking. Transactions acquire shared locks for reads and exclusive locks for writes. The lock manager detects deadlocks using a wait-for graph and aborts the youngest transaction to resolve cycles.

**Undo Log** for rollback support. Before each modification, I'd log the old state. On abort, I replay the log backwards to restore the original state.

The tricky part is threading the transaction context through all operators. Every table access—scan, read, update—needs to acquire the appropriate lock. Insert and delete also need locks.

An alternative is MVCC—multi-version concurrency control—where readers see snapshots and don't block writers. That's more complex but has better concurrency. PostgreSQL uses MVCC, MySQL InnoDB uses MVCC, Oracle uses MVCC—it's the industry standard for read-heavy workloads."

### Q6: "What would you do differently if you rebuilt it?"

**Answer**:

"Three things:

**Start with NULL support**: I implemented it without NULLs, and now adding them requires updating every expression evaluator, every operator, and the storage encoding. If I'd designed for NULLs from day one, it would be easier.

**Vectorized execution**: The current Volcano model processes one tuple at a time. Modern databases use vectorized execution—processing batches of 1000 tuples—for better CPU cache locality and SIMD opportunities. The architecture is the same, just next() returns a batch instead of a single tuple.

**Better cost model integration**: Right now the optimizer uses hardcoded selectivity estimates. I should have designed for pluggable statistics from the start—histograms, distinct value counts, correlation statistics. The framework is there, but retrofitting real statistics will be awkward.

That said, the core architecture is solid. The layer separation, the use of interfaces, and the modular structure all held up well as the system grew."

### Q7: "How does this compare to PostgreSQL?"

**Answer**:

"PostgreSQL is a production database with 30+ years of development. EvolvDB is educational but uses similar techniques.

**Similarities**:
- Both use slotted pages for variable-length records
- Both use a Volcano iterator model for execution
- Both have multi-algorithm join support (nested loop, hash, merge)
- Both use cost-based optimization
- Both support full CRUD operations (SELECT, INSERT, UPDATE, DELETE)

**Key differences**:

PostgreSQL has MVCC with snapshot isolation—readers never block writers. EvolvDB would use simpler two-phase locking initially.

PostgreSQL has sophisticated statistics—histograms, distinct counts, correlation—feeding a mature cost model. EvolvDB uses default estimates.

PostgreSQL has B+Tree, Hash, GiST, GIN, and BRIN indexes. EvolvDB has no indexes yet, just sequential scans.

PostgreSQL has write-ahead logging with ARIES recovery. EvolvDB has no durability guarantees—crashes lose uncommitted data.

PostgreSQL handles concurrent workloads with thousands of connections. EvolvDB is currently single-threaded.

PostgreSQL supports NULLs, OUTER JOINs, subqueries, window functions. EvolvDB has basic SQL without NULLs yet.

The gap isn't architectural—EvolvDB is designed to support these features—it's implementation effort. I estimate about 2-2.5 months to add indexes, transactions, and WAL for a minimal production database."

### Q8: "Walk me through a query execution."

**Answer**:

"Let me trace `SELECT name FROM users WHERE age > 30`:

**Parsing**: The tokenizer breaks it into tokens—SELECT, name, FROM, users, WHERE, age, GT, 30. The parser builds an AST with a Select node containing select items, table references, and a WHERE expression.

**Validation**: The validator checks that the 'users' table exists in the catalog and that 'name' and 'age' are valid columns.

**Planning**: The binder resolves column names to their types—'age' is INT, 'name' is VARCHAR. The analyzer builds a logical plan: Project(name) over Filter(age > 30) over Scan(users).

**Optimization**: The logical rewriter looks for optimizations but finds none—filter is already pushed down. The Volcano optimizer generates physical alternatives—only SeqScan for the scan, FilterPlan for the filter, ProjectPlan for the projection—and picks them (no alternatives in this simple case).

**Execution**: We create the operator tree and call open() which cascades down to the table scan. Then we repeatedly call next():

First call: SeqScan reads the first record from the HeapFile. BufferPool has a cache miss, so it reads page 0 from disk via DiskManager. RowCodec decodes the bytes to a Tuple. FilterExec evaluates age > 30—it's 35, so true. ProjectExec extracts the name field. Returns 'Alice'.

Second call: SeqScan reads the second record. Page is cached now, so no disk I/O. Tuple is (Bob, 25). FilterExec evaluates 25 > 30—false, so it loops back to get another tuple. Third record is (Charlie, 40). Filter passes, project returns 'Charlie'.

Third call: Iterator exhausted, returns null. We call close() which unpins the page from the buffer pool.

The key insight is the pull-based model—each operator pulls from its child only when needed, and filtering happens before projection, so Bob's tuple never reaches the project operator."

---

## Technical Deep-Dive Questions

### Q: "Explain slotted pages in detail."

**Answer**:

"Slotted pages solve the variable-length record problem. You have a fixed-size page—say 4KB—and need to store variable-length records efficiently.

**Layout**: The page has three regions:

A fixed header at byte 0: page type (4 bytes), LSN for recovery (4 bytes), slot count (2 bytes), and free space start offset (2 bytes). That's 12 bytes.

The payload region grows upward from byte 12. Each record is written contiguously—no padding, no alignment. When you insert, you append to the free space start and increment that offset.

The slot directory grows downward from the page end. Each slot is 4 bytes: offset (2 bytes) and length (2 bytes). Slot 0 is at page_end - 4, slot 1 at page_end - 8, and so on.

**RecordId**: You address records by PageId + slot index. This is stable—even if the record moves during compaction, the slot index stays the same. You just update the offset in the slot entry.

**Deletion**: You mark a slot as deleted by negating its length. The payload bytes aren't reclaimed immediately, causing fragmentation.

**Compaction**: When free space is insufficient but total live bytes would fit, you compact: scan all live slots, copy payloads to a contiguous region starting at byte 12, update slot offsets, and reset free space start. This is O(records) and blocks the page.

**Free space calculation**: It's the gap between free space start and where the slot directory would grow to. Specifically: `page_size - free_start - (slot_count * 4)`.

The beauty is that it handles arbitrary-length records, maintains stable identifiers, and keeps deletions cheap at the cost of eventual compaction."

### Q: "How does the buffer pool handle concurrency?"

**Answer**:

"Currently, it doesn't—it's single-threaded with coarse-grained synchronization. All methods are synchronized, so only one thread can access the buffer pool at a time. This is safe but terrible for concurrency.

For production, I'd do this:

**Page-level latches**: Each frame has a read-write latch. Multiple threads can read the same page concurrently (shared latch), but writes are exclusive. This is separate from transaction-level locks—latches protect physical structures, locks protect logical data.

**Frame table latch**: A single latch protects the frames map itself. You grab it to look up a page, increment the pin count, then release it. The page is now safe to access under its own latch.

**Eviction**: Trickier because you need to scan the frames map for victims. You'd hold the frame table latch briefly to find candidates, then release it and try to latch individual pages. If a page can't be latched, skip it.

**Deadlock-free order**: Always acquire latches in a consistent order—frame table first, then page latches by PageId order—to prevent deadlocks between buffer pool operations.

PostgreSQL uses this approach. MySQL InnoDB has a similar design. The key insight is separating latching (short-term, physical) from locking (long-term, logical)."

### Q: "Why Volcano model instead of push-based or vectorized?"

**Answer**:

"Volcano is pull-based—parents pull from children via next() calls. This has pros and cons:

**Pros**:
- Simple to implement and understand
- Composable—any operator can be a child of any other
- Lazy evaluation—if client stops early, remaining work is skipped
- Low memory—only one tuple in flight for pipelined operators

**Cons**:
- Function call overhead—millions of next() calls for large scans
- Poor CPU cache locality—jumping between operators causes cache misses
- Can't leverage SIMD—no vectors to vectorize over

**Push-based** inverts control—parents push data to children. This is what Spark does. Benefit: fewer function calls. Downside: harder to implement blocking operators like joins.

**Vectorized** processes batches—next() returns 1000 tuples, not one. MonetDB and VectorWise do this. Benefit: better CPU cache locality, SIMD opportunities, amortized function call overhead. Downside: more memory, operators need to handle batches.

**Compiled** generates code for each query—JIT compiles the entire pipeline into a tight loop. HyPer and MemSQL do this. Benefit: optimal CPU usage, no virtual calls. Downside: compilation overhead, complex implementation.

I chose Volcano because it's simple, proven, and pedagogically clear. For production scale, I'd explore vectorization—it offers most benefits of compilation with less complexity."

---

## Handling Tough Questions

### Q: "This seems like a toy project. Why should we care?"

**Response**:

"Fair question. Let me clarify what makes this non-trivial:

It's not using an embedded database or key-value store as a shortcut—I implemented the storage engine from scratch, including page management, caching, and variable-length record handling.

It's not just INSERT and SELECT—full CRUD operations are implemented with WHERE clauses, so you can actually use it like a real database.

The query optimizer isn't a simple rule applier—it explores multiple physical plans, estimates costs, and picks the best. I implemented three different join algorithms with different algorithmic complexities.

The architecture follows real database design patterns—slotted pages from PostgreSQL/MySQL, Volcano model from academic research, cost-based optimization from commercial databases.

It's substantive—about 15,000 lines of code with comprehensive tests. Each module has unit tests, integration tests, and end-to-end query tests.

What it demonstrates is systems thinking—understanding trade-offs between algorithms, managing memory and I/O carefully, designing for extensibility. These skills transfer to any systems programming role.

That said, it's not production-ready—no transactions, no WAL, no indexes. But it's a complete vertical slice that implements the core ideas, and the architecture is designed to support those features."

### Q: "What bugs did you encounter?"

**Response**:

"Five memorable ones that each taught me something fundamental:

**Volcano pipeline bypass (UPDATE/DELETE)**: My original UPDATE and DELETE operators accepted a child operator but never called it. Instead, they extracted the WHERE filter from the logical plan and rescanned the table directly. This meant the entire Volcano pull-based model was broken for DML—predicate pushdown, optimizer decisions, all ignored. I fixed it by introducing `SeqScanWithRidExec` (a scan that tracks RecordIds) and a `lastRecordId()` method on the operator interface that propagates through intermediate operators like FilterExec. Now UPDATE/DELETE fully participate in the Volcano pipeline.

**Exception swallowing in HeapFile**: HeapFile.insert() caught `IllegalStateException` and treated it as 'no space on this page.' Any real bug in the page format code would be silently swallowed and retried on the next page. I introduced a domain-specific `PageFullException` so only genuine 'page full' conditions are caught. This is a textbook example of why catch-all is dangerous.

**Aggregate type shadowing**: A `SumAgg` inner class had a field named `f` (for 'isFloat'). After Google Java Format reformatted the code, the `instanceof Float` pattern match interacted with the single-letter field name in a way that caused the aggregate to always return Float instead of Long. Renaming to `isFloat`, `floatSum`, `intSum` fixed it. Lesson: meaningful variable names aren't just style—they prevent subtle bugs.

**NULL parsed as column reference**: The parser didn't recognize `NULL` as a keyword. `UPDATE t SET col = NULL` would parse `NULL` as a column reference named 'NULL', causing an 'unknown column' error instead of a 'null value' assignment. Adding NULL to the tokenizer keyword map and parser fixed it immediately.

**No unified API**: The full SQL pipeline existed and worked in tests, but there was no single entry point. The CLI just inserted hardcoded data. I added `Database.execute(String sql)` that wires parser → validator → analyzer → planner → executor, plus an interactive REPL. This is the Facade pattern in action."

---

## Answering "How Would You..." Questions

**Framework**:
1. Acknowledge the question
2. State the high-level approach
3. Identify key challenges
4. Discuss trade-offs
5. Mention what you'd need to learn

### Example: "How would you add parallelism?"

"Great question. There are two levels:

**Intra-query parallelism**: Parallelize a single query across cores. I'd add an Exchange operator that partitions data across workers. For example, a parallel hash join would partition both sides by join key, then each worker independently joins its partition. Challenge: load balancing—if keys are skewed, some workers do more work. Trade-off: partitioning overhead versus parallelism benefit.

**Inter-query parallelism**: Multiple queries concurrently. This requires fine-grained locking in the buffer pool and catalog, plus a connection manager to handle multiple clients. Challenge: contention on shared structures like the buffer pool frame table. Trade-off: locking overhead versus throughput.

I'd start with intra-query for analytics workloads (large scans benefit most), then add inter-query for OLTP workloads (many small queries). I'd study PostgreSQL's parallel query execution and the Morsel-Driven paper from HyPer to understand best practices."

---

## Project Showcase Tips

### Prepare a Demo

**1-minute demo script using the interactive REPL** (`evolvdb-cli`):
```
1. Launch the REPL (./gradlew :evolvdb-cli:run -q --console=plain)
2. CREATE TABLE users (id INT, name STRING, age INT);
3. INSERT INTO users VALUES (1, 'Alice', 35), (2, 'Bob', 25), (3, 'Charlie', 40);
4. SELECT name, age FROM users WHERE age > 30;
   → Explain: parser → validator → binder → planner → SeqScan → Filter → Project
5. Multi-table JOIN with WHERE → explain optimizer's join algorithm choice
6. SELECT region, COUNT(*), SUM(amount) FROM sales GROUP BY region;
   → Explain hash aggregation
7. UPDATE users SET age = age + 1 WHERE name = 'Alice';
   → Explain Volcano pipeline with SeqScanWithRidExec → FilterExec → UpdateExec
8. DROP TABLE users;
```

### Have Metrics Ready

- **Lines of code**: ~15,000 across 13 modules
- **Test coverage**: 85%+ for core modules
- **Query performance**: "Simple filter query in ~10ms, 2-table join in ~50ms"
- **Module count**: 13 Gradle modules with clear dependencies

### Know Your Weaknesses

**Be honest**:
- "No indexes yet—all queries scan entire tables"
- "No transactions—concurrent access is unsafe"
- "NULL is parsed but not yet supported in storage/types (M13)"
- "Single-threaded execution"

**But frame positively**:
- "The architecture supports indexes—I designed PageFormat as an interface"
- "I have a plan for 2PL transactions with undo logs"
- "NULL support is M13 with a clear design"

---

## Summary: Interview Confidence

### What You Can Confidently Claim

✅ "I built a database from scratch"  
✅ "I understand query optimization deeply"  
✅ "I know systems programming—bytes, caching, I/O"  
✅ "I can design extensible architectures"  
✅ "I follow SOLID principles and design patterns"  
✅ "I write clean, tested code"  

### What to Avoid Claiming

❌ "It's production-ready" (it's not)  
❌ "It's as good as PostgreSQL" (it's not)  
❌ "It's novel research" (it's not)  

### The Right Positioning

**"EvolvDB is an educational database that demonstrates real systems programming skills and deep understanding of database internals. It implements production techniques at smaller scale and has a clean architecture designed for extensibility."**

---

## Next Steps

You're now ready to ace database system design interviews. In **Phase 13**, we'll provide a final summary:
- Complete mental model
- Quick reference guide
- Learning checklist
- Concept dependency graph

**Continue to**: [`phase-13-summary.md`](./phase-13-summary.md)
