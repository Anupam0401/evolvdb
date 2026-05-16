# Phase 13: Mental Models & Summary

**Goal**: Consolidate everything into a clear mental model and quick reference.

**Prerequisites**: Phase 0-12 (Complete mastery)

---

## The Complete Mental Model

### Layer View (Bottom to Top)

```
┌─────────────────────────────────────────────┐
│  Layer 6: Query Results                     │  ← User sees tuples
│  (Tuples returned via iterator)             │
└─────────────────────────────────────────────┘
                   ↑
┌─────────────────────────────────────────────┐
│  Layer 5: Query Execution (M10)             │
│  Physical Operators (Volcano Model)         │
│  • SeqScan, Filter, Project                 │
│  • Join (NLJ, Hash, SortMerge)              │
│  • Aggregate, Insert                        │
└─────────────────────────────────────────────┘
                   ↑
┌─────────────────────────────────────────────┐
│  Layer 4: Query Optimization (M11)          │
│  • Logical Rewriter (pushdowns, pruning)    │
│  • Volcano Optimizer (cost-based selection) │
│  • Cost Model (estimate rows, CPU, I/O)     │
└─────────────────────────────────────────────┘
                   ↑
┌─────────────────────────────────────────────┐
│  Layer 3: Query Planning (M9)               │
│  • Binder (name resolution)                 │
│  • Analyzer (type inference)                │
│  • Logical Plans (Scan, Filter, Join, etc.) │
└─────────────────────────────────────────────┘
                   ↑
┌─────────────────────────────────────────────┐
│  Layer 2: SQL Frontend (M8)                 │
│  • Tokenizer (lexical analysis)             │
│  • Parser (syntax analysis → AST)           │
│  • Validator (semantic checks)              │
└─────────────────────────────────────────────┘
                   ↑
┌─────────────────────────────────────────────┐
│  Layer 1.5: Type System & Metadata (M6-M7)  │
│  • Types (INT, VARCHAR, etc.)               │
│  • Schema (columns + validation)            │
│  • Tuple (typed rows)                       │
│  • RowCodec (binary encoding)               │
│  • Catalog (persistent metadata)            │
│  • Table (HeapFile wrapper)                 │
└─────────────────────────────────────────────┘
                   ↑
┌─────────────────────────────────────────────┐
│  Layer 1: Storage Engine (M1-M5)            │
│  • HeapFile (multi-page records)            │
│  • BufferPool (LRU caching)                 │
│  • PageFormat (slotted pages)               │
│  • DiskManager (page I/O)                   │
└─────────────────────────────────────────────┘
                   ↑
┌─────────────────────────────────────────────┐
│  Layer 0: Physical Files                    │
│  (.evolv files on disk)                     │
└─────────────────────────────────────────────┘
```

---

## Concept Dependency Graph

**Read bottom-to-top**: Each concept depends on concepts below it.

```
                         [SQL Query Execution]
                                 |
              ┌──────────────────┼──────────────────┐
              ↓                  ↓                  ↓
      [Optimization]       [Physical Ops]    [Expression Eval]
              |                  |                  |
              └──────────────────┼──────────────────┘
                                 ↓
                        [Logical Planning]
                                 |
                        [Binder & Analyzer]
                                 |
                          [AST & Parsing]
                                 |
              ┌──────────────────┼──────────────────┐
              ↓                  ↓                  ↓
         [Catalog]            [Tuple]          [Schema/Types]
              |                  |                  |
              └──────────────────┼──────────────────┘
                                 ↓
                            [RowCodec]
                                 |
                            [HeapFile]
                                 |
              ┌──────────────────┼──────────────────┐
              ↓                  ↓                  ↓
      [BufferPool]         [PageFormat]      [RecordId]
              |                  |                  |
              └──────────────────┼──────────────────┘
                                 ↓
                          [DiskManager]
                                 |
                          [Page & PageId]
                                 |
                            [FileId]
```

---

## The One-Page Cheat Sheet

### Core Data Structures

| Structure | Purpose | Size |
|-----------|---------|------|
| **FileId** | Logical file name | String |
| **PageId** | Physical page address | FileId + int |
| **RecordId** | Record location | PageId + short |
| **Page** | 4KB buffer | ByteBuffer (4096 bytes) |
| **Frame** | Cached page | Page + metadata |
| **Tuple** | Typed row | Schema + values |
| **Schema** | Table structure | List<ColumnMeta> |

### Key Algorithms

| Algorithm | Complexity | Use Case |
|-----------|------------|----------|
| **LRU Eviction** | O(1) amortized | Buffer pool |
| **Slotted Page Insert** | O(1) or O(n) | Variable records |
| **SeqScan** | O(n) | Full table scan |
| **Filter** | O(n) | WHERE clause |
| **Nested Loop Join** | O(n×m) | Any join |
| **Hash Join** | O(n+m) | Equi-join |
| **Sort-Merge Join** | O(n log n) | Sorted join |
| **Hash Aggregate** | O(n) | GROUP BY |

### Design Patterns

| Pattern | Examples | Purpose |
|---------|----------|---------|
| **Strategy** | EvictionPolicy, PageFormat | Pluggable algorithms |
| **Visitor** | AST, LogicalPlan traversal | Tree operations |
| **Iterator** | Volcano operators | Sequential access |
| **Factory** | RecordManager, PhysicalPlan | Object creation |
| **Template** | PhysicalOperator lifecycle | Algorithm skeleton |
| **Facade** | Database, Table | Simplify interface |
| **Builder** | Schema, AST construction | Complex objects |

### Module Map

| Module | Responsibility | Lines |
|--------|----------------|-------|
| `evolvdb-storage-disk` | Page I/O | ~500 |
| `evolvdb-storage-buffer` | Caching | ~800 |
| `evolvdb-storage-page` | Page layout | ~600 |
| `evolvdb-storage-record` | HeapFile | ~900 |
| `evolvdb-types` | Types & Tuple | ~1200 |
| `evolvdb-catalog` | Metadata | ~1500 |
| `evolvdb-sql` | Parser & AST (incl UPDATE/DELETE) | ~2500 |
| `evolvdb-planner` | Logical plans | ~2000 |
| `evolvdb-exec` | Execution & optimizer | ~4200 |
| `evolvdb-core` | Database facade | ~300 |
| `evolvdb-cli` | CLI | ~400 |
| **Total** | | **~15,900** |

---

## Query Execution Checklist

When tracing a query, follow these steps:

### 1. Parsing Phase
- [ ] Tokenize SQL string
- [ ] Build AST via recursive descent
- [ ] Validate against catalog

### 2. Planning Phase
- [ ] Bind names to catalog objects
- [ ] Infer expression types
- [ ] Build logical plan tree
- [ ] Apply logical rewrite rules

### 3. Optimization Phase
- [ ] Logical rewriter (pushdowns, pruning)
- [ ] Generate physical alternatives
- [ ] Estimate costs
- [ ] Pick lowest-cost plan

### 4. Execution Phase
- [ ] Create operator tree
- [ ] Call open() (cascade down)
- [ ] Loop: next() until null
- [ ] Call close() (cascade down)

### 5. Storage Access
- [ ] Table → HeapFile
- [ ] BufferPool (cache hit/miss)
- [ ] DiskManager (page read/write)
- [ ] RowCodec (decode/encode)

---

## Learning Checklist

### Phase 0-1: Foundations ✓
- [ ] Understand why databases exist
- [ ] Know the three-layer architecture
- [ ] Grasp the big picture flow
- [ ] Distinguish logical vs physical

### Phase 2: Storage Engine ✓
- [ ] Explain DiskManager (page I/O)
- [ ] Explain BufferPool (LRU caching)
- [ ] Explain Slotted Pages (variable records)
- [ ] Explain HeapFile (multi-page storage)
- [ ] Trace insert end-to-end

### Phase 3: Types & Metadata ✓
- [ ] Know all type definitions
- [ ] Understand Schema validation
- [ ] Understand Tuple immutability
- [ ] Explain RowCodec encoding
- [ ] Explain Catalog persistence
- [ ] Know NULL is not yet supported

### Phase 4: SQL Frontend ✓
- [ ] Tokenization process
- [ ] Recursive descent parsing
- [ ] AST structure
- [ ] Visitor pattern usage
- [ ] Validation rules

### Phase 5: Logical Planning ✓
- [ ] Name resolution (Binder)
- [ ] Type inference (Analyzer)
- [ ] Logical plan tree
- [ ] Schema propagation
- [ ] Rule engine

### Phase 6: Physical Execution ✓
- [ ] Volcano iterator model
- [ ] All physical operators
- [ ] Expression evaluation
- [ ] Pull-based execution flow
- [ ] Tuple-at-a-time processing

### Phase 7: Query Optimizer ✓
- [ ] Why optimization matters
- [ ] Logical rewriter
- [ ] Volcano optimizer algorithm
- [ ] Cost model formulas
- [ ] Join algorithm selection

### Phase 8: Complete Trace ✓
- [ ] Trace simple query end-to-end
- [ ] Follow data through all layers
- [ ] Understand every component's role
- [ ] Explain timing and performance

### Phase 9-10: Architecture ✓
- [ ] Draw HLD diagram
- [ ] Explain layer responsibilities
- [ ] Know all design patterns
- [ ] Understand SOLID application
- [ ] Explain key algorithms

### Phase 11: Missing Features & Bug Fixes ✓
- [ ] Know UPDATE/DELETE are implemented (M12 complete) with proper Volcano pipeline
- [ ] Know bug fixes: PageFullException, NULL parsing, SumAgg shadowing, unified execute API
- [ ] Know what's missing (indexes, txns, WAL, NULL semantics)
- [ ] Understand why they matter
- [ ] Explain how they'd integrate
- [ ] Can demo the interactive REPL end-to-end
- [ ] Estimate implementation effort (~2-2.5 months)

### Phase 12: Interview Prep ✓
- [ ] 30-second elevator pitch
- [ ] 2-minute architecture overview
- [ ] Answer all common questions
- [ ] Handle tough questions
- [ ] Technical deep-dives ready

---

## Quick Reference: Common Questions

### "How does a query execute?"
SQL → Parse → Validate → Bind → Logical Plan → Optimize → Physical Plan → Execute (Volcano) → Storage → Results

### "How does storage work?"
DiskManager (page I/O) → BufferPool (cache) → PageFormat (layout) → HeapFile (multi-page) → Table (typed)

### "How does the optimizer work?"
Logical rewrite → Generate alternatives → Estimate costs → Pick minimum

### "What's the Volcano model?"
Pull-based iterators: open() → next()* → close(). Tuple-at-a-time processing.

### "What design patterns are used?"
Strategy, Visitor, Iterator, Factory, Template, Facade, Builder

### "What's missing for production?"
Indexes (10-1000x speedup), Transactions (ACID), WAL (durability), NULLs, ORDER BY/LIMIT

### "How long to production?"
~2-2.5 months for indexes + transactions + WAL (UPDATE/DELETE already done)

---

## The Elevator Explanation

**For non-technical audience**:

"I built a database that stores and queries data, similar to PostgreSQL but from scratch. It handles everything from reading files on disk, to understanding SQL commands, to figuring out the fastest way to find your data."

**For technical audience**:

"I implemented a relational database in Java covering the full stack: NIO-based storage with buffer pool caching, a SQL parser with semantic validation, a cost-based query optimizer with multiple join algorithms, and Volcano-style execution. The architecture is modular following SOLID principles with about 15K lines of tested code."

**For database experts**:

"It's a Postgres-inspired educational database with slotted pages, LRU buffer management, recursive descent SQL parsing, relational algebra planning, and a simplified Volcano optimizer that explores join orders and algorithms. Not production-ready—no indexes, transactions, or WAL—but demonstrates the core techniques with clean, extensible architecture."

---

## Concept Mastery Self-Test

Score yourself 1-5 on each (1=confused, 5=can teach it):

### Storage Layer
- [ ] /5 - Explain why pages are 4KB
- [ ] /5 - Explain pin/unpin semantics
- [ ] /5 - Explain LRU eviction algorithm
- [ ] /5 - Explain slotted page layout
- [ ] /5 - Explain why RecordIds are stable

### Type System
- [ ] /5 - Explain RowCodec encoding format
- [ ] /5 - Explain why Tuple is immutable
- [ ] /5 - Explain catalog persistence
- [ ] /5 - Explain schema validation

### Query Processing
- [ ] /5 - Explain AST vs logical plan
- [ ] /5 - Explain binder vs analyzer
- [ ] /5 - Explain predicate pushdown
- [ ] /5 - Explain cost model formulas
- [ ] /5 - Explain Volcano iterator model

### Algorithms
- [ ] /5 - Explain nested loop join
- [ ] /5 - Explain hash join build phase
- [ ] /5 - Explain sort-merge join
- [ ] /5 - Explain hash aggregation
- [ ] /5 - Explain filter short-circuit

**Target**: All 5s (can explain clearly to others)

---

## Your Learning Path Summary

You started knowing **nothing** about database internals.

Now you understand:

✅ **Storage**: How data is organized on disk and cached in memory  
✅ **Types**: How bytes become typed data with schema enforcement  
✅ **SQL**: How text becomes executable plans  
✅ **Planning**: How queries are analyzed and transformed  
✅ **Optimization**: How the best execution plan is chosen  
✅ **Execution**: How plans run and produce results  
✅ **Architecture**: How components fit together cleanly  
✅ **Trade-offs**: Why design decisions were made  

**You can now**:

🎯 Explain EvolvDB end-to-end confidently  
🎯 Draw HLD and LLD diagrams from memory  
🎯 Discuss design trade-offs intelligently  
🎯 Answer database interview questions  
🎯 Extend the system safely  
🎯 Compare with production databases  

---

## Final Thoughts

### What Makes EvolvDB Valuable

**Not the scale** (it's educational, not production)  
**Not the novelty** (it uses established techniques)  

**But rather**:

1. **Completeness**: Full vertical slice from disk to SQL
2. **Clarity**: Clean architecture with clear boundaries
3. **Correctness**: Well-tested, follows best practices
4. **Comprehensibility**: Documented and understandable
5. **Capability**: Demonstrates real systems skills

### Your Achievement

Building a database from scratch is **hard**. You've:

- Managed complexity across 13 modules
- Implemented 7+ design patterns correctly
- Written 15,000+ lines of tested code
- Understood trade-offs at every level
- Created something that actually works

**This is non-trivial systems programming.**

### Next Steps in Your Journey

**To deepen understanding**:
- Implement M12-M18 (UPDATE/DELETE → transactions → WAL)
- Read database papers (Volcano, ARIES, etc.)
- Study PostgreSQL or MySQL source code
- Build query visualizations (EXPLAIN output)

**To apply knowledge**:
- System design interviews (you're ready!)
- Distributed systems (build on this foundation)
- Performance engineering (you understand I/O, caching)
- Other systems projects (compilers, VMs, OS)

**To share**:
- Write blog posts explaining concepts
- Create video walkthroughs
- Mentor others learning databases
- Contribute to open-source databases

---

## Closing

You've completed a comprehensive journey through database internals. From not knowing what a "page" is, to explaining query optimization in interviews.

**You're ready.**

Go build. Go interview. Go teach others.

And remember: **understanding how databases work makes you a better engineer**, regardless of what you build.

---

## Quick Links for Revision

- **Phase 0-1**: [`phase-0-1-foundations.md`](./phase-0-1-foundations.md) - Start here for review
- **Phase 2**: [`phase-2-storage-engine.md`](./phase-2-storage-engine.md) - Storage deep dive
- **Phase 3**: [`phase-3-types-metadata.md`](./phase-3-types-metadata.md) - Types & metadata
- **Phase 4**: [`phase-4-sql-frontend.md`](./phase-4-sql-frontend.md) - SQL parsing
- **Phase 5**: [`phase-5-logical-planning.md`](./phase-5-logical-planning.md) - Logical plans
- **Phase 6**: [`phase-6-physical-execution.md`](./phase-6-physical-execution.md) - Execution
- **Phase 7**: [`phase-7-query-optimizer.md`](./phase-7-query-optimizer.md) - Optimization
- **Phase 8**: [`phase-8-query-walkthrough.md`](./phase-8-query-walkthrough.md) - Complete trace
- **Phase 9**: [`phase-9-hld.md`](./phase-9-hld.md) - High-level design
- **Phase 10**: [`phase-10-lld.md`](./phase-10-lld.md) - Low-level design
- **Phase 11**: [`phase-11-missing-features.md`](./phase-11-missing-features.md) - Production gaps
- **Phase 12**: [`phase-12-interview-prep.md`](./phase-12-interview-prep.md) - Interview ready

**Main Index**: [`README.md`](./README.md)

---

**Congratulations on completing the EvolvDB Learning Guide!** 🎉
