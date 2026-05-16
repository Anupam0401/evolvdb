# EvolvDB Learning Guide

**Complete Zero-to-Expert Learning Path for EvolvDB Database System**

This guide takes you from zero database knowledge to expert-level understanding of EvolvDB, a Postgres-inspired SQL database built from scratch in Java. By the end, you'll be able to explain every component confidently, draw architecture diagrams, and discuss design trade-offs in technical interviews.

## Learning Philosophy

This is a **bottom-up, first-principles approach**:
- Start with absolute fundamentals
- Build concepts gradually
- Connect theory to actual implementation
- Explain **why**, not just **what**
- Use intuition first, then rigor

## How to Use This Guide

1. **Read sequentially** - Each phase builds on previous ones
2. **Map concepts to code** - Every concept references actual EvolvDB modules
3. **Take notes** - This is dense material meant for mastery
4. **Draw diagrams** - Visualize as you learn
5. **Revisit** - Come back for interview prep

## Learning Phases

### Phase 0-1: Foundations (Start Here)
- [`phase-0-1-foundations.md`](./phase-0-1-foundations.md)
  - What problem databases solve
  - Mental model of EvolvDB
  - High-level architecture overview
  - First principles: data, persistence, pages

### Phase 2: Storage Engine Deep Dive
- [`phase-2-storage-engine.md`](./phase-2-storage-engine.md)
  - DiskManager: page I/O
  - BufferPool: caching and eviction
  - Slotted Page Format: variable-length records
  - HeapFile: multi-page record management
  - Scan & Update operations

### Phase 3: Type System & Metadata
- [`phase-3-types-metadata.md`](./phase-3-types-metadata.md)
  - Type system (INT, VARCHAR, etc.)
  - Schema and ColumnMeta
  - Tuple: logical rows
  - RowCodec: binary encoding
  - Catalog: persistent metadata

### Phase 4: SQL Frontend
- [`phase-4-sql-frontend.md`](./phase-4-sql-frontend.md)
  - Tokenizer and Parser
  - Abstract Syntax Tree (AST)
  - SQL validation
  - Supported SQL subset

### Phase 5: Logical Planning
- [`phase-5-logical-planning.md`](./phase-5-logical-planning.md)
  - What is a logical plan?
  - Relational algebra operators
  - Binder: name resolution
  - Analyzer: type inference
  - Logical rewrite rules

### Phase 6: Physical Execution
- [`phase-6-physical-execution.md`](./phase-6-physical-execution.md)
  - Volcano iterator model
  - Physical operators (Scan, Filter, Project, Join, Aggregate)
  - Expression evaluation
  - Pull-based execution

### Phase 7: Query Optimizer
- [`phase-7-query-optimizer.md`](./phase-7-query-optimizer.md)
  - Why optimization matters
  - Volcano optimizer architecture
  - Memo structure and equivalence
  - Cost model
  - Join algorithm selection
  - Predicate pushdown and projection pruning

### Phase 8: End-to-End Query Walkthrough
- [`phase-8-query-walkthrough.md`](./phase-8-query-walkthrough.md)
  - Complete query execution trace
  - SQL → AST → Logical → Physical → Results
  - Step-by-step with code references

### Phase 9: High-Level Design
- [`phase-9-hld.md`](./phase-9-hld.md)
  - System architecture
  - Layer responsibilities
  - Data flow diagrams
  - Component interactions

### Phase 10: Low-Level Design
- [`phase-10-lld.md`](./phase-10-lld.md)
  - Key classes and interfaces
  - Design patterns used
  - SOLID principles application
  - Module boundaries

### Phase 11: What's Missing & Why
- [`phase-11-missing-features.md`](./phase-11-missing-features.md)
  - Indexes (B+Tree)
  - Transactions (2PL/MVCC)
  - Durability (WAL)
  - NULL support
  - How they would integrate

### Phase 12: Interview Preparation
- [`phase-12-interview-prep.md`](./phase-12-interview-prep.md)
  - 30-second pitch
  - 2-minute overview
  - 10-minute deep dive
  - Common questions with answers
  - Design trade-offs
  - Comparison with PostgreSQL

### Phase 13: Mental Models & Summary
- [`phase-13-summary.md`](./phase-13-summary.md)
  - Complete mental model
  - Learning checklist
  - Concept dependency graph
  - Quick reference guide

### Phase 14: Java 25 Migration
- [`phase-14-java25-migration.md`](./phase-14-java25-migration.md)
  - Virtual Threads and M:N scheduling
  - Scoped Values for transaction context
  - MemorySegment replacing ByteBuffer
  - Stream Gatherers
  - Concurrency patterns (ReadWriteLock, lock striping)

## Time Commitment

- **Phase 0-1**: 2-3 hours (foundation)
- **Phase 2**: 4-5 hours (storage deep dive)
- **Phase 3**: 2-3 hours (types & metadata)
- **Phase 4**: 1-2 hours (SQL parsing)
- **Phase 5**: 2-3 hours (logical planning)
- **Phase 6**: 3-4 hours (execution)
- **Phase 7**: 3-4 hours (optimizer)
- **Phase 8**: 2-3 hours (walkthrough)
- **Phase 9-10**: 2-3 hours (architecture)
- **Phase 11**: 1-2 hours (missing features)
- **Phase 12**: 2-3 hours (interview prep)
- **Phase 13**: 1-2 hours (summary)

**Total: ~28-38 hours for complete mastery**

## Learning Outcomes

After completing this guide, you will:

✅ **Understand** every layer of the database from disk I/O to SQL queries  
✅ **Explain** how queries execute end-to-end  
✅ **Draw** HLD and LLD diagrams from memory  
✅ **Discuss** design trade-offs confidently  
✅ **Extend** the system safely  
✅ **Compare** EvolvDB with production databases  
✅ **Ace** system design interviews involving databases  

## Quick Start

If you're already familiar with database concepts:
1. Read Phase 0 for orientation
2. Skim Phase 2-3 focusing on implementation details
3. Focus deeply on Phase 5-7 (planning, execution, optimization)
4. Jump to Phase 12 for interview prep

If you're new to databases:
1. Start at Phase 0 and work through sequentially
2. Don't skip phases
3. Code exploration recommended alongside reading

## Additional Resources

- Main README: `/README.md` - Project overview and quick reference
- Architecture Summary: `/docs/architecture/ARCHITECTURE_SUMMARY.md`
- Module-specific docs: `/docs/storage/`, `/docs/catalog/`, etc.
- Source code: Explore packages referenced in each phase

---

**Ready to begin? Start with [Phase 0-1: Foundations](./phase-0-1-foundations.md)**
