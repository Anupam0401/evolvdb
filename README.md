# EvolvDB

A Postgres-inspired SQL database built from scratch in Java with clean architecture, SOLID principles, and extensibility for future NoSQL models.

## Vision & Scope

- Educational yet practical: learn database internals by building core components without external DB libraries.
- Modular architecture that cleanly separates storage, execution, SQL parsing, catalog, transactions, and recovery.
- Extensible foundation for alternative data models (e.g., key-value, document) and storage engines.

## Project Status (Milestones)

- DiskManager (M1): ✅ NIO-based page I/O with tests
- BufferPool (M2): ✅ Pin/unpin, LRU eviction (Strategy), dirty + flush-on-evict with tests
- Slotted Page (M3): ✅ Spec + API + implementation with compaction and tests
- HeapFile/RecordManager (M4): ✅ HeapFile + RecordManager with tests and CLI demo
- Catalog & Types: ⏳ (basic components scaffolded)
- SQL Parser/AST, Execution, Optimizer, Txn/Recovery: later milestones

## Module Overview

- `evolvdb-common`: shared exceptions/utilities
- `evolvdb-config`: `DbConfig` (page size, buffer pool size, data dir, ...)
- `evolvdb-types`: type system and schema (`Type`, `ColumnMeta`, `Schema`)
- `evolvdb-storage-disk`: `DiskManager`, `NioDiskManager`, tests
- `evolvdb-storage-page`: page abstractions and formats (`Page`, `PageFormat`, `SlottedPageFormat`), tests
- `evolvdb-storage-buffer`: `BufferPool`, eviction policies (`EvictionPolicy`, `LruEvictionPolicy`), tests
- `evolvdb-storage-record`: `HeapFile`, `RecordManager`, tests
- `evolvdb-catalog`: catalog scaffolding (to be implemented)
- `evolvdb-core`: `Database` facade (composition root)
- `evolvdb-cli`: minimal CLI entrypoint for demos

## Build & Run

- Build all modules:

```bash
./gradlew clean build
```

- Run CLI:

```bash
# Preferred: configure data directory
./gradlew :evolvdb-cli:run -Devolvdb.dataDir=./data

# Or via environment variable
EVOLVDB_DATA_DIR=./data ./gradlew :evolvdb-cli:run
```

## Tests

Run all tests:

```bash
./gradlew test
```

Run specific module tests, e.g. BufferPool:

```bash
./gradlew :evolvdb-storage-buffer:test
```

## Data Directory Handling

Priority order used by CLI:
1) System property `-Devolvdb.dataDir=<path>`
2) Env var `EVOLVDB_DATA_DIR=<path>`
3) Fallback: `./data` within the repository root (auto-detected by walking up to find `settings.gradle.kts` or `gradlew`).

## Documentation

See the `docs/` folder. Start here:
- Storage subsystem overview: `docs/storage/README.md`
- DiskManager: `docs/storage/disk-manager.md`
- BufferPool: `docs/storage/buffer-pool.md`
- Slotted Page: `docs/storage/slotted-page.md`

Diagrams are provided using Mermaid (flows, sequences, class relationships). All components include HLD, LLD, patterns, SOLID notes, and trade-offs.

## Contribution Guidelines

- Follow SOLID principles and clean OOP design.
- Keep modules decoupled and avoid leaking implementation details across boundaries.
- Use Strategy/Factory/Builder/etc. where appropriate; call out patterns in code comments and docs.
- Tests must use behavior-driven naming (given...when...then...).
- Every milestone change must include: code + tests + docs.

## Java Toolchain

- Target: Java 21 LTS via Gradle toolchain
- Development: you can use Java 23 locally; Gradle compiles against 21 for compatibility.
