# Slotted Page Format

Status: Spec finalized; API defined. Implementation to follow.

See also: [BufferPool](./buffer-pool.md) for how pages are fetched, pinned, and flushed while using this format.

## High-Level Design (HLD)

- Goal: Efficient variable-length record storage within fixed-size pages.
- Structure:
  - Header at the beginning (fixed-size).
  - Payload area grows upward from header.
  - Slot directory at the end grows downward.
  - Each slot points to a payload slice (offset, length). Negative length denotes a tombstone (deleted).
- Advantages:
  - Supports variable-length tuples.
  - Stable record identifiers via slots (RecordId).
  - Cheap deletes (tombstone) and compaction on demand.

```mermaid
flowchart TB
  subgraph Page
    direction TB
    Header[[Header]] --> Payload[Payload region grows up]
    SlotDir[Slot Directory grows down] -->|slots| Entries[slot: (offset,len)]
  end
```

## Low-Level Design (LLD)

Header layout (little-endian):
- int pageType (heap=1)
- int lsn
- short slotCount
- short freeStartOffset

Slot directory (from page end towards header):
- Each slot entry: short offset, short len
- len < 0 => tombstone

Constants and Offsets (see `SlottedPageFormat`):
- `OFF_TYPE=0`, `OFF_LSN=4`, `OFF_SLOT_COUNT=8`, `OFF_FREE_START=10`
- `HEADER_SIZE=12`, `SLOT_ENTRY_SIZE=4`

```mermaid
sequenceDiagram
  participant C as Client
  participant F as SlottedPageFormat
  participant P as Page(ByteBuffer)

  C->>F: insert(P, record)
  F->>P: read header (slotCount, freeStart)
  F->>F: ensure free space >= record.length + SLOT_ENTRY_SIZE
  F->>P: copy record at freeStart
  F->>P: append new slot at end (offset=freeStart, len=record.length)
  F->>P: update freeStart += record.length, slotCount++
  F-->>C: RecordId(pageId, slotIndex)

  C->>F: delete(P, rid)
  F->>P: locate slot entry
  F->>P: mark len = -abs(len)

  C->>F: read(P, rid)
  F->>P: locate slot entry; if len < 0 -> empty
  F->>P: slice payload(offset,len) and return bytes
```

### Free Space and Fragmentation

- `freeSpace(P)` computes available contiguous bytes between `freeStart` and start of slot directory.
- Fragmentation occurs after deletes; we delay compaction to maintain speed.
- Compaction strategy (future):
  - When `freeSpace(P)` < threshold for likely inserts, scan live slots, pack payloads contiguously, update offsets, and adjust `freeStart`.

## Interfaces and Contracts

- API (package `evolvdb-storage-page`):
  - `PageFormat` with `init`, `freeSpace`, `insert`, `read`, `delete`.
  - `RecordId(PageId pageId, short slot)` provides stable identity within page.
  - `SlottedPageFormat` defines header and slot constants and will implement the API.

## Design Patterns

- Template/Strategy: `PageFormat` abstracts page layout; `SlottedPageFormat` is one concrete strategy.

## SOLID

- SRP: Page format encapsulates only in-page layout mechanics.
- OCP: Alternative formats (e.g., fixed records) can be added without changing clients.
- LSP: Any `PageFormat` can replace `SlottedPageFormat` where `PageFormat` is expected.
- ISP: Clients use only the required methods (`insert/read/delete/freeSpace`).
- DIP: Higher layers depend on `PageFormat` abstraction.

## Trade-offs & Alternatives

- Slotted pages enable variable-length records but can fragment; requires compaction.
- Fixed-size slots simplify compaction but waste space.
- Alternative designs: Free lists instead of compaction; prefix/suffix optimizations for variable fields.

## Test Plan (before implementation)

- givenEmptyPage_whenInit_thenHeaderAndFreeSpaceCorrect
- givenInsertedRecords_whenRead_thenBytesMatch
- givenDeletedRecord_whenRead_thenEmpty
- givenFragmentation_whenCompactionTriggered_thenSpaceReclaimed
