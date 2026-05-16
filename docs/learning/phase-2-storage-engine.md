# Phase 2: Storage Engine Fundamentals (M1-M5)

**Goal**: Deep understanding of how data is physically stored, cached, and retrieved.

**Prerequisite**: Phase 0-1 (Foundations)

---

## Overview: The Storage Stack

The storage engine is the foundation of EvolvDB. It manages the physical bytes on disk and provides abstractions for higher layers.

**Bottom-up layers**:
```
DiskManager (M1)      ← Raw page I/O
    ↓
BufferPool (M2)       ← Caching with eviction
    ↓
Page Format (M3)      ← Record layout within pages
    ↓
HeapFile (M4)         ← Multi-page record management
    ↓
Scan & Update (M5)    ← Access patterns
```

---

## M1: DiskManager - Page I/O

**Module**: `evolvdb-storage-disk`

### What Problem Does It Solve?

The operating system doesn't understand "database pages." It just knows files and byte ranges.

**DiskManager's job**: Translate between:
- **Database language**: "Give me page 5 of file 'users'"
- **OS language**: "Read bytes 20480-24575 of `/data/users.evolv`"

### Core Concepts

#### FileId
```java
record FileId(String name) {}
```

A **logical identifier** for a file. Example: `FileId("users")`

The DiskManager maps this to a physical file: `/data/users.evolv`

#### PageId
```java
record PageId(FileId fileId, int pageNo) {}
```

A **physical page address**. Example: `PageId(FileId("users"), 5)`

This means: "Page 5 of the 'users' file"

**Why 0-based page numbers?**
- Standard in programming
- Math is simpler: `offset = pageNo * pageSize`

#### Page Size

EvolvDB uses **4KB pages** by default (configurable via `DbConfig`).

**Why 4KB?**
- OS page size is typically 4KB
- SSDs use 4KB pages internally
- Good balance: not too small (overhead), not too large (waste)

### API

```java
interface DiskManager {
    PageId allocatePage(FileId fileId);
    void readPage(PageId pageId, ByteBuffer dst);
    void writePage(PageId pageId, ByteBuffer src, long lsn);
    void sync();
    void close();
}
```

#### allocatePage
Extends the file by one page (zero-filled).

**Why zero-filled?**
- Security: Don't leak old data
- Predictability: Known initial state

#### readPage
Reads exactly one page into the provided ByteBuffer.

**Contract**:
- `dst.remaining() >= pageSize`
- Page must exist (pageNo < page count)
- Thread-safe

#### writePage
Writes exactly one page from the ByteBuffer.

**Parameters**:
- `pageId`: Which page to write
- `src`: Source data
- `lsn`: Log Sequence Number (unused in M1-M11, for WAL in M18)

#### sync
Forces all buffered writes to disk.

**Why needed?**
- OS buffers writes for performance
- Crash before OS flush = data loss
- `sync()` calls `FileChannel.force()`

### Implementation: NioDiskManager

**Module**: `evolvdb-storage-disk`  
**Class**: `io.github.anupam.evolvdb.storage.disk.NioDiskManager`

#### File Naming

Physical file: `<dataDir>/<fileName>.evolv`

Example:
- FileId: `"users"`
- Data dir: `/data`
- Physical file: `/data/users.evolv`

#### Page Number → Byte Offset

```java
long offset = pageNo * pageSize;
```

Example (4KB pages):
- Page 0: bytes 0-4095
- Page 1: bytes 4096-8191
- Page 5: bytes 20480-24575

#### File Layout

```
┌────────────┬────────────┬────────────┬─────
│  Page 0    │  Page 1    │  Page 2    │ ...
│  (4KB)     │  (4KB)     │  (4KB)     │
└────────────┴────────────┴────────────┴─────
Offset:  0        4096        8192
```

#### Concurrency

**M1-M11 approach**: Coarse-grained synchronization
- One lock per `FileChannel`
- Simple but limits parallelism

**Future improvement**: Fine-grained locks per page

### Why NIO (java.nio)?

**Old I/O (java.io)**:
- Stream-based
- Always copies data through JVM heap
- Less control

**New I/O (java.nio)**:
- ByteBuffer-based
- Direct memory (off-heap) possible
- `FileChannel` for efficient I/O
- Better control over positioning

### Edge Cases

1. **Page doesn't exist**: Exception thrown
2. **Disk full**: OS-level exception propagated
3. **Corrupt file**: Detected by BufferPool/PageFormat layers
4. **Concurrent access**: Synchronized at FileChannel level

### What DiskManager Does NOT Do

❌ Caching (that's BufferPool)  
❌ Understanding record layout (that's PageFormat)  
❌ Managing records across pages (that's HeapFile)  
❌ Write-ahead logging (that's M18)  

**Single Responsibility**: Page-level I/O only

---

## M2: BufferPool - Caching

**Module**: `evolvdb-storage-buffer`

### What Problem Does It Solve?

**Disk is slow** (milliseconds). **Memory is fast** (nanoseconds).

**BufferPool's job**: Keep frequently-used pages in memory.

### Mental Model: A Library

Imagine a library with:
- **Stacks**: Huge storage (disk)
- **Reading desks**: Limited workspace (buffer pool)
- **Librarian**: Fetches books you need (BufferPool)

When you request a book:
1. **Cache hit**: Already on a desk → instant access
2. **Cache miss**: Librarian fetches from stacks → slower

When desks are full and you need another book:
- Librarian returns least-recently-used book to stacks
- Brings your requested book to the freed desk

### Core Concepts

#### Frame

A **frame** is a slot in the buffer pool that can hold one page.

```java
class Frame {
    PageId pageId;           // Which page is cached
    ByteBuffer buffer;       // The actual page data (4KB)
    int pinCount;            // How many users need this page
    boolean dirty;           // Modified since read?
}
```

**Buffer pool size**: Number of frames (e.g., 100 frames = 400KB memory for 4KB pages)

#### Pin Count

**Pinning** prevents eviction while a page is in use.

```
pinCount = 0  →  Page can be evicted
pinCount > 0  →  Page is in use, must stay in memory
```

**Why needed?**
- Thread 1 is reading page 5
- Thread 2 needs memory and evicts page 5
- Thread 1's data is now garbage → **crash!**

**Solution**: Pin before use, unpin after use.

#### Dirty Flag

A page is **dirty** if modified in memory but not yet written to disk.

```
dirty = false  →  Memory matches disk, can evict without writing
dirty = true   →  Must write to disk before evicting
```

#### Eviction Policy

When buffer pool is full and we need to load a new page, **which page do we evict?**

**Strategy pattern**: `EvictionPolicy` interface with different implementations.

**M2 implements**: `LruEvictionPolicy` (Least Recently Used)

**LRU logic**:
- Track access order
- Evict the page accessed longest ago (among unpinned pages)

**Why LRU?**
- Simple to implement
- Works well for temporal locality (recently used = likely used again)

**Alternatives** (not implemented):
- **CLOCK**: Approximates LRU with less overhead
- **2Q**: Handles scan resistance better
- **ARC**: Adaptive replacement (balances recency and frequency)

### API

```java
interface BufferPool {
    Page getPage(PageId pageId, boolean forUpdate);
    void unpin(PageId pageId, boolean dirty);
    void flush(PageId pageId);
    void flushAll();
}
```

#### getPage

Retrieves a page, pinning it in memory.

**Parameters**:
- `pageId`: Which page to fetch
- `forUpdate`: If true, caller intends to modify (optimization hint)

**Returns**: `Page` object (wrapper around ByteBuffer)

**Behavior**:
1. **Cache hit**: Increment pinCount, update eviction policy
2. **Cache miss**:
   - If pool full: evict victim (flush if dirty)
   - Read page from DiskManager
   - Store in frame
   - Set pinCount = 1

#### unpin

Releases a pinned page.

**Parameters**:
- `pageId`: Which page to release
- `dirty`: Did caller modify the page?

**Behavior**:
- Decrement pinCount
- If `dirty=true`, mark frame as dirty

**Critical**: Every `getPage` must have a matching `unpin`, or page stays pinned forever (memory leak).

#### flush

Forces a specific page to disk if dirty.

#### flushAll

Forces all dirty pages to disk (used at shutdown).

### Sequence Diagram: Cache Miss with Eviction

```
Client       BufferPool   EvictionPolicy   DiskManager
  │              │               │              │
  │─getPage(P7)─>│               │              │
  │              │─frames full?─>│              │
  │              │               │              │
  │              │─evictCandidate(canEvict)────>│
  │              │<─────return P2───────────────│
  │              │                              │
  │              │─P2.dirty?────────────────────│
  │              │─writePage(P2)────────────────>
  │              │<─────done────────────────────│
  │              │─onRemove(P2)────────────────>│
  │              │                              │
  │              │─readPage(P7)─────────────────>
  │              │<─────bytes───────────────────│
  │              │─onInsert(P7)────────────────>│
  │              │                              │
  │<────Page(P7)─│                              │
```

### Implementation: DefaultBufferPool

**Class**: `io.github.anupam.evolvdb.storage.buffer.DefaultBufferPool`

**Data structures**:
```java
Map<PageId, Frame> frames;       // PageId → Frame
EvictionPolicy policy;           // Pluggable strategy
int capacity;                    // Max frames
```

**Synchronization**: `synchronized` methods (coarse-grained for M1-M11)

### Eviction Policy: LruEvictionPolicy

**Class**: `io.github.anupam.evolvdb.storage.buffer.LruEvictionPolicy`

**Data structure**:
```java
LinkedHashMap<PageId, Void>  // Insertion order = access order
```

**Operations**:
- `onInsert(pageId)`: Add to end
- `onAccess(pageId)`: Move to end
- `onRemove(pageId)`: Remove from map
- `evictCandidate(canEvict)`: Iterate from front until `canEvict` returns true

### Edge Cases

1. **All pages pinned**: `evictCandidate` returns null → exception
2. **Dirty page eviction**: Must flush first
3. **Concurrent access**: Synchronized methods prevent races
4. **Unpin without matching getPage**: Ignored (idempotent for safety)

### Performance Characteristics

**Hit rate** = (cache hits) / (total requests)

**Good hit rate**: 90-99% (typical for database workloads with locality)

**Buffer pool too small**: Thrashing (constant eviction/reload)

**Buffer pool too large**: Wastes memory, diminishing returns

---

## M3: Slotted Page Format

**Module**: `evolvdb-storage-page`

### What Problem Does It Solve?

Pages contain **variable-length records** (rows).

**Challenge**: How to efficiently pack variable-length data into fixed-size pages?

**Slotted Page Format** is the industry-standard solution (used in PostgreSQL, MySQL).

### High-Level Structure

A page has three regions:

```
┌─────────────────────────────────────────────┐
│ Header (fixed size: 12 bytes)               │
├─────────────────────────────────────────────┤
│ Payload Region (grows upward →)            │
│ [Record 1][Record 2][Record 3]...          │
│                                             │
│                       ← Free Space →        │
│                                             │
│                        ...[Slot 2][Slot 1]│
│                    (← grows downward)      │
├─────────────────────────────────────────────┤
│ Slot Directory (grows downward from end)    │
└─────────────────────────────────────────────┘
```

**Key insight**: Payload and slots grow toward each other. When they meet, page is full.

### Header Layout

```
Offset  Size  Field
------  ----  -----------------
0       4     pageType (int)       // Heap page = 1
4       4     lsn (int)            // Log Sequence Number (unused in M1-M11)
8       2     slotCount (short)    // Number of slots
10      2     freeStartOffset (short)  // Start of free space
```

**Total header size**: 12 bytes

### Slot Entry

Each slot is **4 bytes**:

```
Offset  Size  Field
------  ----  -----------------
0       2     offset (short)      // Byte offset of record in payload
2       2     length (short)      // Length of record (-1 if deleted)
```

**Slot directory** starts at end of page and grows **backward**:

```
Page end - 4 bytes:  Slot 0
Page end - 8 bytes:  Slot 1
Page end - 12 bytes: Slot 2
...
```

### RecordId

```java
record RecordId(PageId pageId, short slotIndex) {}
```

**Critical property**: RecordId is **stable**.

Even if record moves within the page (compaction), the slot index stays the same. Just update the slot's offset field.

### Operations

#### init(Page page)

Initialize an empty page:
```java
page.putInt(0, HEAP_PAGE_TYPE);     // pageType = 1
page.putInt(4, 0);                  // lsn = 0
page.putShort(8, 0);                // slotCount = 0
page.putShort(10, HEADER_SIZE);     // freeStart = 12
```

#### freeSpace(Page page)

Calculate available contiguous space:

```java
int freeStart = page.getShort(10);
int slotCount = page.getShort(8);
int slotDirStart = pageSize - (slotCount * SLOT_ENTRY_SIZE);
return slotDirStart - freeStart;
```

**Free space** = gap between `freeStart` and start of slot directory.

#### insert(Page page, byte[] record)

1. Check: `freeSpace >= record.length + SLOT_ENTRY_SIZE`
2. Copy record to `freeStart` offset
3. Append new slot entry at end of directory
4. Update `freeStart += record.length`
5. Update `slotCount++`
6. Return `RecordId(pageId, slotCount - 1)`

#### read(Page page, RecordId rid)

1. Validate: `rid.slot < slotCount`
2. Read slot entry at: `pageSize - (rid.slot + 1) * SLOT_ENTRY_SIZE`
3. Extract offset and length
4. If `length < 0`: Record is deleted (tombstone)
5. Otherwise: Copy `length` bytes from `offset` into result array

#### delete(Page page, RecordId rid)

1. Find slot entry
2. Read current length
3. Set length = `-abs(length)` (negative = tombstone)

**Note**: Payload bytes are **not** reclaimed immediately. Space fragmented.

#### update(Page page, RecordId rid, byte[] newRecord)

1. Read current record length
2. If `newRecord.length <= currentLength`: Overwrite in place
3. Otherwise: Mark as deleted, insert new record elsewhere
   - Returns new RecordId

### Compaction

After many deletes, **fragmentation** occurs:

```
Before:
[R1][deleted][R2][deleted][R3]  ← Fragmented

After compaction:
[R1][R2][R3]                    ← Contiguous, more free space
```

**Compaction algorithm**:
1. Scan all slots
2. For each live record, copy to new offset (packing contiguously)
3. Update slot offsets
4. Update `freeStart`

**When to compact?**
- When `freeSpace` insufficient but total live bytes would fit
- Triggered automatically in `insert` if needed

### Why This Design?

**Advantages**:
1. ✅ Handles variable-length records efficiently
2. ✅ Stable RecordId (slot index never changes)
3. ✅ Cheap deletes (just flip length sign)
4. ✅ Delayed compaction (only when needed)

**Disadvantages**:
1. ❌ Fragmentation after deletes
2. ❌ Compaction is O(records) and blocks page
3. ❌ Slot directory overhead (4 bytes per record)

**Alternatives** (not implemented):
- Fixed-size records: Simpler but wastes space
- Linked lists: More flexible but pointer overhead
- Log-structured: No in-place updates, different trade-offs

---

## M4: HeapFile - Multi-Page Records

**Module**: `evolvdb-storage-record`

### What Problem Does It Solve?

A table can't fit in one page. We need to manage records **across multiple pages**.

**HeapFile's job**: Abstract away the multi-page complexity.

### Heap File Concept

A **heap file** is an unordered collection of records spread across pages.

**"Heap" means**: No particular order. Records inserted wherever there's space.

**Not a heap data structure**: Confusing terminology, but standard in databases.

### API

```java
interface HeapFile {
    RecordId insert(byte[] record);
    byte[] read(RecordId recordId);
    void delete(RecordId recordId);
    RecordId update(RecordId recordId, byte[] newRecord);
    Iterator<byte[]> scan();
}
```

### Insert Algorithm

**Goal**: Find a page with enough space, or allocate a new one.

```
1. Get page count from DiskManager
2. For each existing page (0 to count-1):
   a. Pin page for update
   b. Check PageFormat.freeSpace(page)
   c. If sufficient:
      - PageFormat.insert(page, record)
      - Unpin page as dirty
      - Return RecordId
   d. Otherwise:
      - Unpin page (not dirty)
3. If no space found in any page:
   a. Allocate new page via DiskManager
   b. Pin new page
   c. PageFormat.init(page)
   d. PageFormat.insert(page, record)
   e. Unpin page as dirty
   f. Return RecordId
```

**Performance**: O(n) in worst case (scan all pages).

**Future optimization**: Free-space map (tracks which pages have space).

### Read Algorithm

```
1. Extract PageId from RecordId
2. Pin page (read-only)
3. PageFormat.read(page, rid)
4. Unpin page (not dirty)
5. Return bytes
```

**Performance**: O(1) - direct page access.

### Delete Algorithm

```
1. Pin page for update
2. PageFormat.delete(page, rid)
3. Unpin page as dirty
```

**Note**: Space is fragmented but not immediately reclaimed.

### Update Algorithm

**Two strategies**:

**In-place update** (if new record fits in old slot):
```
1. Pin page for update
2. PageFormat.update(page, rid, newRecord)
3. If succeeded:
   - Unpin page as dirty
   - Return same RecordId
```

**Relocate update** (if new record doesn't fit):
```
1. Delete old record
2. Insert new record elsewhere
3. Return new RecordId
```

**Implication**: RecordId can change on update!

**Future improvement**: Forwarding pointers (keep RecordId stable).

### Scan (Iterator)

Iterate over **all live records** across **all pages**:

```java
class HeapFileIterator implements Iterator<byte[]> {
    int currentPageNo;
    int currentSlot;
    int totalPages;
    
    public boolean hasNext() {
        // Skip deleted records and advance page/slot
    }
    
    public byte[] next() {
        // Return current record, advance position
    }
}
```

**Performance**: O(total records) - must scan all pages.

**Optimizations** (not implemented):
- Skip pages with no live records
- Parallel scan (future M23)

### RecordManager

**Class**: `io.github.anupam.evolvdb.storage.record.RecordManager`

A **factory** for HeapFiles:

```java
class RecordManager {
    Map<String, HeapFile> openFiles;
    
    HeapFile openHeapFile(String name, PageFormat format) {
        if (openFiles.contains(name)) {
            return openFiles.get(name);
        }
        HeapFile hf = new HeapFile(name, diskManager, bufferPool, format);
        openFiles.put(name, hf);
        return hf;
    }
}
```

**Why needed?**
- Caches open HeapFile instances
- Ensures one HeapFile per logical file name

---

## M5: Scan & Update Operations

**Module**: `evolvdb-storage-record`

### Scan Operation

**Purpose**: Iterate over all records in a HeapFile.

**Used by**: Sequential scan operator (SeqScanExec in execution layer).

**Implementation**: HeapFile.scan() returns `Iterator<byte[]>`

**Contract**:
- Skips tombstones (deleted records)
- Order is undefined (heap = unordered)
- Concurrent modifications may or may not be visible (no MVCC yet)

### Update Semantics

**Challenge**: What if record doesn't fit in current slot?

**EvolvDB approach**:

1. **Try in-place update**: If `newRecordSize <= currentSlotSize`, overwrite
2. **Relocate if necessary**: Delete old, insert new elsewhere

**Impact on higher layers**:

The **Table** layer (M7) wraps this and handles RecordId changes:

```java
RecordId Table.update(RecordId oldRid, Tuple newTuple) {
    byte[] newBytes = RowCodec.encode(schema, newTuple);
    RecordId newRid = heapFile.update(oldRid, newBytes);
    return newRid;  // May differ from oldRid
}
```

**Implication for indexes** (M16):
- If RecordId changes, index entries must be updated
- This is why indexes have overhead on updates

---

## Storage Engine: Complete Picture

Let's trace an insert from top to bottom:

```sql
INSERT INTO users VALUES (1, 'Alice', 30)
```

**Step-by-step execution**:

```
1. Table.insert(tuple)
     ↓
2. RowCodec.encode(schema, tuple) → bytes
     ↓
3. HeapFile.insert(bytes)
     ↓
4. For each page:
     BufferPool.getPage(pageId, forUpdate=true)
     ↓
     (Cache miss → DiskManager.readPage)
     ↓
     PageFormat.freeSpace(page)
     ↓
     If insufficient: BufferPool.unpin(dirty=false), try next page
     ↓
     If sufficient: PageFormat.insert(page, bytes) → RecordId
     ↓
     BufferPool.unpin(dirty=true)
     ↓
     Return RecordId
     
5. If no space in any page:
     DiskManager.allocatePage(fileId) → new PageId
     ↓
     BufferPool.getPage(newPageId, forUpdate=true)
     ↓
     PageFormat.init(page)
     ↓
     PageFormat.insert(page, bytes) → RecordId
     ↓
     BufferPool.unpin(dirty=true)
     ↓
     Return RecordId
```

**Dirty page flush** happens when:
- Page evicted from BufferPool
- Explicit flush call
- Database shutdown (flushAll)

---

## Key Takeaways

### Layered Abstraction

Each layer has a clear job:
- **DiskManager**: Dumb page I/O (no caching, no understanding of content)
- **BufferPool**: Caching with LRU (no understanding of record layout)
- **PageFormat**: Record layout within a page (doesn't know about other pages)
- **HeapFile**: Multi-page record management (doesn't know about types or schemas)

### Performance Characteristics

**DiskManager**:
- Page I/O: O(1) per page
- Allocation: O(1) (append)

**BufferPool**:
- Cache hit: O(1)
- Cache miss: O(1) + disk I/O + possible eviction
- Eviction (LRU): O(n) worst case, typically O(1) amortized

**PageFormat (Slotted Page)**:
- Insert: O(1) without compaction, O(records) with compaction
- Read: O(1)
- Delete: O(1)
- Free space: O(1)

**HeapFile**:
- Insert: O(pages) worst case (scan for space)
- Read: O(1)
- Delete: O(1)
- Update: O(1) in-place, O(pages) if relocated
- Scan: O(total records)

### Design Patterns Used

1. **Strategy**: PageFormat, EvictionPolicy
2. **Factory**: RecordManager
3. **Iterator**: HeapFile.scan()
4. **Facade**: HeapFile (hides BufferPool and DiskManager complexity)

### What We Haven't Covered (Planned)

- **Indexes**: Fast lookup without scanning (M16)
- **Free-space map**: O(1) insert via bitmap of pages with space
- **WAL integration**: Crash recovery (M18)
- **Concurrent access**: Fine-grained locking (M17)

---

## Self-Test Questions

1. **Why does BufferPool need pin counts?**
   - Prevents eviction while page is in use

2. **What happens if all pages are pinned and we need a new page?**
   - Exception thrown (no evictable frames)

3. **How does slotted page format maintain stable RecordIds?**
   - Slot index never changes; only offset within page changes during compaction

4. **Why is HeapFile insert O(pages)?**
   - Must scan pages sequentially to find one with space

5. **What is the difference between dirty=true and forUpdate=true?**
   - `forUpdate`: Intent to modify (hint for BufferPool)
   - `dirty`: Actual modification occurred (must flush on evict)

6. **Why can't we just use HashMap<RecordId, byte[]>?**
   - No persistence, no page management, no buffer pool, no disk I/O

---

## Next Steps

You now understand how data is physically stored and accessed. In **Phase 3**, we'll move up to the logical layer:
- Type system (INT, VARCHAR, etc.)
- Schemas and metadata
- Tuples (logical rows)
- Binary encoding
- Catalog (persistent metadata)

**Continue to**: [`phase-3-types-metadata.md`](./phase-3-types-metadata.md)
