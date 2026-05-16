# Phase 14: Java 25 Migration — Modern JVM Features for Database Systems

**Goal**: Understand the Java 25 features adopted by EvolvDB, why each matters for a database system, and how they integrate with the existing architecture.

**Prerequisites**: Phase 0-13 (Complete system understanding)

---

## Overview: Why Migrate?

EvolvDB was originally built on Java 21. Java 25 (LTS, September 2025) accumulates four releases of finalized features that directly benefit database systems:

| Feature | JEP | Available Since | Database Relevance |
|---------|-----|-----------------|-------------------|
| Virtual Threads | 444 | Java 21 | Thousands of concurrent clients without OS thread exhaustion |
| Scoped Values | 506 | Java 25 | Transaction context propagation without ThreadLocal leaks |
| Foreign Function & Memory API | 454 | Java 22 | Safer, faster page I/O replacing ByteBuffer |
| Stream Gatherers | 485 | Java 24 | Custom intermediate stream operations for result processing |
| Unnamed Variables `_` | 456 | Java 22 | Cleaner catch blocks and lambda parameters |
| Flexible Constructor Bodies | 513 | Java 25 | Validation before super() calls in constructors |
| Compact Object Headers | 519 | Java 25 | Reduced memory footprint for millions of in-memory objects |
| Generational Shenandoah | 521 | Java 25 | Low-pause GC for buffer-pool-heavy workloads |

---

## 1. Virtual Threads

### The Problem

Traditional (platform) threads are 1:1 with OS threads. Each costs ~1 MB of stack memory and is expensive to create/destroy. A database server handling 10,000 concurrent connections would need 10,000 OS threads — ~10 GB just for stacks, plus constant context switching overhead.

This forces databases to use thread pools (e.g., 100 threads) and multiplex connections, adding complexity (connection queuing, async callbacks, reactive patterns).

### The Solution: M:N Threading

Virtual threads are **lightweight, JVM-managed threads** that are multiplexed onto a small pool of platform (carrier) threads. The JVM schedules them cooperatively:

```
10,000 virtual threads
        ↓
    JVM Scheduler (M:N mapping)
        ↓
    ~CPU-cores platform threads
```

Key properties:
- **Cheap**: ~200 bytes each (vs ~1 MB for platform threads)
- **Blocking is free**: When a virtual thread blocks on I/O (disk read, network), the JVM unmounts it from the carrier thread and schedules another — no thread wasted waiting
- **No code changes needed**: `java.lang.Thread` API works identically; `synchronized`, `Lock`, etc. all work

### Why It Matters for EvolvDB

Database workloads are I/O-bound: reading pages from disk, waiting for network responses from clients. Virtual threads let us:

1. **One thread per client connection** — no connection pooling complexity
2. **Blocking I/O model** — simpler code than async/reactive; the JVM handles the efficiency
3. **Cheap concurrency inside queries** — e.g., parallel hash join build, concurrent index lookups

### Usage in EvolvDB

```java
// TCP server: one virtual thread per client
try (var server = ServerSocket(port)) {
    while (true) {
        Socket client = server.accept();
        Thread.startVirtualThread(() -> handleClient(client, database));
    }
}
```

Or with an executor:
```java
try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
    executor.submit(() -> database.execute(sql));
}
```

### What NOT to Do

- Do not use virtual threads for CPU-bound work (e.g., sorting, hashing) — they compete on the same carrier threads
- Avoid `synchronized` blocks that perform I/O inside them — this pins the virtual thread to its carrier. Use `ReentrantLock` instead.

---

## 2. Scoped Values

### The Problem: ThreadLocal is Broken for Modern Concurrency

Databases need to thread context (transaction ID, isolation level, session state) through deeply nested call stacks: SQL → planner → executor → buffer pool → disk.

The traditional Java approach is `ThreadLocal`:

```java
static final ThreadLocal<Transaction> CURRENT_TXN = new ThreadLocal<>();
```

ThreadLocal has three serious problems:
1. **Mutable**: Any code can silently overwrite the value → hard-to-trace bugs
2. **Memory leaks**: Values persist until explicitly removed; forgotten `remove()` calls leak memory, especially with thread pools
3. **No inheritance by virtual threads**: Virtual threads don't automatically inherit ThreadLocal values from their parent scope

### The Solution: ScopedValue

`ScopedValue` (JEP 506, finalized in Java 25) provides an immutable, automatically-scoped alternative:

```java
static final ScopedValue<Transaction> TXN = ScopedValue.newInstance();

// Bind the value for a scope
ScopedValue.where(TXN, txn).run(() -> {
    // All code within this lambda (and any calls it makes) can read TXN.get()
    database.execute(sql);
});
// Binding is automatically removed when run() returns
```

Key properties:
- **Immutable within scope**: Once bound, cannot be changed (only rebound in a nested scope)
- **Automatic cleanup**: No manual `remove()` — the binding vanishes when the scope ends
- **Inherited by child threads**: Virtual threads forked within a scope automatically see the parent's scoped values
- **Zero-allocation fast path**: Reading a ScopedValue is as fast as reading a local variable

### Why It Matters for EvolvDB

When we implement transactions (M17), every operator in the execution tree needs access to the active transaction context. Without ScopedValue, we'd have to thread a `TransactionContext` parameter through every single method:

```java
// BEFORE: Explicit parameter threading (painful)
Tuple next(TransactionContext txn) {
    Page page = bufferPool.getPage(pageId, txn);
    // ...
}
```

With ScopedValue:
```java
// AFTER: Implicit context via ScopedValue
static final ScopedValue<TransactionContext> TXN_CTX = ScopedValue.newInstance();

Tuple next() {
    TransactionContext txn = TXN_CTX.get();
    Page page = bufferPool.getPage(pageId, txn);
    // ...
}
```

The transaction context is bound once at the top level and is visible throughout the entire call stack without any parameter changes.

---

## 3. Foreign Function & Memory API (MemorySegment)

### The Problem with ByteBuffer

EvolvDB's storage layer uses `java.nio.ByteBuffer` for page-level I/O. ByteBuffer has fundamental issues:

1. **No deterministic deallocation**: Direct ByteBuffers (off-heap) rely on GC finalization — memory can accumulate unpredictably
2. **Unsafe position/limit state**: ByteBuffer is stateful (position, limit, mark) — easy to corrupt when shared
3. **No structured access**: Reading fields requires manual offset arithmetic (`buf.getInt(offset + 4)`)
4. **2 GB limit**: ByteBuffer is indexed by `int`, capping at ~2 GB per buffer

### The Solution: MemorySegment + Arena

The Foreign Function & Memory API (JEP 454, finalized in Java 22) provides:

**MemorySegment**: A safe, bounded region of memory (on-heap or off-heap).

```java
// Allocate 4KB page in an Arena
Arena arena = Arena.ofConfined();
MemorySegment page = arena.allocate(4096);

// Structured access (no manual offset arithmetic)
page.set(ValueLayout.JAVA_INT, 0, slotCount);     // write int at offset 0
page.set(ValueLayout.JAVA_SHORT, 4, freeSpace);   // write short at offset 4
int slots = page.get(ValueLayout.JAVA_INT, 0);    // read back
```

**Arena**: Controls the lifetime of all segments allocated within it.

```java
try (Arena arena = Arena.ofConfined()) {
    MemorySegment seg = arena.allocate(4096);
    // ... use seg ...
}   // All memory freed deterministically here
```

Arena types:
- `Arena.ofConfined()` — single-thread ownership, freed on close
- `Arena.ofShared()` — multi-thread access, freed on close
- `Arena.global()` — never freed (process lifetime)

### The Before/After for EvolvDB

**Before (ByteBuffer)**:
```java
// SlottedPageFormat — manual offset arithmetic, stateful buffer
ByteBuffer buf = page.buffer();
buf.position(0);
int slotCount = buf.getShort();
buf.position(2);
int freeEnd = buf.getShort();
// ... fragile, order-dependent
```

**After (MemorySegment)**:
```java
// SlottedPageFormat — positional, stateless reads
MemorySegment seg = page.segment();
int slotCount = seg.get(ValueLayout.JAVA_SHORT_UNALIGNED, 0);
int freeEnd = seg.get(ValueLayout.JAVA_SHORT_UNALIGNED, 2);
// ... safe, order-independent
```

### Buffer Pool Integration

The BufferPool manages page frames. With MemorySegment:

```java
class DefaultBufferPool {
    private final Arena arena = Arena.ofShared();  // lives as long as the pool

    Frame allocateFrame() {
        MemorySegment seg = arena.allocate(pageSize);
        return new Frame(seg);
    }

    void close() {
        arena.close();  // deterministically frees ALL frame memory
    }
}
```

### Why This Matters

- **Deterministic deallocation**: When the BufferPool closes, all page memory is freed instantly — no GC delay
- **Safety**: MemorySegment performs bounds checking; accessing beyond the segment throws immediately
- **Performance**: Off-heap segments avoid GC pressure entirely; the JVM doesn't scan them
- **Foundation for mmap**: `FileChannel.map()` returns a `MemorySegment` — enables future memory-mapped I/O

---

## 4. Stream Gatherers

### The Problem

Java Streams provide built-in operations (`map`, `filter`, `reduce`, `collect`). But some useful operations don't fit neatly:

- Sliding windows (e.g., moving average)
- Take-while-cumulative (e.g., "take rows until total > 100")
- Stateful transformations (e.g., deduplication with memory)

Before Java 24, you had to break out of the Stream pipeline and write imperative loops.

### The Solution: Gatherer

`Stream.gather()` (JEP 485, finalized in Java 24) lets you define custom intermediate operations:

```java
// Built-in gatherers
stream.gather(Gatherers.windowFixed(3))     // [1,2,3,4,5] → [[1,2,3],[4,5]]
stream.gather(Gatherers.windowSliding(2))   // [1,2,3,4] → [[1,2],[2,3],[3,4]]
```

A `Gatherer<T,A,R>` has four functions:
- **initializer**: create state `A`
- **integrator**: process each element `T`, optionally emit `R`
- **combiner**: merge parallel states (optional)
- **finisher**: emit remaining elements at end of stream

### Database Relevance

Gatherers are useful for:
- **LIMIT/OFFSET**: Take N elements after skipping M
- **Running aggregates**: SUM-so-far, COUNT-so-far
- **Window functions** (future M22): RANK, ROW_NUMBER, running averages
- **Top-N**: Maintain a bounded heap while streaming

---

## 5. Concurrency Patterns for Databases

### Why Coarse Locking is a Problem

EvolvDB's `DefaultBufferPool` uses `synchronized` on every method:

```java
public synchronized Page getPage(PageId id, boolean forUpdate) { ... }
public synchronized void unpin(PageId id, boolean dirty) { ... }
public synchronized void flush(PageId id) { ... }
```

This means: **only one thread at a time can touch the buffer pool**. Even two concurrent reads of different pages block each other. With virtual threads enabling thousands of concurrent queries, this becomes the bottleneck.

### ReadWriteLock

`ReentrantReadWriteLock` separates read and write access:

```java
private final ReadWriteLock lock = new ReentrantReadWriteLock();

Page getPage(PageId id, boolean forUpdate) {
    lock.readLock().lock();
    try {
        Frame f = frames.get(id);
        if (f != null) return f.page();  // cache hit — no mutation
    } finally {
        lock.readLock().unlock();
    }
    // Cache miss — need write lock to insert
    lock.writeLock().lock();
    try { /* load from disk, insert into frames */ }
    finally { lock.writeLock().unlock(); }
}
```

Multiple readers proceed concurrently. Only writers are exclusive.

### Why Not Just `synchronized`?

With virtual threads, `synchronized` has an additional problem: it **pins the virtual thread** to its carrier thread. If the synchronized block does I/O (disk read, network), the carrier thread is blocked and cannot run other virtual threads. `ReentrantLock` and `ReadWriteLock` do not have this pinning problem.

### Lock Striping (Future)

For even more concurrency, partition the frame map by page ID:

```java
// 16 independent lock stripes
Lock[] stripes = new Lock[16];
Lock stripe(PageId id) { return stripes[id.hashCode() & 15]; }
```

This allows 16 concurrent writers to different pages. We defer this until benchmarks show the ReadWriteLock is the bottleneck.

---

## 6. Mapping to EvolvDB Modules

| Feature | Module(s) Affected | Change Summary |
|---------|-------------------|----------------|
| Virtual Threads | `evolvdb-server` (new), `evolvdb-cli` | TCP server with virtual-thread-per-connection |
| Scoped Values | `evolvdb-common`, `evolvdb-core` | `ScopedValue<TransactionContext>` for future M17 |
| MemorySegment | `evolvdb-storage-disk`, `evolvdb-storage-buffer`, `evolvdb-storage-page` | Replace ByteBuffer with MemorySegment+Arena |
| Stream Gatherers | `evolvdb-exec` | Custom gatherers for pagination, windowing |
| ReadWriteLock | `evolvdb-storage-buffer`, `evolvdb-catalog` | Replace `synchronized` with fine-grained locks |

---

## 7. Interview-Ready Answers

**Q: Why did you migrate to Java 25?**

> Java 25 is the newest LTS release and brings three features critical for databases: Virtual Threads for scaling to thousands of concurrent connections without thread-pool complexity, Scoped Values for safely propagating transaction context through the execution stack, and the Foreign Function & Memory API for deterministic memory management in the buffer pool. We also benefit from Compact Object Headers reducing memory overhead and Generational Shenandoah providing lower GC pause times.

**Q: Why MemorySegment over ByteBuffer?**

> ByteBuffer has three fundamental issues for a buffer pool: no deterministic deallocation (relies on GC finalization), stateful position/limit that's error-prone when shared, and a 2 GB size cap. MemorySegment with Arena gives us deterministic deallocation on pool shutdown, stateless positional access, bounds checking, and no size limit. It's also the foundation for memory-mapped I/O.

**Q: Why Scoped Values instead of ThreadLocal?**

> ThreadLocal is mutable (any code can overwrite it), leaks memory if you forget remove(), and doesn't automatically inherit into virtual threads. ScopedValue is immutable within its scope, automatically cleaned up, and inherited by child virtual threads — making it ideal for transaction context that must flow through the entire operator tree.

**Q: How do virtual threads help a database?**

> Database workloads are I/O-bound: reading pages from disk, waiting for client network responses. With platform threads, blocking wastes an expensive OS thread. Virtual threads are cheap (~200 bytes) and when they block on I/O, the JVM automatically unmounts them and schedules other work on the carrier thread. This means we can use the simple one-thread-per-connection model while scaling to thousands of clients.

---

**Next**: Apply these concepts in the actual migration — see the Java 25 migration plan.
