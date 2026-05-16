package io.github.anupam.evolvdb.storage.buffer;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import io.github.anupam.evolvdb.config.DbConfig;
import io.github.anupam.evolvdb.storage.buffer.policy.EvictionPolicy;
import io.github.anupam.evolvdb.storage.buffer.policy.LruEvictionPolicy;
import io.github.anupam.evolvdb.storage.disk.DiskManager;
import io.github.anupam.evolvdb.storage.disk.PageId;
import io.github.anupam.evolvdb.storage.page.Page;

/**
 * Default BufferPool with pin/unpin and LRU eviction. Uses Arena-managed MemorySegments and
 * ReadWriteLock for virtual-thread-friendly concurrency.
 */
public final class DefaultBufferPool implements BufferPool {
    private final int pageSize;
    private final int capacity;
    private final DiskManager diskManager;
    private final EvictionPolicy evictionPolicy;
    private final Arena arena;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final Map<PageId, Frame> frames = new HashMap<>();

    public DefaultBufferPool(DbConfig config, DiskManager diskManager) {
        this(config, diskManager, new LruEvictionPolicy());
    }

    public DefaultBufferPool(
            DbConfig config, DiskManager diskManager, EvictionPolicy evictionPolicy) {
        this.pageSize = Objects.requireNonNull(config).pageSize();
        this.capacity = Objects.requireNonNull(config).bufferPoolPages();
        this.diskManager = Objects.requireNonNull(diskManager);
        this.evictionPolicy = Objects.requireNonNull(evictionPolicy);
        this.arena = Arena.ofShared();
    }

    @Override
    public Page getPage(PageId pageId, boolean forUpdate) throws IOException {
        lock.readLock().lock();
        try {
            Frame f = frames.get(pageId);
            if (f != null) {
                f.pinCount++;
                evictionPolicy.onAccess(pageId);
                return f.asPage();
            }
        } finally {
            lock.readLock().unlock();
        }
        lock.writeLock().lock();
        try {
            Frame f = frames.get(pageId);
            if (f != null) {
                f.pinCount++;
                evictionPolicy.onAccess(pageId);
                return f.asPage();
            }
            if (frames.size() >= capacity) {
                evictOne();
            }
            Frame nf = new Frame(pageId, arena.allocate(pageSize));
            diskManager.readPage(pageId, nf.segment);
            nf.pinCount = 1;
            frames.put(pageId, nf);
            evictionPolicy.onInsert(pageId);
            return nf.asPage();
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void evictOne() throws IOException {
        PageId victim =
                evictionPolicy.evictCandidate(
                        id -> {
                            Frame fr = frames.get(id);
                            return fr != null && fr.pinCount == 0;
                        });
        if (victim == null) {
            throw new IllegalStateException("No evictable frame available (all pinned)");
        }
        Frame vf = frames.remove(victim);
        evictionPolicy.onRemove(victim);
        if (vf.dirty) {
            diskManager.writePage(victim, vf.segment, 0);
        }
    }

    @Override
    public void unpin(PageId pageId, boolean dirty) {
        lock.writeLock().lock();
        try {
            Frame f = frames.get(pageId);
            if (f == null) throw new IllegalArgumentException("Page not in buffer: " + pageId);
            if (f.pinCount <= 0)
                throw new IllegalStateException("Page already unpinned: " + pageId);
            f.pinCount--;
            if (dirty) f.dirty = true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void flush(PageId pageId) throws IOException {
        lock.writeLock().lock();
        try {
            Frame f = frames.get(pageId);
            if (f == null) return;
            if (f.dirty) {
                diskManager.writePage(pageId, f.segment, 0);
                f.dirty = false;
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void flushAll() throws IOException {
        lock.writeLock().lock();
        try {
            IOException first = null;
            for (var e : frames.entrySet()) {
                try {
                    Frame f = e.getValue();
                    if (f.dirty) {
                        diskManager.writePage(e.getKey(), f.segment, 0);
                        f.dirty = false;
                    }
                } catch (IOException ex) {
                    if (first == null) first = ex;
                }
            }
            if (first != null) throw first;
            diskManager.sync();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        lock.writeLock().lock();
        try {
            flushAll();
            frames.clear();
            arena.close();
        } finally {
            lock.writeLock().unlock();
        }
    }

    private final class Frame {
        final PageId id;
        final MemorySegment segment;
        int pinCount = 0;
        boolean dirty = false;

        Frame(PageId id, MemorySegment segment) {
            this.id = id;
            this.segment = segment;
        }

        Page asPage() {
            return new Page() {
                @Override
                public PageId id() {
                    return id;
                }

                @Override
                public MemorySegment segment() {
                    return segment;
                }

                @Override
                public boolean isDirty() {
                    return dirty;
                }

                @Override
                public void markDirty(boolean d) {
                    dirty = d;
                }
            };
        }
    }
}
