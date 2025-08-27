package io.github.anupam.evolvdb.storage.buffer;

import io.github.anupam.evolvdb.config.DbConfig;
import io.github.anupam.evolvdb.storage.buffer.policy.EvictionPolicy;
import io.github.anupam.evolvdb.storage.buffer.policy.LruEvictionPolicy;
import io.github.anupam.evolvdb.storage.disk.DiskManager;
import io.github.anupam.evolvdb.storage.disk.PageId;
import io.github.anupam.evolvdb.storage.page.Page;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Default BufferPool with pin/unpin and LRU eviction (Strategy).
 */
public final class DefaultBufferPool implements BufferPool {
    private final int pageSize;
    private final int capacity;
    private final DiskManager diskManager;
    private final EvictionPolicy evictionPolicy;

    private final Map<PageId, Frame> frames = new HashMap<>();

    public DefaultBufferPool(DbConfig config, DiskManager diskManager) {
        this(config, diskManager, new LruEvictionPolicy());
    }

    public DefaultBufferPool(DbConfig config, DiskManager diskManager, EvictionPolicy evictionPolicy) {
        this.pageSize = Objects.requireNonNull(config).pageSize();
        this.capacity = Objects.requireNonNull(config).bufferPoolPages();
        this.diskManager = Objects.requireNonNull(diskManager);
        this.evictionPolicy = Objects.requireNonNull(evictionPolicy);
    }

    @Override
    public synchronized Page getPage(PageId pageId, boolean forUpdate) throws IOException {
        Frame f = frames.get(pageId);
        if (f != null) {
            f.pinCount++;
            evictionPolicy.onAccess(pageId);
            return f.asPage();
        }
        // Need to load
        if (frames.size() >= capacity) {
            evictOne();
        }
        // Load from disk
        Frame nf = new Frame(pageId, ByteBuffer.allocate(pageSize));
        ByteBuffer dst = nf.buffer.duplicate();
        dst.clear();
        diskManager.readPage(pageId, dst);
        nf.pinCount = 1;
        frames.put(pageId, nf);
        evictionPolicy.onInsert(pageId);
        return nf.asPage();
    }

    private void evictOne() throws IOException {
        PageId victim = evictionPolicy.evictCandidate(id -> {
            Frame fr = frames.get(id);
            return fr != null && fr.pinCount == 0;
        });
        if (victim == null) {
            throw new IllegalStateException("No evictable frame available (all pinned)");
        }
        Frame vf = frames.remove(victim);
        evictionPolicy.onRemove(victim);
        if (vf.dirty) {
            ByteBuffer src = vf.buffer.duplicate();
            src.clear();
            diskManager.writePage(victim, src, 0);
        }
    }

    @Override
    public synchronized void unpin(PageId pageId, boolean dirty) {
        Frame f = frames.get(pageId);
        if (f == null) throw new IllegalArgumentException("Page not in buffer: " + pageId);
        if (f.pinCount <= 0) throw new IllegalStateException("Page already unpinned: " + pageId);
        f.pinCount--;
        if (dirty) f.dirty = true;
    }

    @Override
    public synchronized void flush(PageId pageId) throws IOException {
        Frame f = frames.get(pageId);
        if (f == null) return;
        if (f.dirty) {
            ByteBuffer src = f.buffer.duplicate();
            src.clear();
            diskManager.writePage(pageId, src, 0);
            f.dirty = false;
        }
    }

    @Override
    public synchronized void flushAll() throws IOException {
        IOException first = null;
        for (var e : frames.entrySet()) {
            try {
                flush(e.getKey());
            } catch (IOException ex) {
                if (first == null) first = ex;
            }
        }
        if (first != null) throw first;
        diskManager.sync();
    }

    @Override
    public synchronized void close() throws IOException {
        flushAll();
        frames.clear();
    }

    private final class Frame {
        final PageId id;
        final ByteBuffer buffer;
        int pinCount = 0;
        boolean dirty = false;

        Frame(PageId id, ByteBuffer buffer) {
            this.id = id;
            this.buffer = buffer;
        }

        Page asPage() {
            return new Page() {
                @Override
                public PageId id() { return id; }
                @Override
                public ByteBuffer buffer() {
                    ByteBuffer dup = buffer.duplicate();
                    dup.clear();
                    return dup;
                }
                @Override
                public boolean isDirty() { return dirty; }
                @Override
                public void markDirty(boolean d) { dirty = d; }
            };
        }
    }
}
