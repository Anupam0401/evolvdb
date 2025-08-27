package io.github.anupam.evolvdb.storage.buffer;

import io.github.anupam.evolvdb.config.DbConfig;
import io.github.anupam.evolvdb.storage.disk.FileId;
import io.github.anupam.evolvdb.storage.disk.NioDiskManager;
import io.github.anupam.evolvdb.storage.disk.PageId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class DefaultBufferPoolTest {
    private Path tmpDir;

    private DbConfig newConfig(int poolPages) throws IOException {
        tmpDir = Files.createTempDirectory("evolvdb-buf-");
        return DbConfig.builder().pageSize(4096).dataDir(tmpDir).bufferPoolPages(poolPages).build();
    }

    @AfterEach
    void cleanup() throws IOException {
        if (tmpDir != null && Files.exists(tmpDir)) {
            try (var paths = Files.walk(tmpDir)) {
                paths.sorted((a,b) -> b.getNameCount()-a.getNameCount()).forEach(p -> {
                    try { Files.deleteIfExists(p); } catch (IOException ignored) {}
                });
            }
        }
    }

    @Test
    void givenSmallPool_whenThirdPageLoaded_thenEvictsLruAndFlushesDirty() throws Exception {
        var cfg = newConfig(2);
        try (var dm = new NioDiskManager(cfg);
             var bp = new DefaultBufferPool(cfg, dm)) {
            var file = new FileId("tab");
            PageId p0 = dm.allocatePage(file);
            PageId p1 = dm.allocatePage(file);
            PageId p2 = dm.allocatePage(file);

            // Write initial content so we can distinguish later
            byte[] c0 = pattern(cfg.pageSize(), (byte) 1);
            byte[] c1 = pattern(cfg.pageSize(), (byte) 2);
            dm.writePage(p0, ByteBuffer.wrap(c0), 0);
            dm.writePage(p1, ByteBuffer.wrap(c1), 0);

            // Load p0 and modify it (make it dirty)
            var pg0 = bp.getPage(p0, true);
            ByteBuffer b0 = pg0.buffer();
            b0.clear();
            byte[] new0 = pattern(cfg.pageSize(), (byte) 9);
            b0.put(new0);
            pg0.markDirty(true);
            bp.unpin(p0, true);

            // Load p1 (most recent), unpinned
            var pg1 = bp.getPage(p1, false);
            bp.unpin(p1, false);

            // Now load p2 -> should evict p0 (LRU) and flush dirty content
            var pg2 = bp.getPage(p2, false);
            bp.unpin(p2, false);

            // Verify p0 content on disk equals new0 (flushed on eviction)
            ByteBuffer read = ByteBuffer.allocate(cfg.pageSize());
            dm.readPage(p0, read);
            assertArrayEquals(new0, toArray(read));
        }
    }

    @Test
    void givenAllPinned_whenLoadNewPage_thenThrowsNoEvictable() throws Exception {
        var cfg = newConfig(2);
        try (var dm = new NioDiskManager(cfg);
             var bp = new DefaultBufferPool(cfg, dm)) {
            var file = new FileId("tab2");
            PageId p0 = dm.allocatePage(file);
            PageId p1 = dm.allocatePage(file);
            PageId p2 = dm.allocatePage(file);

            bp.getPage(p0, false); // pinned
            bp.getPage(p1, false); // pinned

            IllegalStateException ex = assertThrows(IllegalStateException.class, () -> bp.getPage(p2, false));
            assertTrue(ex.getMessage().contains("No evictable frame"));
        }
    }

    private static byte[] pattern(int n, byte seed) {
        byte[] a = new byte[n];
        for (int i = 0; i < n; i++) a[i] = (byte) (seed + i);
        return a;
    }

    private static byte[] toArray(ByteBuffer buf) {
        buf.flip();
        byte[] out = new byte[buf.remaining()];
        buf.get(out);
        return out;
    }
}
