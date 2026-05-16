package io.github.anupam.evolvdb.storage.buffer;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;

import io.github.anupam.evolvdb.config.DbConfig;
import io.github.anupam.evolvdb.storage.disk.FileId;
import io.github.anupam.evolvdb.storage.disk.NioDiskManager;
import io.github.anupam.evolvdb.storage.disk.PageId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

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
                paths.sorted((a, b) -> b.getNameCount() - a.getNameCount())
                        .forEach(
                                p -> {
                                    try {
                                        Files.deleteIfExists(p);
                                    } catch (IOException _) {
                                    }
                                });
            }
        }
    }

    @Test
    void givenSmallPool_whenThirdPageLoaded_thenEvictsLruAndFlushesDirty() throws Exception {
        var cfg = newConfig(2);
        try (var dm = new NioDiskManager(cfg);
                var bp = new DefaultBufferPool(cfg, dm);
                var arena = Arena.ofConfined()) {
            var file = new FileId("tab");
            PageId p0 = dm.allocatePage(file);
            PageId p1 = dm.allocatePage(file);
            PageId p2 = dm.allocatePage(file);

            byte[] c0 = pattern(cfg.pageSize(), (byte) 1);
            byte[] c1 = pattern(cfg.pageSize(), (byte) 2);
            MemorySegment src0 = arena.allocate(cfg.pageSize());
            MemorySegment src1 = arena.allocate(cfg.pageSize());
            src0.copyFrom(MemorySegment.ofArray(c0));
            src1.copyFrom(MemorySegment.ofArray(c1));
            dm.writePage(p0, src0, 0);
            dm.writePage(p1, src1, 0);

            var pg0 = bp.getPage(p0, true);
            byte[] new0 = pattern(cfg.pageSize(), (byte) 9);
            pg0.segment().copyFrom(MemorySegment.ofArray(new0));
            pg0.markDirty(true);
            bp.unpin(p0, true);

            var pg1 = bp.getPage(p1, false);
            bp.unpin(p1, false);

            var pg2 = bp.getPage(p2, false);
            bp.unpin(p2, false);

            MemorySegment readSeg = arena.allocate(cfg.pageSize());
            dm.readPage(p0, readSeg);
            assertArrayEquals(new0, readSeg.toArray(ValueLayout.JAVA_BYTE));
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

            bp.getPage(p0, false);
            bp.getPage(p1, false);

            IllegalStateException ex =
                    assertThrows(IllegalStateException.class, () -> bp.getPage(p2, false));
            assertTrue(ex.getMessage().contains("No evictable frame"));
        }
    }

    private static byte[] pattern(int n, byte seed) {
        byte[] a = new byte[n];
        for (int i = 0; i < n; i++) a[i] = (byte) (seed + i);
        return a;
    }
}
