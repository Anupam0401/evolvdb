package io.github.anupam.evolvdb.storage.disk;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import io.github.anupam.evolvdb.config.DbConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class NioDiskManagerTest {
    private Path tmpDir;

    private DbConfig newConfig() throws IOException {
        tmpDir = Files.createTempDirectory("evolvdb-test-");
        return DbConfig.builder().pageSize(4096).dataDir(tmpDir).build();
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
    void givenAllocatedPages_whenWriteAndRead_thenBytesRoundTrip() throws Exception {
        var cfg = newConfig();
        try (var dm = new NioDiskManager(cfg);
                var arena = Arena.ofConfined()) {
            var file = new FileId("table1");
            var p0 = dm.allocatePage(file);
            var p1 = dm.allocatePage(file);
            assertEquals(0, p0.pageNo());
            assertEquals(1, p1.pageNo());

            byte[] a = new byte[cfg.pageSize()];
            byte[] b = new byte[cfg.pageSize()];
            fillPattern(a, (byte) 1);
            fillPattern(b, (byte) 2);

            MemorySegment srcA = arena.allocate(cfg.pageSize());
            MemorySegment srcB = arena.allocate(cfg.pageSize());
            srcA.copyFrom(MemorySegment.ofArray(a));
            srcB.copyFrom(MemorySegment.ofArray(b));

            dm.writePage(p0, srcA, 0);
            dm.writePage(p1, srcB, 0);

            MemorySegment dstA = arena.allocate(cfg.pageSize());
            MemorySegment dstB = arena.allocate(cfg.pageSize());
            dm.readPage(p0, dstA);
            dm.readPage(p1, dstB);

            assertArrayEquals(a, dstA.toArray(java.lang.foreign.ValueLayout.JAVA_BYTE));
            assertArrayEquals(b, dstB.toArray(java.lang.foreign.ValueLayout.JAVA_BYTE));
        }
    }

    @Test
    void givenWrittenPage_whenReopenManager_thenDataPersists() throws Exception {
        var cfg = newConfig();
        var file = new FileId("tableX");
        byte[] payload = new byte[cfg.pageSize()];
        new Random(42).nextBytes(payload);

        PageId pid;
        try (var dm = new NioDiskManager(cfg);
                var arena = Arena.ofConfined()) {
            pid = dm.allocatePage(file);
            MemorySegment src = arena.allocate(cfg.pageSize());
            src.copyFrom(MemorySegment.ofArray(payload));
            dm.writePage(pid, src, 0);
            dm.sync();
        }

        try (var dm2 = new NioDiskManager(cfg);
                var arena = Arena.ofConfined()) {
            MemorySegment dst = arena.allocate(cfg.pageSize());
            dm2.readPage(pid, dst);
            assertArrayEquals(payload, dst.toArray(java.lang.foreign.ValueLayout.JAVA_BYTE));
        }
    }

    private static void fillPattern(byte[] arr, byte value) {
        for (int i = 0; i < arr.length; i++) arr[i] = (byte) (value + i);
    }
}
