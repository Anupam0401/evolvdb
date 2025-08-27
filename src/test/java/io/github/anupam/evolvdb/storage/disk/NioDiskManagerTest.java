package io.github.anupam.evolvdb.storage.disk;

import io.github.anupam.evolvdb.config.DbConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

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
                paths.sorted((a,b) -> b.getNameCount()-a.getNameCount()).forEach(p -> {
                    try { Files.deleteIfExists(p); } catch (IOException ignored) {}
                });
            }
        }
    }

    @Test
    void allocateReadWrite() throws Exception {
        var cfg = newConfig();
        try (var dm = new NioDiskManager(cfg)) {
            var file = new FileId("table1");
            var p0 = dm.allocatePage(file);
            var p1 = dm.allocatePage(file);
            assertEquals(0, p0.pageNo());
            assertEquals(1, p1.pageNo());

            byte[] a = new byte[cfg.pageSize()];
            byte[] b = new byte[cfg.pageSize()];
            fillPattern(a, (byte)1);
            fillPattern(b, (byte)2);

            dm.writePage(p0, ByteBuffer.wrap(a), 0);
            dm.writePage(p1, ByteBuffer.wrap(b), 0);

            ByteBuffer ra = ByteBuffer.allocate(cfg.pageSize());
            ByteBuffer rb = ByteBuffer.allocate(cfg.pageSize());
            dm.readPage(p0, ra);
            dm.readPage(p1, rb);

            assertArrayEquals(a, toArray(ra));
            assertArrayEquals(b, toArray(rb));
        }
    }

    @Test
    void persistAcrossInstances() throws Exception {
        var cfg = newConfig();
        var file = new FileId("tableX");
        byte[] payload = new byte[cfg.pageSize()];
        new Random(42).nextBytes(payload);

        PageId pid;
        try (var dm = new NioDiskManager(cfg)) {
            pid = dm.allocatePage(file);
            dm.writePage(pid, ByteBuffer.wrap(payload), 0);
            dm.sync();
        }

        try (var dm2 = new NioDiskManager(cfg)) {
            ByteBuffer read = ByteBuffer.allocate(cfg.pageSize());
            dm2.readPage(pid, read);
            assertArrayEquals(payload, toArray(read));
        }
    }

    private static void fillPattern(byte[] arr, byte value) {
        for (int i = 0; i < arr.length; i++) arr[i] = (byte)(value + i);
    }

    private static byte[] toArray(ByteBuffer buf) {
        buf.flip();
        byte[] out = new byte[buf.remaining()];
        buf.get(out);
        return out;
    }
}
