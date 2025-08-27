package io.github.anupam.evolvdb.storage.record;

import io.github.anupam.evolvdb.config.DbConfig;
import io.github.anupam.evolvdb.storage.buffer.DefaultBufferPool;
import io.github.anupam.evolvdb.storage.disk.NioDiskManager;
import io.github.anupam.evolvdb.storage.page.RecordId;
import io.github.anupam.evolvdb.storage.page.SlottedPageFormat;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class RecordManagerTest {
    private Path tmpDir;

    private DbConfig cfg() throws IOException {
        tmpDir = Files.createTempDirectory("evolvdb-rm-");
        return DbConfig.builder().pageSize(4096).dataDir(tmpDir).bufferPoolPages(8).build();
    }

    @AfterEach
    void cleanup() throws IOException {
        if (tmpDir != null && Files.exists(tmpDir)) {
            try (var paths = Files.walk(tmpDir)) {
                paths.sorted((a, b) -> b.getNameCount() - a.getNameCount()).forEach(p -> {
                    try { Files.deleteIfExists(p); } catch (IOException ignored) {}
                });
            }
        }
    }

    @Test
    void givenSameName_whenOpenHeapFileTwice_thenReturnsSameInstance() throws Exception {
        var config = cfg();
        try (var dm = new NioDiskManager(config);
             var bp = new DefaultBufferPool(config, dm)) {
            var rm = new RecordManager(dm, bp);
            var fmt = new SlottedPageFormat();
            HeapFile hf1 = rm.openHeapFile("t", fmt);
            HeapFile hf2 = rm.openHeapFile("t", fmt);
            assertSame(hf1, hf2);
        }
    }

    @Test
    void givenDifferentNames_whenOpen_thenDifferentInstances() throws Exception {
        var config = cfg();
        try (var dm = new NioDiskManager(config);
             var bp = new DefaultBufferPool(config, dm)) {
            var rm = new RecordManager(dm, bp);
            var fmt = new SlottedPageFormat();
            HeapFile a = rm.openHeapFile("a", fmt);
            HeapFile b = rm.openHeapFile("b", fmt);
            assertNotSame(a, b);
        }
    }

    @Test
    void givenHeapFile_fromRecordManager_whenInsertAndRead_thenOk() throws Exception {
        var config = cfg();
        try (var dm = new NioDiskManager(config);
             var bp = new DefaultBufferPool(config, dm)) {
            var rm = new RecordManager(dm, bp);
            var fmt = new SlottedPageFormat();
            HeapFile hf = rm.openHeapFile("t2", fmt);
            byte[] rec = "rm-test".getBytes();
            RecordId rid = hf.insert(rec);
            assertArrayEquals(rec, hf.read(rid));
        }
    }
}
