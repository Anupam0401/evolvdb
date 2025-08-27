package io.github.anupam.evolvdb.storage.record;

import io.github.anupam.evolvdb.config.DbConfig;
import io.github.anupam.evolvdb.storage.buffer.DefaultBufferPool;
import io.github.anupam.evolvdb.storage.disk.FileId;
import io.github.anupam.evolvdb.storage.disk.NioDiskManager;
import io.github.anupam.evolvdb.storage.page.RecordId;
import io.github.anupam.evolvdb.storage.page.SlottedPageFormat;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class HeapFileTest {
    private Path tmpDir;

    private DbConfig cfg(int poolPages) throws IOException {
        tmpDir = Files.createTempDirectory("evolvdb-hf-");
        return DbConfig.builder().pageSize(4096).dataDir(tmpDir).bufferPoolPages(poolPages).build();
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
    void givenEmptyFile_whenInsert_thenRecordReadable() throws Exception {
        var config = cfg(8);
        try (var dm = new NioDiskManager(config);
             var bp = new DefaultBufferPool(config, dm)) {
            var fmt = new SlottedPageFormat();
            var hf = new HeapFile(new FileId("t1"), dm, bp, fmt);
            byte[] rec = "hello".getBytes();
            RecordId rid = hf.insert(rec);
            assertArrayEquals(rec, hf.read(rid));
        }
    }

    @Test
    void givenPageFull_whenInsert_thenAllocatesNewPage() throws Exception {
        var config = cfg(4);
        try (var dm = new NioDiskManager(config);
             var bp = new DefaultBufferPool(config, dm)) {
            var fmt = new SlottedPageFormat();
            var hf = new HeapFile(new FileId("t2"), dm, bp, fmt);

            int beforePages = dm.pageCount(new FileId("t2"));
            // Fill one page: record size 100 bytes => max approx floor((4096-12)/104)=39
            byte[] rec = new byte[100];
            for (int i = 0; i < rec.length; i++) rec[i] = (byte) i;
            List<RecordId> ids = new ArrayList<>();
            for (int i = 0; i < 39; i++) ids.add(hf.insert(rec));
            assertEquals(beforePages == 0 ? 1 : beforePages, dm.pageCount(new FileId("t2")));
            // Next insert should allocate a new page
            hf.insert(rec);
            assertEquals((beforePages == 0 ? 2 : beforePages + 1), dm.pageCount(new FileId("t2")));
            // sanity check: read back a record
            assertArrayEquals(rec, hf.read(ids.get(0)));
        }
    }

    @Test
    void givenDeletions_whenInsertLarge_thenCompactionAllowsInsert() throws Exception {
        var config = cfg(8);
        try (var dm = new NioDiskManager(config);
             var bp = new DefaultBufferPool(config, dm)) {
            var fmt = new SlottedPageFormat();
            var hf = new HeapFile(new FileId("t3"), dm, bp, fmt);

            // Insert many small records into one page
            byte[] small = new byte[50];
            for (int i = 0; i < small.length; i++) small[i] = (byte) (i + 1);
            List<RecordId> ids = new ArrayList<>();
            for (int i = 0; i < 50; i++) ids.add(hf.insert(small));

            // Delete every other to fragment
            for (int i = 0; i < ids.size(); i += 2) hf.delete(ids.get(i));

            // Attempt to insert a large record that requires compaction but fits total space
            byte[] large = new byte[4096 / 6];
            for (int i = 0; i < large.length; i++) large[i] = (byte) (255 - (i % 255));
            RecordId lid = hf.insert(large);
            assertArrayEquals(large, hf.read(lid));
        }
    }
}
