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
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class HeapFileScanAndUpdateTest {
    private Path tmpDir;

    private DbConfig cfg() throws IOException {
        tmpDir = Files.createTempDirectory("evolvdb-m5-");
        return DbConfig.builder().pageSize(4096).dataDir(tmpDir).bufferPoolPages(16).build();
    }

    @AfterEach
    void cleanup() throws IOException {
        if (tmpDir != null && Files.exists(tmpDir)) {
            try (var paths = Files.walk(tmpDir)) {
                paths.sorted((a,b)->b.getNameCount()-a.getNameCount()).forEach(p -> {
                    try { Files.deleteIfExists(p); } catch (IOException ignored) {}
                });
            }
        }
    }

    @Test
    void givenMultiplePages_whenScan_thenAllLiveRecordsVisitedInOrder() throws Exception {
        var config = cfg();
        try (var dm = new NioDiskManager(config);
             var bp = new DefaultBufferPool(config, dm)) {
            var fmt = new SlottedPageFormat();
            var hf = new HeapFile(new FileId("scan"), dm, bp, fmt);
            List<RecordId> ids = new ArrayList<>();
            // Insert enough records to span multiple pages
            byte[] rec = new byte[200];
            for (int i = 0; i < 200; i++) rec[i] = (byte) i;
            for (int i = 0; i < 100; i++) ids.add(hf.insert(rec));
            // Tombstone some
            for (int i = 0; i < ids.size(); i += 10) hf.delete(ids.get(i));

            Iterator<RecordId> it = hf.iterator();
            int count = 0;
            while (it.hasNext()) {
                RecordId rid = it.next();
                assertNotNull(rid);
                assertArrayEquals(rec, hf.read(rid));
                count++;
            }
            assertTrue(count < ids.size() && count > 0);
        }
    }

    @Test
    void givenUpdateLarger_whenNotAtEnd_thenRelocateAndRecordIdChanges() throws Exception {
        var config = cfg();
        try (var dm = new NioDiskManager(config);
             var bp = new DefaultBufferPool(config, dm)) {
            var fmt = new SlottedPageFormat();
            var hf = new HeapFile(new FileId("upd1"), dm, bp, fmt);
            byte[] small = new byte[50];
            for (int i = 0; i < small.length; i++) small[i] = (byte) (i+1);
            RecordId rid = hf.insert(small);
            // Insert another to make the first not at end of payload
            hf.insert(small);
            byte[] larger = new byte[120];
            for (int i = 0; i < larger.length; i++) larger[i] = (byte) (255 - (i%255));
            RecordId newRid = hf.update(rid, larger);
            assertNotEquals(rid, newRid);
            assertArrayEquals(larger, hf.read(newRid));
        }
    }

    @Test
    void givenUpdateSmaller_whenShrink_thenInPlaceAndIdStable() throws Exception {
        var config = cfg();
        try (var dm = new NioDiskManager(config);
             var bp = new DefaultBufferPool(config, dm)) {
            var fmt = new SlottedPageFormat();
            var hf = new HeapFile(new FileId("upd2"), dm, bp, fmt);
            byte[] big = new byte[150];
            for (int i = 0; i < big.length; i++) big[i] = (byte) (i % 100);
            RecordId rid = hf.insert(big);
            byte[] small = new byte[60];
            for (int i = 0; i < small.length; i++) small[i] = (byte) (255 - (i % 100));
            RecordId out = hf.update(rid, small);
            assertEquals(rid, out);
            assertArrayEquals(small, hf.read(out));
        }
    }
}
