package io.github.anupam.evolvdb.storage.page;

import io.github.anupam.evolvdb.storage.disk.FileId;
import io.github.anupam.evolvdb.storage.disk.PageId;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class SlottedPageFormatTest {

    private static final int PAGE_SIZE = 4096;

    private static Page newStubPage() {
        return new Page() {
            private final PageId id = new PageId(new FileId("test"), 0);
            private final ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
            private boolean dirty;

            @Override public PageId id() { return id; }
            @Override public ByteBuffer buffer() { ByteBuffer d = buf.duplicate(); d.clear(); return d; }
            @Override public boolean isDirty() { return dirty; }
            @Override public void markDirty(boolean dirty) { this.dirty = dirty; }
        };
    }

    @Test
    void givenEmptyPage_whenInit_thenHeaderAndFreeSpaceCorrect() {
        var fmt = new SlottedPageFormat();
        var page = newStubPage();
        fmt.init(page);
        int free = fmt.freeSpace(page);
        // Expect PAGE_SIZE - header(12) - slots(0)
        assertEquals(PAGE_SIZE - 12, free);
    }

    @Test
    void givenInsertedRecords_whenRead_thenBytesMatch() {
        var fmt = new SlottedPageFormat();
        var page = newStubPage();
        fmt.init(page);

        byte[] r1 = "hello".getBytes();
        byte[] r2 = "world!".getBytes();

        RecordId id1 = fmt.insert(page, r1);
        RecordId id2 = fmt.insert(page, r2);

        Optional<byte[]> out1 = fmt.read(page, id1);
        Optional<byte[]> out2 = fmt.read(page, id2);

        assertArrayEquals(r1, out1.orElseThrow());
        assertArrayEquals(r2, out2.orElseThrow());
    }

    @Test
    void givenDeletedRecord_whenRead_thenEmpty() {
        var fmt = new SlottedPageFormat();
        var page = newStubPage();
        fmt.init(page);

        byte[] r1 = "foo".getBytes();
        byte[] r2 = "barbaz".getBytes();
        RecordId id1 = fmt.insert(page, r1);
        RecordId id2 = fmt.insert(page, r2);

        fmt.delete(page, id1);

        assertTrue(fmt.read(page, id1).isEmpty());
        assertArrayEquals(r2, fmt.read(page, id2).orElseThrow());
    }

    @Test
    void givenFragmentation_whenCompactionTriggered_thenSpaceReclaimedAndInsertSucceeds() {
        var fmt = new SlottedPageFormat();
        var page = newStubPage();
        fmt.init(page);

        // Insert many small records
        RecordId[] ids = new RecordId[50];
        byte[] small = new byte[50];
        for (int i = 0; i < small.length; i++) small[i] = (byte) i;
        for (int i = 0; i < ids.length; i++) ids[i] = fmt.insert(page, small);

        // Delete every other to create fragmentation
        for (int i = 0; i < ids.length; i += 2) fmt.delete(page, ids[i]);

        // Now insert a large record which should require compaction
        byte[] large = new byte[PAGE_SIZE / 4]; // big enough to force compaction in this setup
        for (int i = 0; i < large.length; i++) large[i] = (byte) (255 - (i % 256));

        RecordId lid = fmt.insert(page, large); // should not throw
        assertArrayEquals(large, fmt.read(page, lid).orElseThrow());
    }
}
