package io.github.anupam.evolvdb.storage.page;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Optional;

import io.github.anupam.evolvdb.storage.disk.FileId;
import io.github.anupam.evolvdb.storage.disk.PageId;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SlottedPageFormatTest {

    private static final int PAGE_SIZE = 4096;
    private static final Arena ARENA = Arena.ofShared();

    private static Page newStubPage() {
        return new Page() {
            private final PageId id = new PageId(new FileId("test"), 0);
            private final MemorySegment seg = ARENA.allocate(PAGE_SIZE);
            private boolean dirty;

            @Override
            public PageId id() {
                return id;
            }

            @Override
            public MemorySegment segment() {
                return seg;
            }

            @Override
            public boolean isDirty() {
                return dirty;
            }

            @Override
            public void markDirty(boolean dirty) {
                this.dirty = dirty;
            }
        };
    }

    @Test
    void givenEmptyPage_whenInit_thenHeaderAndFreeSpaceCorrect() {
        var fmt = new SlottedPageFormat();
        var page = newStubPage();
        fmt.init(page);
        int free = fmt.freeSpace(page);
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

        RecordId[] ids = new RecordId[50];
        byte[] small = new byte[50];
        for (int i = 0; i < small.length; i++) small[i] = (byte) i;
        for (int i = 0; i < ids.length; i++) ids[i] = fmt.insert(page, small);

        for (int i = 0; i < ids.length; i += 2) fmt.delete(page, ids[i]);

        byte[] large = new byte[PAGE_SIZE / 4];
        for (int i = 0; i < large.length; i++) large[i] = (byte) (255 - (i % 256));

        RecordId lid = fmt.insert(page, large);
        assertArrayEquals(large, fmt.read(page, lid).orElseThrow());
    }
}
