package io.github.anupam.evolvdb.storage.page;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Optional;

/**
 * Slotted page layout using MemorySegment for structured access.
 *
 * <p>Header (little-endian): int pageType (4 bytes), int lsn (4 bytes), short slotCount (2 bytes),
 * short freeStartOffset (2 bytes) = 12 bytes total.
 *
 * <p>Slots grow from the end of the page backward; payload grows from header forward. Each slot
 * entry: short offset (2 bytes), short len (2 bytes). Negative len indicates tombstone/deleted.
 */
public final class SlottedPageFormat implements PageFormat {
    public static final int PAGE_TYPE_HEAP = 1;

    private static final long OFF_TYPE = 0;
    private static final long OFF_LSN = 4;
    private static final long OFF_SLOT_COUNT = 8;
    private static final long OFF_FREE_START = 10;
    private static final int HEADER_SIZE = 12;
    private static final int SLOT_ENTRY_SIZE = 4;

    private static final ValueLayout.OfInt INT_LE =
            ValueLayout.JAVA_INT_UNALIGNED.withOrder(java.nio.ByteOrder.LITTLE_ENDIAN);
    private static final ValueLayout.OfShort SHORT_LE =
            ValueLayout.JAVA_SHORT_UNALIGNED.withOrder(java.nio.ByteOrder.LITTLE_ENDIAN);

    @Override
    public void init(Page page) {
        MemorySegment seg = page.segment();
        seg.set(INT_LE, OFF_TYPE, PAGE_TYPE_HEAP);
        seg.set(INT_LE, OFF_LSN, 0);
        seg.set(SHORT_LE, OFF_SLOT_COUNT, (short) 0);
        seg.set(SHORT_LE, OFF_FREE_START, (short) HEADER_SIZE);
    }

    @Override
    public int freeSpace(Page page) {
        MemorySegment seg = page.segment();
        int cap = (int) seg.byteSize();
        int slotCount = Short.toUnsignedInt(seg.get(SHORT_LE, OFF_SLOT_COUNT));
        int freeStart = Short.toUnsignedInt(seg.get(SHORT_LE, OFF_FREE_START));
        int slotDirStart = cap - slotCount * SLOT_ENTRY_SIZE;
        return Math.max(0, slotDirStart - freeStart);
    }

    @Override
    public RecordId insert(Page page, byte[] record) {
        MemorySegment seg = page.segment();
        int cap = (int) seg.byteSize();
        int slotCount = Short.toUnsignedInt(seg.get(SHORT_LE, OFF_SLOT_COUNT));
        int freeStart = Short.toUnsignedInt(seg.get(SHORT_LE, OFF_FREE_START));
        int slotDirStart = cap - slotCount * SLOT_ENTRY_SIZE;

        int need = record.length + SLOT_ENTRY_SIZE;
        if ((slotDirStart - freeStart) < need) {
            compactInPlace(seg);
            slotCount = Short.toUnsignedInt(seg.get(SHORT_LE, OFF_SLOT_COUNT));
            freeStart = Short.toUnsignedInt(seg.get(SHORT_LE, OFF_FREE_START));
            slotDirStart = cap - slotCount * SLOT_ENTRY_SIZE;
            if ((slotDirStart - freeStart) < need) {
                throw new PageFullException(need, slotDirStart - freeStart);
            }
        }

        MemorySegment.copy(MemorySegment.ofArray(record), 0, seg, freeStart, record.length);
        int recOffset = freeStart;
        freeStart += record.length;

        int newSlotIndex = slotCount;
        long slotPos = cap - (long) (newSlotIndex + 1) * SLOT_ENTRY_SIZE;
        seg.set(SHORT_LE, slotPos, (short) recOffset);
        seg.set(SHORT_LE, slotPos + 2, (short) record.length);

        seg.set(SHORT_LE, OFF_SLOT_COUNT, (short) (slotCount + 1));
        seg.set(SHORT_LE, OFF_FREE_START, (short) freeStart);

        return new RecordId(page.id(), (short) newSlotIndex);
    }

    @Override
    public Optional<byte[]> read(Page page, RecordId rid) {
        MemorySegment seg = page.segment();
        int cap = (int) seg.byteSize();
        int slotCount = Short.toUnsignedInt(seg.get(SHORT_LE, OFF_SLOT_COUNT));
        int slot = Short.toUnsignedInt(rid.slot());
        if (slot >= slotCount) return Optional.empty();
        long slotPos = cap - (long) (slot + 1) * SLOT_ENTRY_SIZE;
        int off = Short.toUnsignedInt(seg.get(SHORT_LE, slotPos));
        short lenRaw = seg.get(SHORT_LE, slotPos + 2);
        int len = Math.abs(lenRaw);
        if (lenRaw <= 0) return Optional.empty();
        if (off + len > cap) return Optional.empty();
        byte[] out = new byte[len];
        MemorySegment.copy(seg, off, MemorySegment.ofArray(out), 0, len);
        return Optional.of(out);
    }

    @Override
    public void delete(Page page, RecordId rid) {
        MemorySegment seg = page.segment();
        int cap = (int) seg.byteSize();
        int slotCount = Short.toUnsignedInt(seg.get(SHORT_LE, OFF_SLOT_COUNT));
        int slot = Short.toUnsignedInt(rid.slot());
        if (slot >= slotCount) return;
        long slotPos = cap - (long) (slot + 1) * SLOT_ENTRY_SIZE;
        short lenRaw = seg.get(SHORT_LE, slotPos + 2);
        if (lenRaw > 0) {
            seg.set(SHORT_LE, slotPos + 2, (short) -lenRaw);
        }
    }

    @Override
    public int slotCount(Page page) {
        MemorySegment seg = page.segment();
        return Short.toUnsignedInt(seg.get(SHORT_LE, OFF_SLOT_COUNT));
    }

    @Override
    public boolean isLive(Page page, short slotIndex) {
        if (slotIndex < 0) return false;
        MemorySegment seg = page.segment();
        int cap = (int) seg.byteSize();
        int slotCount = Short.toUnsignedInt(seg.get(SHORT_LE, OFF_SLOT_COUNT));
        int idx = Short.toUnsignedInt(slotIndex);
        if (idx >= slotCount) return false;
        long slotPos = cap - (long) (idx + 1) * SLOT_ENTRY_SIZE;
        short lenRaw = seg.get(SHORT_LE, slotPos + 2);
        return lenRaw > 0;
    }

    @Override
    public boolean update(Page page, RecordId rid, byte[] newRecord) {
        MemorySegment seg = page.segment();
        int cap = (int) seg.byteSize();
        int slotCount = Short.toUnsignedInt(seg.get(SHORT_LE, OFF_SLOT_COUNT));
        int slot = Short.toUnsignedInt(rid.slot());
        if (slot >= slotCount) return false;
        long slotPos = cap - (long) (slot + 1) * SLOT_ENTRY_SIZE;
        int off = Short.toUnsignedInt(seg.get(SHORT_LE, slotPos));
        short lenRaw = seg.get(SHORT_LE, slotPos + 2);
        if (lenRaw <= 0) return false;
        int currLen = lenRaw;

        int newLen = newRecord.length;
        if (newLen <= currLen) {
            MemorySegment.copy(MemorySegment.ofArray(newRecord), 0, seg, off, newLen);
            seg.set(SHORT_LE, slotPos + 2, (short) newLen);
            return true;
        }

        int freeStart = Short.toUnsignedInt(seg.get(SHORT_LE, OFF_FREE_START));
        int slotDirStart = cap - slotCount * SLOT_ENTRY_SIZE;
        int extra = newLen - currLen;
        boolean atEnd = (off + currLen) == freeStart;
        boolean haveContiguous = (slotDirStart - freeStart) >= extra;
        if (atEnd && haveContiguous) {
            MemorySegment.copy(MemorySegment.ofArray(newRecord), 0, seg, off, newLen);
            seg.set(SHORT_LE, slotPos + 2, (short) newLen);
            seg.set(SHORT_LE, OFF_FREE_START, (short) (freeStart + extra));
            return true;
        }

        return false;
    }

    private void compactInPlace(MemorySegment seg) {
        int cap = (int) seg.byteSize();
        int slotCount = Short.toUnsignedInt(seg.get(SHORT_LE, OFF_SLOT_COUNT));
        int writePtr = HEADER_SIZE;

        for (int i = 0; i < slotCount; i++) {
            long slotPos = cap - (long) (i + 1) * SLOT_ENTRY_SIZE;
            short lenRaw = seg.get(SHORT_LE, slotPos + 2);
            int len = Math.abs(lenRaw);
            if (lenRaw <= 0) {
                continue;
            }
            int off = Short.toUnsignedInt(seg.get(SHORT_LE, slotPos));
            if (off != writePtr) {
                MemorySegment.copy(seg, off, seg, writePtr, len);
            }
            seg.set(SHORT_LE, slotPos, (short) writePtr);
            writePtr += len;
        }
        seg.set(SHORT_LE, OFF_FREE_START, (short) writePtr);
    }
}
