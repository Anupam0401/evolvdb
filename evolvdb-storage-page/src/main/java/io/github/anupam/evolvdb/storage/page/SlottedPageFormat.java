package io.github.anupam.evolvdb.storage.page;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Optional;

/**
 * Slotted page layout.
 * Header (little-endian, binary):
 *  - int pageType (1 for heap)
 *  - int lsn
 *  - short slotCount
 *  - short freeStartOffset
 * Slots grow from the end of the page backward; payload grows from header forward.
 * Each slot entry: short offset, short len (len < 0 indicates tombstone/deleted).
 *
 * This class implements insert/read/delete, free space tracking, and compaction on demand.
 */
public final class SlottedPageFormat implements PageFormat {
    public static final int PAGE_TYPE_HEAP = 1;

    private static final int OFF_TYPE = 0;          // int
    private static final int OFF_LSN = 4;           // int
    private static final int OFF_SLOT_COUNT = 8;    // short
    private static final int OFF_FREE_START = 10;   // short
    private static final int HEADER_SIZE = 12;
    private static final int SLOT_ENTRY_SIZE = 4;   // short offset, short len

    @Override
    public void init(Page page) {
        ByteBuffer buf = page.buffer();
        buf.order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(OFF_TYPE, PAGE_TYPE_HEAP);
        buf.putInt(OFF_LSN, 0);
        buf.putShort(OFF_SLOT_COUNT, (short) 0);
        buf.putShort(OFF_FREE_START, (short) HEADER_SIZE);
    }

    @Override
    public int freeSpace(Page page) {
        ByteBuffer buf = page.buffer();
        buf.order(ByteOrder.LITTLE_ENDIAN);
        int cap = buf.capacity();
        int slotCount = Short.toUnsignedInt(buf.getShort(OFF_SLOT_COUNT));
        int freeStart = Short.toUnsignedInt(buf.getShort(OFF_FREE_START));
        int slotDirStart = cap - slotCount * SLOT_ENTRY_SIZE;
        return Math.max(0, slotDirStart - freeStart);
    }

    @Override
    public RecordId insert(Page page, byte[] record) {
        ByteBuffer buf = page.buffer();
        buf.order(ByteOrder.LITTLE_ENDIAN);
        int cap = buf.capacity();
        int slotCount = Short.toUnsignedInt(buf.getShort(OFF_SLOT_COUNT));
        int freeStart = Short.toUnsignedInt(buf.getShort(OFF_FREE_START));
        int slotDirStart = cap - slotCount * SLOT_ENTRY_SIZE;

        int need = record.length + SLOT_ENTRY_SIZE;
        if ((slotDirStart - freeStart) < need) {
            compactInPlace(buf);
            // reload values after compaction
            slotCount = Short.toUnsignedInt(buf.getShort(OFF_SLOT_COUNT));
            freeStart = Short.toUnsignedInt(buf.getShort(OFF_FREE_START));
            slotDirStart = cap - slotCount * SLOT_ENTRY_SIZE;
            if ((slotDirStart - freeStart) < need) {
                throw new IllegalStateException("Insufficient space for record of " + record.length + " bytes");
            }
        }

        // Write payload
        buf.position(freeStart);
        buf.put(record);
        int recOffset = freeStart;
        freeStart += record.length;

        // Write slot entry at new slot position
        int newSlotIndex = slotCount;
        int slotPos = cap - (newSlotIndex + 1) * SLOT_ENTRY_SIZE;
        buf.putShort(slotPos, (short) recOffset);
        buf.putShort(slotPos + 2, (short) record.length);

        // Update header
        buf.putShort(OFF_SLOT_COUNT, (short) (slotCount + 1));
        buf.putShort(OFF_FREE_START, (short) freeStart);

        return new RecordId(page.id(), (short) newSlotIndex);
    }

    @Override
    public Optional<byte[]> read(Page page, RecordId rid) {
        ByteBuffer buf = page.buffer();
        buf.order(ByteOrder.LITTLE_ENDIAN);
        int cap = buf.capacity();
        int slotCount = Short.toUnsignedInt(buf.getShort(OFF_SLOT_COUNT));
        int slot = Short.toUnsignedInt(rid.slot());
        if (slot >= slotCount) return Optional.empty();
        int slotPos = cap - (slot + 1) * SLOT_ENTRY_SIZE;
        int off = Short.toUnsignedInt(buf.getShort(slotPos));
        short lenRaw = buf.getShort(slotPos + 2);
        int len = Math.abs(lenRaw);
        if (lenRaw <= 0) return Optional.empty();
        if (off + len > cap) return Optional.empty();
        byte[] out = new byte[len];
        int oldPos = buf.position();
        buf.position(off);
        buf.get(out, 0, len);
        buf.position(oldPos);
        return Optional.of(out);
    }

    @Override
    public void delete(Page page, RecordId rid) {
        ByteBuffer buf = page.buffer();
        buf.order(ByteOrder.LITTLE_ENDIAN);
        int cap = buf.capacity();
        int slotCount = Short.toUnsignedInt(buf.getShort(OFF_SLOT_COUNT));
        int slot = Short.toUnsignedInt(rid.slot());
        if (slot >= slotCount) return;
        int slotPos = cap - (slot + 1) * SLOT_ENTRY_SIZE;
        short lenRaw = buf.getShort(slotPos + 2);
        if (lenRaw > 0) {
            buf.putShort(slotPos + 2, (short) -lenRaw);
        }
    }

    /** Packs live records from slots into a contiguous area starting at HEADER_SIZE; updates offsets and freeStart. */
    private void compactInPlace(ByteBuffer buf) {
        buf.order(ByteOrder.LITTLE_ENDIAN);
        int cap = buf.capacity();
        int slotCount = Short.toUnsignedInt(buf.getShort(OFF_SLOT_COUNT));
        int writePtr = HEADER_SIZE;

        for (int i = 0; i < slotCount; i++) {
            int slotPos = cap - (i + 1) * SLOT_ENTRY_SIZE;
            short lenRaw = buf.getShort(slotPos + 2);
            int len = Math.abs(lenRaw);
            if (lenRaw <= 0) {
                continue; // tombstone
            }
            int off = Short.toUnsignedInt(buf.getShort(slotPos));
            // Copy record to writePtr
            byte[] temp = new byte[len];
            int old = buf.position();
            buf.position(off);
            buf.get(temp, 0, len);
            buf.position(writePtr);
            buf.put(temp, 0, len);
            buf.position(old);
            // Update slot offset to new location
            buf.putShort(slotPos, (short) writePtr);
            writePtr += len;
        }
        buf.putShort(OFF_FREE_START, (short) writePtr);
    }
}
