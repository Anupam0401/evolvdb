package io.github.anupam.evolvdb.storage.page;

import java.util.Optional;

/**
 * PageFormat defines the record layout within a fixed-size page buffer.
 * Implementations (e.g., SlottedPageFormat) encapsulate insert/read/delete and space management.
 */
public interface PageFormat {
    /** Initializes an empty page with this format's header/metadata. */
    void init(Page page);

    /** Returns the number of contiguous free bytes currently available for new record payloads. */
    int freeSpace(Page page);

    /** Inserts a record payload and returns its RecordId. Throws if insufficient space. */
    RecordId insert(Page page, byte[] record);

    /** Reads a record by id; empty if deleted. */
    Optional<byte[]> read(Page page, RecordId rid);

    /** Deletes a record by id (may tombstone). */
    void delete(Page page, RecordId rid);

    // --- M5 scanning & update helpers ---

    /** Total number of slots currently present (including tombstones). */
    int slotCount(Page page);

    /** True if the given slot index refers to a live record (not deleted). */
    boolean isLive(Page page, short slotIndex);

    /**
     * Attempts an in-place update. Returns true if updated in-place; false if insufficient space
     * or record is deleted. If false, caller should relocate via delete+insert.
     */
    boolean update(Page page, RecordId rid, byte[] newRecord);
}
