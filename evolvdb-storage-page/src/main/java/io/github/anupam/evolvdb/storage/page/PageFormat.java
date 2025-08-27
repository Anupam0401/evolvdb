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
}
