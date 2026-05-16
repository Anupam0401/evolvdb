package io.github.anupam.evolvdb.storage.page;

import java.lang.foreign.MemorySegment;

import io.github.anupam.evolvdb.storage.disk.PageId;

/** A page is a fixed-size memory segment identified by a PageId. */
public interface Page {
    PageId id();

    MemorySegment segment();

    boolean isDirty();

    void markDirty(boolean dirty);
}
