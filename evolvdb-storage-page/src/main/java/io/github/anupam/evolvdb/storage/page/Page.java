package io.github.anupam.evolvdb.storage.page;

import io.github.anupam.evolvdb.storage.disk.PageId;

import java.nio.ByteBuffer;

/** A page is a fixed-size buffer identified by a PageId. */
public interface Page {
    PageId id();
    ByteBuffer buffer();
    boolean isDirty();
    void markDirty(boolean dirty);
}
