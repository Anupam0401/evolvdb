package io.github.anupam.evolvdb.storage.page;

import java.nio.ByteBuffer;

import io.github.anupam.evolvdb.storage.disk.PageId;

/** A page is a fixed-size buffer identified by a PageId. */
public interface Page {
    PageId id();

    ByteBuffer buffer();

    boolean isDirty();

    void markDirty(boolean dirty);
}
