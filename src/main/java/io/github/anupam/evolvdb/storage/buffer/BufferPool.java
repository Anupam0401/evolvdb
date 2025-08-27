package io.github.anupam.evolvdb.storage.buffer;

import io.github.anupam.evolvdb.storage.disk.PageId;
import io.github.anupam.evolvdb.storage.page.Page;

import java.io.IOException;

/** BufferPool is responsible for caching pages in memory with pin/unpin semantics. */
public interface BufferPool extends AutoCloseable {
    /** Fetches a page, pinning it in the buffer pool. */
    Page getPage(PageId pageId, boolean forUpdate) throws IOException;

    /** Unpins a page, indicating whether it was dirtied. */
    void unpin(PageId pageId, boolean dirty);

    /** Flushes a specific page to disk if dirty. */
    void flush(PageId pageId) throws IOException;

    /** Flushes all pages to disk. */
    void flushAll() throws IOException;

    @Override
    void close() throws IOException;
}
