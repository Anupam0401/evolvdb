package io.github.anupam.evolvdb.storage.disk;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * DiskManager abstracts page-level I/O on files. It does not buffer; that's BufferPool's job.
 */
public interface DiskManager extends AutoCloseable {
    /** Allocates a new page at the end of the given file and returns its PageId. */
    PageId allocatePage(FileId fileId) throws IOException;

    /** Reads exactly one page into dst (must have at least pageSize remaining). */
    void readPage(PageId pageId, ByteBuffer dst) throws IOException;

    /** Writes exactly one page from src (must have at least pageSize remaining). */
    void writePage(PageId pageId, ByteBuffer src, long lsn) throws IOException;

    /** Flushes data to stable storage. */
    void sync() throws IOException;

    @Override
    void close() throws IOException;
}
