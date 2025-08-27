package io.github.anupam.evolvdb.storage.record;

import io.github.anupam.evolvdb.storage.buffer.BufferPool;
import io.github.anupam.evolvdb.storage.disk.DiskManager;
import io.github.anupam.evolvdb.storage.disk.FileId;
import io.github.anupam.evolvdb.storage.disk.PageId;
import io.github.anupam.evolvdb.storage.page.Page;
import io.github.anupam.evolvdb.storage.page.PageFormat;
import io.github.anupam.evolvdb.storage.page.RecordId;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * HeapFile stores variable-length records across pages using a PageFormat (Strategy).
 * It uses BufferPool for page caching and DiskManager for page allocation.
 */
public final class HeapFile {
    private final FileId fileId;
    private final DiskManager disk;
    private final BufferPool buffer;
    private final PageFormat format;

    public HeapFile(FileId fileId, DiskManager disk, BufferPool buffer, PageFormat format) {
        this.fileId = Objects.requireNonNull(fileId);
        this.disk = Objects.requireNonNull(disk);
        this.buffer = Objects.requireNonNull(buffer);
        this.format = Objects.requireNonNull(format);
    }

    public FileId fileId() { return fileId; }

    /** Inserts a record, allocating and initializing a new page if necessary.
     *  Intentionally avoids assuming any particular PageFormat overhead; it attempts insert and
     *  falls back to the next page on failure (e.g., insufficient space). */
    public RecordId insert(byte[] record) throws IOException {
        Objects.requireNonNull(record);
        int pages = disk.pageCount(fileId);
        // First pass: try to insert into an existing page
        for (int p = 0; p < pages; p++) {
            PageId pid = new PageId(fileId, p);
            Page page = buffer.getPage(pid, true);
            try {
                RecordId rid = format.insert(page, record);
                page.markDirty(true);
                return rid;
            } catch (IllegalStateException noSpace) {
                // Try next page
            } finally {
                buffer.unpin(pid, page.isDirty());
            }
        }
        // None found -> allocate new page
        PageId newPid = disk.allocatePage(fileId);
        Page newPage = buffer.getPage(newPid, true);
        try {
            format.init(newPage);
            RecordId rid = format.insert(newPage, record);
            newPage.markDirty(true);
            return rid;
        } finally {
            buffer.unpin(newPid, newPage.isDirty());
        }
    }

    /** Reads a record or throws if not present (deleted or out of range). */
    public byte[] read(RecordId rid) throws IOException {
        Objects.requireNonNull(rid);
        PageId pid = rid.pageId();
        Page page = buffer.getPage(pid, false);
        try {
            return format.read(page, rid)
                    .orElseThrow(() -> new NoSuchElementException("Record not found: " + rid));
        } finally {
            buffer.unpin(pid, false);
        }
    }

    /** Deletes a record (tombstone). */
    public void delete(RecordId rid) throws IOException {
        Objects.requireNonNull(rid);
        PageId pid = rid.pageId();
        Page page = buffer.getPage(pid, true);
        try {
            format.delete(page, rid);
            page.markDirty(true);
        } finally {
            buffer.unpin(pid, page.isDirty());
        }
    }
}
