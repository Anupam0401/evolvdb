package io.github.anupam.evolvdb.storage.disk;

/** Identifies a page within a file. Page numbers are 0-based. */
public record PageId(FileId fileId, int pageNo) {
    public PageId {
        if (fileId == null) throw new IllegalArgumentException("fileId");
        if (pageNo < 0) throw new IllegalArgumentException("pageNo must be >= 0");
    }
}
