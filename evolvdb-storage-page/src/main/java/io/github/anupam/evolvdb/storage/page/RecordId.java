package io.github.anupam.evolvdb.storage.page;

import io.github.anupam.evolvdb.storage.disk.PageId;

/** Identifies a record within a page by its slot number. */
public record RecordId(PageId pageId, short slot) {
    public RecordId {
        if (pageId == null) throw new IllegalArgumentException("pageId");
        if (slot < 0) throw new IllegalArgumentException("slot must be >= 0");
    }
}
