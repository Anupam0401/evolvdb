package io.github.anupam.evolvdb.storage.buffer.policy;

import io.github.anupam.evolvdb.storage.disk.PageId;
import java.util.function.Predicate;

/**
 * Strategy pattern for choosing a victim page when the buffer pool is full.
 */
public interface EvictionPolicy {
    void onInsert(PageId pageId);
    void onAccess(PageId pageId);
    void onRemove(PageId pageId);
    /**
     * Returns a candidate PageId to evict that satisfies the given predicate (e.g., not pinned),
     * or null if none available.
     */
    PageId evictCandidate(Predicate<PageId> canEvict);
}
