package io.github.anupam.evolvdb.storage.buffer.policy;

import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.function.Predicate;

import io.github.anupam.evolvdb.storage.disk.PageId;

/**
 * LRU implementation using a LinkedHashSet to track recency. onAccess moves the pageId to the
 * most-recent end.
 */
public final class LruEvictionPolicy implements EvictionPolicy {
    private final LinkedHashSet<PageId> order = new LinkedHashSet<>();

    @Override
    public void onInsert(PageId pageId) {
        Objects.requireNonNull(pageId);
        order.remove(pageId);
        order.add(pageId);
    }

    @Override
    public void onAccess(PageId pageId) {
        Objects.requireNonNull(pageId);
        if (order.remove(pageId)) {
            order.add(pageId);
        }
    }

    @Override
    public void onRemove(PageId pageId) {
        order.remove(pageId);
    }

    @Override
    public PageId evictCandidate(Predicate<PageId> canEvict) {
        for (PageId id : order) {
            if (canEvict.test(id)) {
                return id; // caller will remove
            }
        }
        return null;
    }
}
