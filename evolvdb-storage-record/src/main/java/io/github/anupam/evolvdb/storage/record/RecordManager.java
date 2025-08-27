package io.github.anupam.evolvdb.storage.record;

import io.github.anupam.evolvdb.storage.buffer.BufferPool;
import io.github.anupam.evolvdb.storage.disk.DiskManager;
import io.github.anupam.evolvdb.storage.disk.FileId;
import io.github.anupam.evolvdb.storage.page.PageFormat;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * RecordManager provides a simple facade to open HeapFiles by name.
 * One HeapFile per fileId/table.
 */
public final class RecordManager {
    private final DiskManager disk;
    private final BufferPool buffer;
    private final Map<String, HeapFile> open = new ConcurrentHashMap<>();

    public RecordManager(DiskManager disk, BufferPool buffer) {
        this.disk = Objects.requireNonNull(disk);
        this.buffer = Objects.requireNonNull(buffer);
    }

    public HeapFile openHeapFile(String name, PageFormat format) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(format);
        return open.computeIfAbsent(name, n -> new HeapFile(new FileId(n), disk, buffer, format));
    }
}
