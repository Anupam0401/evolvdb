package io.github.anupam.evolvdb.catalog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import io.github.anupam.evolvdb.storage.buffer.BufferPool;
import io.github.anupam.evolvdb.storage.disk.DiskManager;
import io.github.anupam.evolvdb.storage.disk.FileId;
import io.github.anupam.evolvdb.storage.page.PageFormat;
import io.github.anupam.evolvdb.storage.record.HeapFile;
import io.github.anupam.evolvdb.storage.record.RecordManager;
import io.github.anupam.evolvdb.types.Schema;

/**
 * Catalog manager backed by a system HeapFile. Append-only log of UPSERT/DROP records. Uses
 * ReadWriteLock for virtual-thread-friendly concurrency (no carrier-thread pinning).
 */
public final class CatalogManager {
    public static final String CATALOG_FILE_NAME = "__catalog__";

    private final DiskManager disk;
    private final BufferPool buffer;
    private final PageFormat format;
    private final HeapFile catalogFile;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final Map<Long, TableMeta> byId = new HashMap<>();
    private final Map<String, TableMeta> byName = new HashMap<>();
    private long nextId = 1;

    public CatalogManager(DiskManager disk, BufferPool buffer, PageFormat format)
            throws IOException {
        this.disk = Objects.requireNonNull(disk);
        this.buffer = Objects.requireNonNull(buffer);
        this.format = Objects.requireNonNull(format);
        RecordManager rm = new RecordManager(disk, buffer);
        this.catalogFile = rm.openHeapFile(CATALOG_FILE_NAME, format);
        load();
    }

    private void load() throws IOException {
        int pages = disk.pageCount(new FileId(CATALOG_FILE_NAME));
        if (pages == 0) return;
        for (var ridIt = catalogFile.iterator(); ridIt.hasNext(); ) {
            var rid = ridIt.next();
            byte[] rec = catalogFile.read(rid);
            var dec = TableMetaCodec.decode(rec);
            if (dec.drop) {
                var meta = byId.remove(dec.id.value());
                if (meta != null) byName.remove(meta.name().toLowerCase(Locale.ROOT));
            } else {
                byId.put(dec.id.value(), dec.meta);
                byName.put(dec.meta.name().toLowerCase(Locale.ROOT), dec.meta);
            }
            if (dec.id.value() >= nextId) nextId = dec.id.value() + 1;
        }
    }

    public TableId createTable(String name, Schema schema) throws IOException {
        Objects.requireNonNull(name);
        Objects.requireNonNull(schema);
        lock.writeLock().lock();
        try {
            String key = name.toLowerCase(Locale.ROOT);
            if (byName.containsKey(key))
                throw new IllegalArgumentException("table already exists: " + name);
            TableId id = new TableId(nextId++);
            FileId file = new FileId("t_" + id.value());
            TableMeta meta = new TableMeta(id, name, schema, file);
            byte[] rec = TableMetaCodec.encodeUpsert(meta);
            catalogFile.insert(rec);
            byId.put(id.value(), meta);
            byName.put(key, meta);
            return id;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public Optional<TableMeta> getTable(String name) {
        Objects.requireNonNull(name);
        lock.readLock().lock();
        try {
            return Optional.ofNullable(byName.get(name.toLowerCase(Locale.ROOT)));
        } finally {
            lock.readLock().unlock();
        }
    }

    public Optional<TableMeta> getTable(TableId id) {
        Objects.requireNonNull(id);
        lock.readLock().lock();
        try {
            return Optional.ofNullable(byId.get(id.value()));
        } finally {
            lock.readLock().unlock();
        }
    }

    public void dropTable(TableId id) throws IOException {
        Objects.requireNonNull(id);
        lock.writeLock().lock();
        try {
            TableMeta meta = byId.remove(id.value());
            if (meta == null) return;
            byName.remove(meta.name().toLowerCase(Locale.ROOT));
            byte[] rec = TableMetaCodec.encodeDrop(id);
            catalogFile.insert(rec);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public List<TableMeta> listTables() {
        lock.readLock().lock();
        try {
            return Collections.unmodifiableList(new ArrayList<>(byId.values()));
        } finally {
            lock.readLock().unlock();
        }
    }

    public Table openTable(String name) throws IOException {
        Objects.requireNonNull(name);
        lock.readLock().lock();
        try {
            TableMeta meta = byName.get(name.toLowerCase(Locale.ROOT));
            if (meta == null) throw new IllegalArgumentException("unknown table: " + name);
            RecordManager rm = new RecordManager(disk, buffer);
            HeapFile hf = rm.openHeapFile(meta.fileId().name(), format);
            return new Table(meta, hf);
        } finally {
            lock.readLock().unlock();
        }
    }

    public Table openTable(TableId id) throws IOException {
        Objects.requireNonNull(id);
        lock.readLock().lock();
        try {
            TableMeta meta = byId.get(id.value());
            if (meta == null) throw new IllegalArgumentException("unknown table id: " + id);
            RecordManager rm = new RecordManager(disk, buffer);
            HeapFile hf = rm.openHeapFile(meta.fileId().name(), format);
            return new Table(meta, hf);
        } finally {
            lock.readLock().unlock();
        }
    }
}
