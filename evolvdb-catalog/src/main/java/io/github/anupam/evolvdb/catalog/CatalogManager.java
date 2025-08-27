package io.github.anupam.evolvdb.catalog;

import io.github.anupam.evolvdb.storage.buffer.BufferPool;
import io.github.anupam.evolvdb.storage.disk.DiskManager;
import io.github.anupam.evolvdb.storage.disk.FileId;
import io.github.anupam.evolvdb.storage.page.PageFormat;
import io.github.anupam.evolvdb.storage.record.HeapFile;
import io.github.anupam.evolvdb.storage.record.RecordManager;
import io.github.anupam.evolvdb.types.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Catalog manager backed by a system HeapFile. Append-only log of UPSERT/DROP records.
 * Rebuilds in-memory index on startup by scanning the catalog file.
 */
public final class CatalogManager {
    public static final String CATALOG_FILE_NAME = "__catalog__";

    private final DiskManager disk;
    private final BufferPool buffer;
    private final PageFormat format;
    private final HeapFile catalogFile;

    private final Map<Long, TableMeta> byId = new HashMap<>();
    private final Map<String, TableMeta> byName = new HashMap<>(); // lower-case key
    private long nextId = 1;

    public CatalogManager(DiskManager disk, BufferPool buffer, PageFormat format) throws IOException {
        this.disk = Objects.requireNonNull(disk);
        this.buffer = Objects.requireNonNull(buffer);
        this.format = Objects.requireNonNull(format);
        RecordManager rm = new RecordManager(disk, buffer);
        this.catalogFile = rm.openHeapFile(CATALOG_FILE_NAME, format);
        load();
    }

    private void load() throws IOException {
        // Scan catalog file to rebuild state
        int pages = disk.pageCount(new FileId(CATALOG_FILE_NAME));
        if (pages == 0) return; // nothing yet
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

    public synchronized TableId createTable(String name, Schema schema) throws IOException {
        Objects.requireNonNull(name);
        Objects.requireNonNull(schema);
        String key = name.toLowerCase(Locale.ROOT);
        if (byName.containsKey(key)) throw new IllegalArgumentException("table already exists: " + name);
        TableId id = new TableId(nextId++);
        FileId file = new FileId("t_" + id.value());
        TableMeta meta = new TableMeta(id, name, schema, file);
        byte[] rec = TableMetaCodec.encodeUpsert(meta);
        catalogFile.insert(rec);
        byId.put(id.value(), meta);
        byName.put(key, meta);
        return id;
    }

    public synchronized Optional<TableMeta> getTable(String name) {
        Objects.requireNonNull(name);
        return Optional.ofNullable(byName.get(name.toLowerCase(Locale.ROOT)));
    }

    public synchronized Optional<TableMeta> getTable(TableId id) {
        Objects.requireNonNull(id);
        return Optional.ofNullable(byId.get(id.value()));
    }

    public synchronized void dropTable(TableId id) throws IOException {
        Objects.requireNonNull(id);
        TableMeta meta = byId.remove(id.value());
        if (meta == null) return; // idempotent
        byName.remove(meta.name().toLowerCase(Locale.ROOT));
        byte[] rec = TableMetaCodec.encodeDrop(id);
        catalogFile.insert(rec);
    }

    public synchronized List<TableMeta> listTables() {
        return Collections.unmodifiableList(new ArrayList<>(byId.values()));
    }
}
