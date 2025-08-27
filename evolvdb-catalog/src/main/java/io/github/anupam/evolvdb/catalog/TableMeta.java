package io.github.anupam.evolvdb.catalog;

import io.github.anupam.evolvdb.storage.disk.FileId;
import io.github.anupam.evolvdb.types.Schema;

import java.util.Objects;

/** Immutable table metadata connecting logical schema to physical storage. */
public final class TableMeta {
    private final TableId id;
    private final String name;
    private final Schema schema;
    private final FileId fileId;

    public TableMeta(TableId id, String name, Schema schema, FileId fileId) {
        this.id = Objects.requireNonNull(id, "id");
        if (name == null || name.isBlank()) throw new IllegalArgumentException("name");
        this.name = name;
        this.schema = Objects.requireNonNull(schema, "schema");
        this.fileId = Objects.requireNonNull(fileId, "fileId");
    }

    public TableId id() { return id; }
    public String name() { return name; }
    public Schema schema() { return schema; }
    public FileId fileId() { return fileId; }

    @Override public String toString() {
        return "TableMeta{" + id + ", name='" + name + '\'' + ", fileId=" + fileId + '}';
    }
}
