package io.github.anupam.evolvdb.catalog;

import io.github.anupam.evolvdb.storage.disk.FileId;
import io.github.anupam.evolvdb.types.Schema;

/** Catalog exposes table metadata and logical-to-physical mappings. */
public interface Catalog {
    TableId addTable(String name, Schema schema, FileId fileId);
    boolean hasTable(String name);
    TableId tableId(String name);
    Schema schema(TableId tableId);
    FileId fileId(TableId tableId);
}
