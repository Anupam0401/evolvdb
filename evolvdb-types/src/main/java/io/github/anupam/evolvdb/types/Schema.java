package io.github.anupam.evolvdb.types;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Immutable schema: ordered list of columns. */
public final class Schema {
    private final List<ColumnMeta> columns;

    public Schema(List<ColumnMeta> columns) {
        Objects.requireNonNull(columns, "columns");
        if (columns.isEmpty()) throw new IllegalArgumentException("schema must have at least one column");
        this.columns = List.copyOf(columns);
    }

    public List<ColumnMeta> columns() { return Collections.unmodifiableList(columns); }
    public int size() { return columns.size(); }
}
