package io.github.anupam.evolvdb.types;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/** Immutable schema: ordered list of columns. */
public final class Schema {
    private final List<ColumnMeta> columns;

    public Schema(List<ColumnMeta> columns) {
        Objects.requireNonNull(columns, "columns");
        if (columns.isEmpty()) throw new IllegalArgumentException("schema must have at least one column");
        // enforce unique column names (case-insensitive)
        Set<String> lowered = columns.stream().map(c -> c.name().toLowerCase()).collect(Collectors.toSet());
        if (lowered.size() != columns.size()) {
            throw new IllegalArgumentException("duplicate column names in schema");
        }
        this.columns = List.copyOf(columns);
    }

    public List<ColumnMeta> columns() { return Collections.unmodifiableList(columns); }
    public int size() { return columns.size(); }
}
