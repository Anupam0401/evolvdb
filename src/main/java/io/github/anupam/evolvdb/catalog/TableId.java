package io.github.anupam.evolvdb.catalog;

/** Logical identifier for a table. */
public record TableId(String name) {
    public TableId {
        if (name == null || name.isBlank()) throw new IllegalArgumentException("name must be non-empty");
    }
}
