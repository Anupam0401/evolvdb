package io.github.anupam.evolvdb.types;

import java.util.Objects;

/** Column metadata. For VARCHAR, length denotes max chars; for fixed-size types, length may be null. */
public record ColumnMeta(String name, Type type, Integer length) {
    public ColumnMeta {
        if (name == null || name.isBlank()) throw new IllegalArgumentException("name must be non-empty");
        Objects.requireNonNull(type, "type");
        if (type == Type.VARCHAR) {
            if (length == null || length <= 0) throw new IllegalArgumentException("VARCHAR requires positive length");
        }
    }
}
