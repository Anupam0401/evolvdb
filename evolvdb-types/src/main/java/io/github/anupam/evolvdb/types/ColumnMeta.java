package io.github.anupam.evolvdb.types;

import java.util.Objects;

/**
 * Column metadata. For VARCHAR, length denotes max chars; for fixed-size types, length may be null.
 * {@code nullable} controls whether the column accepts SQL NULL values.
 */
public record ColumnMeta(String name, Type type, Integer length, boolean nullable) {
    public ColumnMeta {
        if (name == null || name.isBlank())
            throw new IllegalArgumentException("name must be non-empty");
        Objects.requireNonNull(type, "type");
        if (type == Type.VARCHAR) {
            if (length == null || length <= 0)
                throw new IllegalArgumentException("VARCHAR requires positive length");
        } else {
            if (length != null)
                throw new IllegalArgumentException("length must be null for non-VARCHAR types");
        }
    }

    /** Backward-compatible constructor; defaults to {@code nullable = true}. */
    public ColumnMeta(String name, Type type, Integer length) {
        this(name, type, length, true);
    }
}
