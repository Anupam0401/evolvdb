package io.github.anupam.evolvdb.types;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Immutable tuple (row) bound to a Schema. Values are validated against column types.
 * Nulls are not supported yet (M7 scope).
 */
public final class Tuple {
    private final Schema schema;
    private final List<Object> values; // sized to schema.columns().size()

    public Tuple(Schema schema, List<?> values) {
        this.schema = Objects.requireNonNull(schema, "schema");
        Objects.requireNonNull(values, "values");
        if (values.size() != schema.size()) {
            throw new IllegalArgumentException("Tuple values size does not match schema");
        }
        this.values = new ArrayList<>(schema.size());
        for (int i = 0; i < schema.size(); i++) {
            var col = schema.columns().get(i);
            Object v = values.get(i);
            validate(col, v);
            this.values.add(v);
        }
    }

    private static void validate(ColumnMeta col, Object v) {
        if (v == null) throw new IllegalArgumentException("Nulls not supported yet");
        switch (col.type()) {
            case INT -> {
                if (!(v instanceof Integer)) throw new IllegalArgumentException(col.name() + " expects INT");
            }
            case BIGINT -> {
                if (!(v instanceof Long)) throw new IllegalArgumentException(col.name() + " expects BIGINT");
            }
            case BOOLEAN -> {
                if (!(v instanceof Boolean)) throw new IllegalArgumentException(col.name() + " expects BOOLEAN");
            }
            case FLOAT -> {
                if (!(v instanceof Float)) throw new IllegalArgumentException(col.name() + " expects FLOAT");
            }
            case STRING -> {
                if (!(v instanceof String)) throw new IllegalArgumentException(col.name() + " expects STRING");
            }
            case VARCHAR -> {
                if (!(v instanceof String s)) throw new IllegalArgumentException(col.name() + " expects VARCHAR");
                int max = Objects.requireNonNull(col.length(), "varchar length");
                if (s.length() > max) throw new IllegalArgumentException(col.name() + " exceeds VARCHAR(" + max + ")");
            }
            default -> throw new IllegalStateException("Unsupported type: " + col.type());
        }
    }

    public Schema schema() { return schema; }
    public List<Object> values() { return List.copyOf(values); }
    public Object get(int idx) { return values.get(idx); }
}
