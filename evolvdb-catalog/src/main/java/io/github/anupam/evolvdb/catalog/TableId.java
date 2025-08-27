package io.github.anupam.evolvdb.catalog;

/** Logical identifier for a table. */
public record TableId(long value) implements Comparable<TableId> {
    public TableId {
        if (value <= 0) throw new IllegalArgumentException("TableId must be positive");
    }
    @Override public int compareTo(TableId o) { return Long.compare(this.value, o.value); }
    @Override public String toString() { return "TableId{" + value + '}'; }
}
