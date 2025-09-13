package io.github.anupam.evolvdb.optimizer.stats;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public final class TableStats {
    private final String tableName;
    private long rowCount;
    private final Map<String, ColumnStats> columns = new HashMap<>(); // key: lowercased column name

    public TableStats(String tableName) {
        this.tableName = tableName;
    }

    public String tableName() { return tableName; }
    public long rowCount() { return rowCount; }
    public TableStats rowCount(long rc) { this.rowCount = rc; return this; }

    public TableStats putColumnStats(String columnName, ColumnStats stats) {
        columns.put(columnName.toLowerCase(Locale.ROOT), stats);
        return this;
    }

    public ColumnStats columnStats(String columnName) {
        if (columnName == null) return null;
        ColumnStats cs = columns.get(columnName.toLowerCase(Locale.ROOT));
        if (cs != null) return cs;
        // try unqualified lookup if qualified
        int dot = columnName.indexOf('.');
        if (dot > 0) {
            cs = columns.get(columnName.substring(dot + 1).toLowerCase(Locale.ROOT));
        }
        return cs;
    }
}
