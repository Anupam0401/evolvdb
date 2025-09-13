package io.github.anupam.evolvdb.optimizer.stats;

public interface StatsProvider {
    TableStats getTableStats(String tableName);
    default ColumnStats getColumnStats(String tableName, String columnName) {
        TableStats t = getTableStats(tableName);
        return t == null ? null : t.columnStats(columnName);
    }
}
