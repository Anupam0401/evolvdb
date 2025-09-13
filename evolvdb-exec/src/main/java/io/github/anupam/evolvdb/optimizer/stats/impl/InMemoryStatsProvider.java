package io.github.anupam.evolvdb.optimizer.stats.impl;

import io.github.anupam.evolvdb.optimizer.stats.ColumnStats;
import io.github.anupam.evolvdb.optimizer.stats.StatsProvider;
import io.github.anupam.evolvdb.optimizer.stats.TableStats;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public final class InMemoryStatsProvider implements StatsProvider {
    private final Map<String, TableStats> byTable = new HashMap<>();

    @Override
    public TableStats getTableStats(String tableName) {
        if (tableName == null) return null;
        return byTable.get(tableName.toLowerCase(Locale.ROOT));
    }

    public InMemoryStatsProvider putTable(String tableName, long rowCount) {
        TableStats ts = byTable.computeIfAbsent(tableName.toLowerCase(Locale.ROOT), TableStats::new);
        ts.rowCount(rowCount);
        return this;
    }

    public InMemoryStatsProvider putColumn(String tableName, String column, long distinctCount, double nullFraction) {
        TableStats ts = byTable.computeIfAbsent(tableName.toLowerCase(Locale.ROOT), TableStats::new);
        ts.putColumnStats(column, new ColumnStats(distinctCount, nullFraction));
        return this;
    }
}
