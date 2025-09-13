package io.github.anupam.evolvdb.exec;

import io.github.anupam.evolvdb.catalog.CatalogManager;
import io.github.anupam.evolvdb.optimizer.stats.StatsProvider;
import io.github.anupam.evolvdb.optimizer.stats.impl.InMemoryStatsProvider;

/**
 * Execution-time context. In future this will carry transaction/session state.
 */
public final class ExecContext {
    private final CatalogManager catalog;
    private final boolean useOptimizer;
    private final StatsProvider stats;

    public ExecContext(CatalogManager catalog) {
        this.catalog = catalog;
        this.useOptimizer = false;
        this.stats = new InMemoryStatsProvider();
    }

    public ExecContext(CatalogManager catalog, boolean useOptimizer) {
        this.catalog = catalog;
        this.useOptimizer = useOptimizer;
        this.stats = new InMemoryStatsProvider();
    }

    public ExecContext(CatalogManager catalog, boolean useOptimizer, StatsProvider stats) {
        this.catalog = catalog;
        this.useOptimizer = useOptimizer;
        this.stats = (stats == null) ? new InMemoryStatsProvider() : stats;
    }

    public CatalogManager catalog() { return catalog; }
    public boolean useOptimizer() { return useOptimizer; }
    public StatsProvider stats() { return stats; }
}
