package io.github.anupam.evolvdb.exec;

import io.github.anupam.evolvdb.catalog.CatalogManager;

/**
 * Execution-time context. In future this will carry transaction/session state.
 */
public final class ExecContext {
    private final CatalogManager catalog;
    private final boolean useOptimizer;

    public ExecContext(CatalogManager catalog) {
        this.catalog = catalog;
        this.useOptimizer = false;
    }

    public ExecContext(CatalogManager catalog, boolean useOptimizer) {
        this.catalog = catalog;
        this.useOptimizer = useOptimizer;
    }

    public CatalogManager catalog() { return catalog; }
    public boolean useOptimizer() { return useOptimizer; }
}
