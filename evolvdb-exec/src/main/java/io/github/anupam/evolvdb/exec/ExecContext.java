package io.github.anupam.evolvdb.exec;

import io.github.anupam.evolvdb.catalog.CatalogManager;

/**
 * Execution-time context. In future this will carry transaction/session state.
 */
public final class ExecContext {
    private final CatalogManager catalog;

    public ExecContext(CatalogManager catalog) {
        this.catalog = catalog;
    }

    public CatalogManager catalog() { return catalog; }
}
