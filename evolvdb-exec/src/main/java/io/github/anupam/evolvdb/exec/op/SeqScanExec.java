package io.github.anupam.evolvdb.exec.op;

import io.github.anupam.evolvdb.catalog.CatalogManager;
import io.github.anupam.evolvdb.catalog.Table;
import io.github.anupam.evolvdb.types.Schema;
import io.github.anupam.evolvdb.types.Tuple;

import java.io.IOException;
import java.util.Iterator;

/** Sequential scan over a table. */
public final class SeqScanExec implements PhysicalOperator {
    private final CatalogManager catalog;
    private final String tableName;

    private Table table;
    private Iterator<Tuple> it;

    public SeqScanExec(CatalogManager catalog, String tableName) {
        this.catalog = catalog;
        this.tableName = tableName;
    }

    @Override
    public void open() throws IOException {
        this.table = catalog.openTable(tableName);
        this.it = table.scanTuples().iterator();
    }

    @Override
    public Tuple next() {
        if (it == null) return null;
        if (!it.hasNext()) return null;
        return it.next();
    }

    @Override
    public void close() {
        this.table = null;
        this.it = null;
    }

    @Override
    public Schema schema() {
        if (table != null) return table.schema();
        // fallback: when not opened yet, peek schema via catalog
        try {
            return catalog.openTable(tableName).schema();
        } catch (Exception e) {
            throw new IllegalStateException("Unable to resolve schema for table: " + tableName, e);
        }
    }
}
