package io.github.anupam.evolvdb.exec.op;

import java.io.IOException;
import java.util.Iterator;

import io.github.anupam.evolvdb.catalog.CatalogManager;
import io.github.anupam.evolvdb.catalog.Table;
import io.github.anupam.evolvdb.storage.page.RecordId;
import io.github.anupam.evolvdb.types.Schema;
import io.github.anupam.evolvdb.types.Tuple;

/**
 * Sequential scan that tracks RecordIds, allowing DML operators (UpdateExec, DeleteExec) to
 * identify which physical record to modify. Used as the leaf operator in UPDATE/DELETE plans.
 */
public final class SeqScanWithRidExec implements PhysicalOperator {
    private final CatalogManager catalog;
    private final String tableName;

    private Table table;
    private Iterator<Table.TupleWithRecordId> it;
    private RecordId currentRid;

    public SeqScanWithRidExec(CatalogManager catalog, String tableName) {
        this.catalog = catalog;
        this.tableName = tableName;
    }

    @Override
    public void open() throws IOException {
        this.table = catalog.openTable(tableName);
        this.it = table.scanTuplesWithRecordIds().iterator();
        this.currentRid = null;
    }

    @Override
    public Tuple next() {
        if (it == null || !it.hasNext()) return null;
        Table.TupleWithRecordId twr = it.next();
        this.currentRid = twr.recordId;
        return twr.tuple;
    }

    @Override
    public void close() {
        this.table = null;
        this.it = null;
        this.currentRid = null;
    }

    @Override
    public Schema schema() {
        if (table != null) return table.schema();
        try {
            return catalog.openTable(tableName).schema();
        } catch (Exception e) {
            throw new IllegalStateException("Unable to resolve schema for table: " + tableName, e);
        }
    }

    @Override
    public RecordId lastRecordId() {
        return currentRid;
    }
}
