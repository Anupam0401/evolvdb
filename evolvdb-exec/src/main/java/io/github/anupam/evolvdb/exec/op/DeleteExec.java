package io.github.anupam.evolvdb.exec.op;

import io.github.anupam.evolvdb.catalog.CatalogManager;
import io.github.anupam.evolvdb.catalog.Table;
import io.github.anupam.evolvdb.planner.logical.LogicalDelete;
import io.github.anupam.evolvdb.storage.page.RecordId;
import io.github.anupam.evolvdb.types.ColumnMeta;
import io.github.anupam.evolvdb.types.Schema;
import io.github.anupam.evolvdb.types.Tuple;
import io.github.anupam.evolvdb.types.Type;
import java.util.ArrayList;
import java.util.List;

/** Executes DELETE statements by removing tuples from the table. */
public final class DeleteExec implements PhysicalOperator {
    private final PhysicalOperator child;
    private final CatalogManager catalog;
    private final LogicalDelete delete;
    
    private Table table;
    private int deletedCount = 0;
    private boolean executed = false;
    
    public DeleteExec(PhysicalOperator child, CatalogManager catalog, LogicalDelete delete) {
        this.child = child;
        this.catalog = catalog;
        this.delete = delete;
    }
    
    @Override
    public void open() throws Exception {
        this.table = catalog.openTable(delete.tableName());
        this.child.open();
        this.deletedCount = 0;
        this.executed = false;
    }
    
    @Override
    public Tuple next() throws Exception {
        if (executed) return null;
        
        List<RecordId> ridsToDelete = new ArrayList<>();
        
        for (Table.TupleWithRecordId twr : table.scanTuplesWithRecordIds()) {
            Tuple childTuple = child.next();
            if (childTuple == null) break;
            
            ridsToDelete.add(twr.recordId);
        }
        
        for (RecordId rid : ridsToDelete) {
            table.delete(rid);
            deletedCount++;
        }
        
        executed = true;
        
        Schema resultSchema = new Schema(List.of(
            new ColumnMeta("deleted_count", Type.INT, null)
        ));
        return new Tuple(resultSchema, List.of(deletedCount));
    }
    
    @Override
    public void close() throws Exception {
        if (child != null) child.close();
        this.table = null;
        this.deletedCount = 0;
        this.executed = false;
    }
    
    @Override
    public Schema schema() {
        return new Schema(List.of(
            new ColumnMeta("deleted_count", Type.INT, null)
        ));
    }
}
