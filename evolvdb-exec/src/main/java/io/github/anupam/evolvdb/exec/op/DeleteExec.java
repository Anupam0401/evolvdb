package io.github.anupam.evolvdb.exec.op;

import java.util.ArrayList;
import java.util.List;

import io.github.anupam.evolvdb.catalog.CatalogManager;
import io.github.anupam.evolvdb.catalog.Table;
import io.github.anupam.evolvdb.exec.expr.ExprEvaluator;
import io.github.anupam.evolvdb.planner.logical.LogicalDelete;
import io.github.anupam.evolvdb.planner.logical.LogicalFilter;
import io.github.anupam.evolvdb.planner.logical.LogicalPlan;
import io.github.anupam.evolvdb.sql.ast.Expr;
import io.github.anupam.evolvdb.storage.page.RecordId;
import io.github.anupam.evolvdb.types.ColumnMeta;
import io.github.anupam.evolvdb.types.Schema;
import io.github.anupam.evolvdb.types.Tuple;
import io.github.anupam.evolvdb.types.Type;

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
        this.deletedCount = 0;
        this.executed = false;
    }

    @Override
    public Tuple next() throws Exception {
        if (executed) return null;

        Schema schema = table.schema();
        Expr whereFilter = extractFilter(delete.child());
        ExprEvaluator evaluator = new ExprEvaluator();

        List<RecordId> ridsToDelete = new ArrayList<>();

        // Scan table with RecordIds and apply filter
        for (Table.TupleWithRecordId twr : table.scanTuplesWithRecordIds()) {
            // Check if tuple matches WHERE clause
            if (whereFilter != null) {
                Object result = evaluator.eval(whereFilter, twr.tuple, schema);
                if (result == null || !((Boolean) result)) {
                    continue; // Skip non-matching rows
                }
            }

            ridsToDelete.add(twr.recordId);
        }

        // Perform actual deletes
        for (RecordId rid : ridsToDelete) {
            table.delete(rid);
            deletedCount++;
        }

        executed = true;

        Schema resultSchema = new Schema(List.of(new ColumnMeta("deleted_count", Type.INT, null)));
        return new Tuple(resultSchema, List.of(deletedCount));
    }

    private Expr extractFilter(LogicalPlan plan) {
        if (plan instanceof LogicalFilter) {
            LogicalFilter filter = (LogicalFilter) plan;
            return filter.predicate();
        }
        return null;
    }

    @Override
    public void close() throws Exception {
        this.table = null;
        this.deletedCount = 0;
        this.executed = false;
    }

    @Override
    public Schema schema() {
        return new Schema(List.of(new ColumnMeta("deleted_count", Type.INT, null)));
    }
}
