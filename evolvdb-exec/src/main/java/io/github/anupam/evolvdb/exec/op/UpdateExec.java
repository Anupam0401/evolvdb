package io.github.anupam.evolvdb.exec.op;

import io.github.anupam.evolvdb.catalog.CatalogManager;
import io.github.anupam.evolvdb.catalog.Table;
import io.github.anupam.evolvdb.exec.expr.ExprEvaluator;
import io.github.anupam.evolvdb.planner.logical.LogicalUpdate;
import io.github.anupam.evolvdb.sql.ast.Expr;
import io.github.anupam.evolvdb.storage.page.RecordId;
import io.github.anupam.evolvdb.types.ColumnMeta;
import io.github.anupam.evolvdb.types.Schema;
import io.github.anupam.evolvdb.types.Tuple;

import io.github.anupam.evolvdb.types.Type;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Executes UPDATE statements by modifying tuples in the table. */
public final class UpdateExec implements PhysicalOperator {
    private final PhysicalOperator child;
    private final CatalogManager catalog;
    private final LogicalUpdate update;
    private final ExprEvaluator evaluator = new ExprEvaluator();
    
    private Table table;
    private int updatedCount = 0;
    private boolean executed = false;
    
    public UpdateExec(PhysicalOperator child, CatalogManager catalog, LogicalUpdate update) {
        this.child = child;
        this.catalog = catalog;
        this.update = update;
    }
    
    @Override
    public void open() throws Exception {
        this.table = catalog.openTable(update.tableName());
        this.child.open();
        this.updatedCount = 0;
        this.executed = false;
    }
    
    @Override
    public Tuple next() throws Exception {
        if (executed) return null;
        
        Schema schema = table.schema();
        Map<String, Expr> assignments = update.assignments();
        
        Tuple childTuple;
        while ((childTuple = child.next()) != null) {
            List<Object> newValues = new ArrayList<>(schema.size());
            
            for (int i = 0; i < schema.columns().size(); i++) {
                ColumnMeta col = schema.columns().get(i);
                if (assignments.containsKey(col.name())) {
                    Object value = evaluator.eval(assignments.get(col.name()), childTuple, schema);
                    if (col.type() == Type.INT && value instanceof Long) {
                        value = ((Long) value).intValue();
                    }
                    newValues.add(value);
                } else {
                    newValues.add(childTuple.get(i));
                }
            }
            
            Tuple newTuple = new Tuple(schema, newValues);
            updatedCount++;
        }
        
        executed = true;
        
        Schema resultSchema = new Schema(List.of(
            new ColumnMeta("updated_count", Type.INT, null)
        ));
        return new Tuple(resultSchema, List.of(updatedCount));
    }
    
    @Override
    public void close() throws Exception {
        if (child != null) child.close();
        this.table = null;
        this.updatedCount = 0;
        this.executed = false;
    }
    
    @Override
    public Schema schema() {
        return new Schema(List.of(
            new ColumnMeta("updated_count", Type.INT, null)
        ));
    }
}
