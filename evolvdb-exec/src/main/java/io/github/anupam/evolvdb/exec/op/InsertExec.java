package io.github.anupam.evolvdb.exec.op;

import io.github.anupam.evolvdb.catalog.CatalogManager;
import io.github.anupam.evolvdb.catalog.Table;
import io.github.anupam.evolvdb.storage.page.RecordId;
import io.github.anupam.evolvdb.exec.expr.ExprEvaluator;
import io.github.anupam.evolvdb.planner.logical.LogicalInsert;
import io.github.anupam.evolvdb.sql.ast.Expr;
import io.github.anupam.evolvdb.types.ColumnMeta;
import io.github.anupam.evolvdb.types.Schema;
import io.github.anupam.evolvdb.types.Tuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Executes INSERT statements by writing tuples into the table. */
public final class InsertExec implements PhysicalOperator {
    private final CatalogManager catalog;
    private final LogicalInsert insert;
    private final ExprEvaluator evaluator = new ExprEvaluator();
    
    private Table table;
    private int insertedCount = 0;
    private boolean executed = false;
    
    public InsertExec(CatalogManager catalog, LogicalInsert insert) {
        this.catalog = catalog;
        this.insert = insert;
    }
    
    @Override
    public void open() throws IOException {
        this.table = catalog.openTable(insert.tableName());
        this.insertedCount = 0;
        this.executed = false;
    }
    
    @Override
    public Tuple next() throws Exception {
        if (executed) return null;
        
        // Execute all inserts
        Schema tableSchema = insert.schema();
        for (List<Expr> row : insert.rows()) {
            List<Object> values = new ArrayList<>(insert.targetColumns().size());
            
            // Evaluate each expression to get values
            for (int i = 0; i < row.size(); i++) {
                Expr expr = row.get(i);
                Object value = evaluator.eval(expr, null, null);
                // Convert Long to Integer if the target column expects INT
                ColumnMeta targetCol = insert.targetColumns().get(i);
                if (targetCol.type() == io.github.anupam.evolvdb.types.Type.INT && value instanceof Long) {
                    value = ((Long) value).intValue();
                }
                values.add(value);
            }
            
            // If target columns are subset, build full tuple with defaults
            List<Object> fullValues = new ArrayList<>(tableSchema.size());
            for (ColumnMeta col : tableSchema.columns()) {
                int idx = indexOfColumn(insert.targetColumns(), col);
                if (idx >= 0) {
                    fullValues.add(values.get(idx));
                } else {
                    // Would handle defaults here, for now throw error
                    throw new IllegalStateException("Column " + col.name() + " not specified in INSERT and no default");
                }
            }
            
            Tuple tuple = new Tuple(tableSchema, fullValues);
            table.insert(tuple);
            insertedCount++;
        }
        
        executed = true;
        
        // Return a single tuple with the count of inserted rows
        Schema resultSchema = new Schema(List.of(
            new ColumnMeta("inserted_count", io.github.anupam.evolvdb.types.Type.INT, null)
        ));
        return new Tuple(resultSchema, List.of(insertedCount));
    }
    
    @Override
    public void close() {
        this.table = null;
        this.insertedCount = 0;
        this.executed = false;
    }
    
    @Override
    public Schema schema() {
        // INSERT returns a schema with insert count
        return new Schema(List.of(
            new ColumnMeta("inserted_count", io.github.anupam.evolvdb.types.Type.INT, null)
        ));
    }
    
    private static int indexOfColumn(List<ColumnMeta> columns, ColumnMeta target) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).name().equalsIgnoreCase(target.name())) {
                return i;
            }
        }
        return -1;
    }
}
