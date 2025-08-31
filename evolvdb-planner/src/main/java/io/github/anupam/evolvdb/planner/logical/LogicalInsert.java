package io.github.anupam.evolvdb.planner.logical;

import io.github.anupam.evolvdb.sql.ast.Expr;
import io.github.anupam.evolvdb.types.ColumnMeta;
import io.github.anupam.evolvdb.types.Schema;

import java.util.List;
import java.util.Objects;

/** Logical representation of an INSERT statement. */
public final class LogicalInsert implements LogicalPlan {
    private final String tableName;
    private final List<ColumnMeta> targetColumns; // order of values
    private final List<List<Expr>> rows; // values as expressions (literals or simple exprs)
    private final Schema tableSchema; // full table schema for reference

    public LogicalInsert(String tableName, List<ColumnMeta> targetColumns, List<List<Expr>> rows, Schema tableSchema) {
        this.tableName = Objects.requireNonNull(tableName, "tableName");
        this.targetColumns = List.copyOf(Objects.requireNonNull(targetColumns, "targetColumns"));
        this.rows = List.copyOf(Objects.requireNonNull(rows, "rows"));
        this.tableSchema = Objects.requireNonNull(tableSchema, "tableSchema");
    }

    public String tableName() { return tableName; }
    public List<ColumnMeta> targetColumns() { return targetColumns; }
    public List<List<Expr>> rows() { return rows; }

    @Override public Schema schema() { return tableSchema; }
    @Override public List<LogicalPlan> children() { return List.of(); }
    @Override public <R, C> R accept(LogicalPlanVisitor<R, C> visitor, C context) { return visitor.visitInsert(this, context); }
}
