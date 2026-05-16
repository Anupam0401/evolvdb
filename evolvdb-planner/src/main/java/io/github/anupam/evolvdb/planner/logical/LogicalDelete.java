package io.github.anupam.evolvdb.planner.logical;

import java.util.List;
import java.util.Objects;

import io.github.anupam.evolvdb.types.Schema;

/** Logical representation of a DELETE statement. */
public final class LogicalDelete implements LogicalPlan {
    private final LogicalPlan child;
    private final String tableName;
    private final Schema tableSchema;

    public LogicalDelete(LogicalPlan child, String tableName, Schema tableSchema) {
        this.child = Objects.requireNonNull(child, "child");
        this.tableName = Objects.requireNonNull(tableName, "tableName");
        this.tableSchema = Objects.requireNonNull(tableSchema, "tableSchema");
    }

    public LogicalPlan child() {
        return child;
    }

    public String tableName() {
        return tableName;
    }

    @Override
    public Schema schema() {
        return tableSchema;
    }

    @Override
    public List<LogicalPlan> children() {
        return List.of(child);
    }

    @Override
    public <R, C> R accept(LogicalPlanVisitor<R, C> visitor, C context) {
        return visitor.visitDelete(this, context);
    }
}
