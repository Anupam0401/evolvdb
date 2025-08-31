package io.github.anupam.evolvdb.planner.logical;

import io.github.anupam.evolvdb.types.Schema;

import java.util.List;
import java.util.Objects;

public final class LogicalScan implements LogicalPlan {
    private final String tableName;
    private final String alias; // nullable
    private final Schema schema;

    public LogicalScan(String tableName, String alias, Schema schema) {
        this.tableName = Objects.requireNonNull(tableName, "tableName");
        this.alias = alias;
        this.schema = Objects.requireNonNull(schema, "schema");
    }

    public String tableName() { return tableName; }
    public String alias() { return alias; }

    @Override public Schema schema() { return schema; }
    @Override public List<LogicalPlan> children() { return List.of(); }
    @Override public <R, C> R accept(LogicalPlanVisitor<R, C> visitor, C context) { return visitor.visitScan(this, context); }
}
