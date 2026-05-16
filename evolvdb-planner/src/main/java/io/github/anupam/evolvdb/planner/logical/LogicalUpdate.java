package io.github.anupam.evolvdb.planner.logical;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import io.github.anupam.evolvdb.sql.ast.Expr;
import io.github.anupam.evolvdb.types.Schema;

/** Logical representation of an UPDATE statement. */
public final class LogicalUpdate implements LogicalPlan {
    private final LogicalPlan child;
    private final String tableName;
    private final Map<String, Expr> assignments;
    private final Schema tableSchema;

    public LogicalUpdate(
            LogicalPlan child,
            String tableName,
            Map<String, Expr> assignments,
            Schema tableSchema) {
        this.child = Objects.requireNonNull(child, "child");
        this.tableName = Objects.requireNonNull(tableName, "tableName");
        this.assignments = Map.copyOf(Objects.requireNonNull(assignments, "assignments"));
        this.tableSchema = Objects.requireNonNull(tableSchema, "tableSchema");
    }

    public LogicalPlan child() {
        return child;
    }

    public String tableName() {
        return tableName;
    }

    public Map<String, Expr> assignments() {
        return assignments;
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
        return visitor.visitUpdate(this, context);
    }
}
