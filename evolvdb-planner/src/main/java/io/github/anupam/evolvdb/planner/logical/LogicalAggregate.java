package io.github.anupam.evolvdb.planner.logical;

import io.github.anupam.evolvdb.sql.ast.Expr;
import io.github.anupam.evolvdb.types.Schema;

import java.util.List;
import java.util.Objects;

/** Logical aggregate node (group-by keys and aggregate outputs). */
public final class LogicalAggregate implements LogicalPlan {
    private final LogicalPlan child;
    private final List<Expr> groupBy; // grouping expressions
    private final List<ProjectItem> aggregates; // aggregate outputs
    private final Schema schema;

    public LogicalAggregate(LogicalPlan child, List<Expr> groupBy, List<ProjectItem> aggregates, Schema schema) {
        this.child = Objects.requireNonNull(child, "child");
        this.groupBy = List.copyOf(Objects.requireNonNull(groupBy, "groupBy"));
        this.aggregates = List.copyOf(Objects.requireNonNull(aggregates, "aggregates"));
        this.schema = Objects.requireNonNull(schema, "schema");
    }

    public LogicalPlan child() { return child; }
    public List<Expr> groupBy() { return groupBy; }
    public List<ProjectItem> aggregates() { return aggregates; }

    @Override public Schema schema() { return schema; }
    @Override public List<LogicalPlan> children() { return List.of(child); }
    @Override public <R, C> R accept(LogicalPlanVisitor<R, C> visitor, C context) { return visitor.visitAggregate(this, context); }
}
