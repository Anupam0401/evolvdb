package io.github.anupam.evolvdb.planner.logical;

import io.github.anupam.evolvdb.sql.ast.Expr;
import io.github.anupam.evolvdb.types.Schema;

import java.util.List;
import java.util.Objects;

public final class LogicalFilter implements LogicalPlan {
    private final LogicalPlan child;
    private final Expr predicate; // bound to child's schema context

    public LogicalFilter(LogicalPlan child, Expr predicate) {
        this.child = Objects.requireNonNull(child, "child");
        this.predicate = Objects.requireNonNull(predicate, "predicate");
    }

    public LogicalPlan child() { return child; }
    public Expr predicate() { return predicate; }

    @Override public Schema schema() { return child.schema(); }
    @Override public List<LogicalPlan> children() { return List.of(child); }
    @Override public <R, C> R accept(LogicalPlanVisitor<R, C> visitor, C context) { return visitor.visitFilter(this, context); }
}
