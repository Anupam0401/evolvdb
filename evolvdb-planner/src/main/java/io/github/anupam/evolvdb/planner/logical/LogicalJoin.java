package io.github.anupam.evolvdb.planner.logical;

import io.github.anupam.evolvdb.sql.ast.Expr;
import io.github.anupam.evolvdb.types.Schema;

import java.util.List;
import java.util.Objects;

/** Skeleton join node for future use. */
public final class LogicalJoin implements LogicalPlan {
    public enum JoinType { INNER, LEFT, RIGHT, FULL }

    private final LogicalPlan left;
    private final LogicalPlan right;
    private final JoinType type;
    private final Expr condition; // nullable (for cross join)
    private final Schema schema;

    public LogicalJoin(LogicalPlan left, LogicalPlan right, JoinType type, Expr condition, Schema schema) {
        this.left = Objects.requireNonNull(left, "left");
        this.right = Objects.requireNonNull(right, "right");
        this.type = Objects.requireNonNull(type, "type");
        this.condition = condition;
        this.schema = Objects.requireNonNull(schema, "schema");
    }

    public LogicalPlan left() { return left; }
    public LogicalPlan right() { return right; }
    public JoinType type() { return type; }
    public Expr condition() { return condition; }

    @Override public Schema schema() { return schema; }
    @Override public List<LogicalPlan> children() { return List.of(left, right); }
    @Override public <R, C> R accept(LogicalPlanVisitor<R, C> visitor, C context) { return visitor.visitJoin(this, context); }
}
