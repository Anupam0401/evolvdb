package io.github.anupam.evolvdb.planner.logical;

import io.github.anupam.evolvdb.types.Schema;

import java.util.List;
import java.util.Objects;

public final class LogicalProject implements LogicalPlan {
    private final LogicalPlan child;
    private final List<ProjectItem> items; // resolved names
    private final Schema schema;

    public LogicalProject(LogicalPlan child, List<ProjectItem> items, Schema schema) {
        this.child = Objects.requireNonNull(child, "child");
        this.items = List.copyOf(Objects.requireNonNull(items, "items"));
        this.schema = Objects.requireNonNull(schema, "schema");
    }

    public LogicalPlan child() { return child; }
    public List<ProjectItem> items() { return items; }

    @Override public Schema schema() { return schema; }
    @Override public List<LogicalPlan> children() { return List.of(child); }
    @Override public <R, C> R accept(LogicalPlanVisitor<R, C> visitor, C context) { return visitor.visitProject(this, context); }
}
