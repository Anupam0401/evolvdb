package io.github.anupam.evolvdb.planner.logical;

import io.github.anupam.evolvdb.types.Schema;

import java.util.List;

/** Base interface for all logical plan nodes. Immutable. */
public interface LogicalPlan {
    Schema schema();
    List<LogicalPlan> children();
    <R, C> R accept(LogicalPlanVisitor<R, C> visitor, C context);
}
