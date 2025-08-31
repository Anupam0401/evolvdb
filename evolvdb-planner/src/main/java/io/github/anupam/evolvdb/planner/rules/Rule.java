package io.github.anupam.evolvdb.planner.rules;

import io.github.anupam.evolvdb.planner.logical.LogicalPlan;

/** A transformation rule operating on a logical plan. */
public interface Rule {
    boolean matches(LogicalPlan plan);
    LogicalPlan apply(LogicalPlan plan);
}
