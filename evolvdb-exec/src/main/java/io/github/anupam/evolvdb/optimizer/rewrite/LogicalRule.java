package io.github.anupam.evolvdb.optimizer.rewrite;

import io.github.anupam.evolvdb.planner.logical.LogicalPlan;

/** A logical rewrite rule that transforms a logical plan into another equivalent logical plan. */
public interface LogicalRule {
    /** Returns true if the rule is applicable to the given node. */
    boolean matches(LogicalPlan plan);

    /** Applies the rule to the node and returns a (possibly new) node. */
    LogicalPlan apply(LogicalPlan plan);
}
