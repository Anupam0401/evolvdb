package io.github.anupam.evolvdb.optimizer;

import java.util.List;

import io.github.anupam.evolvdb.exec.ExecContext;
import io.github.anupam.evolvdb.exec.plan.PhysicalPlan;
import io.github.anupam.evolvdb.planner.logical.LogicalPlan;

/** Produces physical alternatives for a logical node. */
public interface PhysicalRule {
    boolean matches(LogicalPlan logical);

    List<PhysicalPlan> apply(
            LogicalPlan logical, List<PhysicalPlan> optimizedChildren, ExecContext ctx);
}
