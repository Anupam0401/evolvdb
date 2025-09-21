package io.github.anupam.evolvdb.optimizer.memo;

import io.github.anupam.evolvdb.exec.plan.PhysicalPlan;
import io.github.anupam.evolvdb.optimizer.Cost;
import io.github.anupam.evolvdb.planner.logical.LogicalPlan;

/** A memo group representing a set of logically equivalent expressions. Minimal scaffold. */
public final class Group {
    private final LogicalPlan logical;
    private PhysicalPlan best;
    private Cost bestCost;

    public Group(LogicalPlan logical) { this.logical = logical; }

    public LogicalPlan logical() { return logical; }
    public PhysicalPlan best() { return best; }
    public void best(PhysicalPlan best) { this.best = best; }
    public Cost bestCost() { return bestCost; }
    public void bestCost(Cost c) { this.bestCost = c; }
}
