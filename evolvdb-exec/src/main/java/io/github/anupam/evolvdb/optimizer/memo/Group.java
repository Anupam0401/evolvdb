package io.github.anupam.evolvdb.optimizer.memo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.github.anupam.evolvdb.exec.plan.PhysicalPlan;
import io.github.anupam.evolvdb.optimizer.Cost;
import io.github.anupam.evolvdb.planner.logical.LogicalPlan;

/** A memo group representing a set of logically equivalent expressions. */
public final class Group {
    private LogicalPlan logical; // representative (first-added) logical expression
    private final List<GroupExpr> exprs = new ArrayList<>();
    private PhysicalPlan best;
    private Cost bestCost;

    public Group() {}

    public LogicalPlan logical() {
        return logical;
    }

    public List<GroupExpr> expressions() {
        return Collections.unmodifiableList(exprs);
    }

    public void addExpr(GroupExpr e) {
        if (this.logical == null) this.logical = e.logical();
        this.exprs.add(e);
    }

    public PhysicalPlan best() {
        return best;
    }

    public void best(PhysicalPlan best) {
        this.best = best;
    }

    public Cost bestCost() {
        return bestCost;
    }

    public void bestCost(Cost c) {
        this.bestCost = c;
    }
}
