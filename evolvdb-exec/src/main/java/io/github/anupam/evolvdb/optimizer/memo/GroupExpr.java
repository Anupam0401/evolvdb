package io.github.anupam.evolvdb.optimizer.memo;

import io.github.anupam.evolvdb.planner.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** One logical expression inside a group, with child group references. */
public final class GroupExpr {
    private final LogicalPlan logical;
    private final List<Group> children;

    public GroupExpr(LogicalPlan logical, List<Group> children) {
        this.logical = logical;
        this.children = new ArrayList<>(children);
    }

    public LogicalPlan logical() { return logical; }
    public List<Group> children() { return Collections.unmodifiableList(children); }
}
