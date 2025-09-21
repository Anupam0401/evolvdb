package io.github.anupam.evolvdb.optimizer.memo;

import io.github.anupam.evolvdb.planner.logical.LogicalPlan;

import java.util.IdentityHashMap;
import java.util.Map;

/** Minimal memo that interns LogicalPlan nodes by identity. */
public final class Memo {
    private final Map<LogicalPlan, Group> groups = new IdentityHashMap<>();

    public Group intern(LogicalPlan logical) {
        return groups.computeIfAbsent(logical, Group::new);
    }
}
