package io.github.anupam.evolvdb.planner.rules;

import io.github.anupam.evolvdb.planner.logical.*;

import java.util.List;
import java.util.Objects;

/** Applies a sequence of rules to a logical plan (top-down, fixed-point per node). */
public final class RuleEngine {
    private final List<Rule> rules;

    public RuleEngine(List<Rule> rules) {
        this.rules = List.copyOf(Objects.requireNonNull(rules, "rules"));
    }

    public LogicalPlan apply(LogicalPlan root) {
        return rewrite(root);
    }

    private LogicalPlan rewrite(LogicalPlan plan) {
        // 1) rewrite children first
        List<LogicalPlan> kids = plan.children();
        boolean changed = false;
        java.util.ArrayList<LogicalPlan> newKids = new java.util.ArrayList<>(kids.size());
        for (LogicalPlan k : kids) {
            LogicalPlan nk = rewrite(k);
            newKids.add(nk);
            if (nk != k) changed = true;
        }
        LogicalPlan current = changed ? rebuildWithChildren(plan, newKids) : plan;

        // 2) apply rules to current node until no change
        boolean applied;
        do {
            applied = false;
            for (Rule r : rules) {
                if (r.matches(current)) {
                    LogicalPlan next = r.apply(current);
                    if (next != current) {
                        current = next;
                        applied = true;
                        break; // restart rule sequence on the new node
                    }
                }
            }
        } while (applied);
        return current;
    }

    private LogicalPlan rebuildWithChildren(LogicalPlan plan, List<LogicalPlan> kids) {
        if (plan instanceof LogicalFilter f) {
            return new LogicalFilter(kids.get(0), f.predicate());
        } else if (plan instanceof LogicalProject p) {
            return new LogicalProject(kids.get(0), p.items(), p.schema());
        } else if (plan instanceof LogicalJoin j) {
            return new LogicalJoin(kids.get(0), kids.get(1), j.type(), j.condition(), j.schema());
        } else if (plan instanceof LogicalAggregate a) {
            return new LogicalAggregate(kids.get(0), a.groupBy(), a.aggregates(), a.schema());
        } else {
            // Leaf or unsupported rebuild -> return as-is
            return plan;
        }
    }
}
