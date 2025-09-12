package io.github.anupam.evolvdb.optimizer;

import io.github.anupam.evolvdb.exec.ExecContext;
import io.github.anupam.evolvdb.exec.plan.PhysicalPlan;
import io.github.anupam.evolvdb.planner.logical.LogicalPlan;

import java.util.List;
import java.util.Arrays;

/** Minimal Volcano-style optimizer: bottom-up, rule-driven, choose lowest cost. */
public final class VolcanoOptimizer {
    private final CostModel costModel;
    private final List<PhysicalRule> rules;

    public VolcanoOptimizer(CostModel costModel, List<PhysicalRule> rules) {
        this.costModel = costModel;
        this.rules = List.copyOf(rules);
    }

    public PhysicalPlan optimize(LogicalPlan logical, ExecContext ctx) {
        // Bottom-up: optimize children first
        List<LogicalPlan> lchildren = logical.children();
        PhysicalPlan[] optimizedChildren = new PhysicalPlan[lchildren.size()];
        for (int i = 0; i < lchildren.size(); i++) {
            optimizedChildren[i] = optimize(lchildren.get(i), ctx);
        }
        // Apply rules for this node
        PhysicalPlan best = null;
        Cost bestCost = Cost.INFINITE;
        for (PhysicalRule r : rules) {
            if (r.matches(logical)) {
                List<PhysicalPlan> alts = r.apply(logical, Arrays.asList(optimizedChildren), ctx);
                for (PhysicalPlan alt : alts) {
                    Cost c = alt.estimate(costModel);
                    if (c.compareTo(bestCost) < 0) {
                        best = alt; bestCost = c;
                    }
                }
            }
        }
        if (best == null) throw new IllegalArgumentException("No physical alternatives produced for " + logical.getClass().getSimpleName());
        return best;
    }
}
