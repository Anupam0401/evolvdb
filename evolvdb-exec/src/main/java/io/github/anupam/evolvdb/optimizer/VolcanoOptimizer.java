package io.github.anupam.evolvdb.optimizer;

import io.github.anupam.evolvdb.exec.ExecContext;
import io.github.anupam.evolvdb.exec.plan.PhysicalPlan;
import io.github.anupam.evolvdb.planner.logical.LogicalPlan;
import io.github.anupam.evolvdb.exec.plan.HashJoinPlan;
import io.github.anupam.evolvdb.exec.plan.SortMergeJoinPlan;
import io.github.anupam.evolvdb.exec.plan.NestedLoopJoinPlan;
import io.github.anupam.evolvdb.optimizer.memo.Memo;
import io.github.anupam.evolvdb.optimizer.memo.Group;

import java.util.List;
import java.util.Arrays;

/** Minimal Volcano-style optimizer: bottom-up, rule-driven, choose lowest cost. */
public final class VolcanoOptimizer {
    private final CostModel costModel;
    private final List<PhysicalRule> rules;
    private final boolean useMemo;
    private final Memo memo;

    public VolcanoOptimizer(CostModel costModel, List<PhysicalRule> rules) {
        this(costModel, rules, false);
    }

    public VolcanoOptimizer(CostModel costModel, List<PhysicalRule> rules, boolean useMemo) {
        this.costModel = costModel;
        this.rules = List.copyOf(rules);
        this.useMemo = useMemo;
        this.memo = useMemo ? new Memo() : null;
    }

    public PhysicalPlan optimize(LogicalPlan logical, ExecContext ctx) {
        if (useMemo) {
            Group g = memo.intern(logical);
            return optimizeGroup(g, ctx);
        }
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
                    int cmp = c.compareTo(bestCost);
                    if (cmp < 0 || (cmp == 0 && betterTieBreak(alt, best))) {
                        best = alt; bestCost = c;
                    }
                }
            }
        }
        if (best == null) throw new IllegalArgumentException("No physical alternatives produced for " + logical.getClass().getSimpleName());
        return best;
    }

    private PhysicalPlan optimizeGroup(Group g, ExecContext ctx) {
        if (g.best() != null) return g.best();
        // Optimize children first
        List<LogicalPlan> lchildren = g.logical().children();
        PhysicalPlan[] optimizedChildren = new PhysicalPlan[lchildren.size()];
        for (int i = 0; i < lchildren.size(); i++) {
            Group cg = memo.intern(lchildren.get(i));
            optimizedChildren[i] = optimizeGroup(cg, ctx);
        }
        PhysicalPlan best = null;
        Cost bestCost = Cost.INFINITE;
        for (PhysicalRule r : rules) {
            if (r.matches(g.logical())) {
                List<PhysicalPlan> alts = r.apply(g.logical(), Arrays.asList(optimizedChildren), ctx);
                for (PhysicalPlan alt : alts) {
                    Cost c = alt.estimate(costModel);
                    int cmp = c.compareTo(bestCost);
                    if (cmp < 0 || (cmp == 0 && betterTieBreak(alt, best))) {
                        best = alt; bestCost = c;
                    }
                }
            }
        }
        if (best == null) throw new IllegalArgumentException("No physical alternatives produced for " + g.logical().getClass().getSimpleName());
        g.best(best);
        g.bestCost(bestCost);
        return best;
    }

    private boolean betterTieBreak(PhysicalPlan cand, PhysicalPlan curBest) {
        if (curBest == null) return true;
        return tieRank(cand) < tieRank(curBest);
    }

    private int tieRank(PhysicalPlan p) {
        if (p instanceof HashJoinPlan) return 0;
        if (p instanceof SortMergeJoinPlan) return 1;
        if (p instanceof NestedLoopJoinPlan) return 2;
        return 3; // default for others
    }
}
