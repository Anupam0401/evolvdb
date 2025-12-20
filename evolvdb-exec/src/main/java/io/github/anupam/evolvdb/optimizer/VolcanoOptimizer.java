package io.github.anupam.evolvdb.optimizer;

import java.util.Arrays;
import java.util.List;

import io.github.anupam.evolvdb.exec.ExecContext;
import io.github.anupam.evolvdb.exec.plan.HashJoinPlan;
import io.github.anupam.evolvdb.exec.plan.NestedLoopJoinPlan;
import io.github.anupam.evolvdb.exec.plan.PhysicalPlan;
import io.github.anupam.evolvdb.exec.plan.SortMergeJoinPlan;
import io.github.anupam.evolvdb.optimizer.memo.Group;
import io.github.anupam.evolvdb.optimizer.memo.Memo;
import io.github.anupam.evolvdb.planner.logical.LogicalPlan;

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
                        best = alt;
                        bestCost = c;
                    }
                }
            }
        }
        if (best == null)
            throw new IllegalArgumentException(
                    "No physical alternatives produced for " + logical.getClass().getSimpleName());
        return best;
    }

    private PhysicalPlan optimizeGroup(Group g, ExecContext ctx) {
        if (g.best() != null) return g.best();
        PhysicalPlan globalBest = null;
        Cost globalBestCost = Cost.INFINITE;
        // Explore each group expression in stable insertion order
        for (var ge : g.expressions()) {
            // Optimize children first for this expression
            var childGroups = ge.children();
            PhysicalPlan[] optimizedChildren = new PhysicalPlan[childGroups.size()];
            for (int i = 0; i < childGroups.size(); i++) {
                optimizedChildren[i] = optimizeGroup(childGroups.get(i), ctx);
            }
            // Apply rules that match this expression's logical node
            for (PhysicalRule r : rules) {
                if (r.matches(ge.logical())) {
                    List<PhysicalPlan> alts =
                            r.apply(ge.logical(), Arrays.asList(optimizedChildren), ctx);
                    for (PhysicalPlan alt : alts) {
                        Cost c = alt.estimate(costModel);
                        int cmp = c.compareTo(globalBestCost);
                        if (cmp < 0 || (cmp == 0 && betterTieBreak(alt, globalBest))) {
                            globalBest = alt;
                            globalBestCost = c;
                        }
                    }
                }
            }
        }
        if (globalBest == null)
            throw new IllegalArgumentException(
                    "No physical alternatives produced for "
                            + g.logical().getClass().getSimpleName());
        g.best(globalBest);
        g.bestCost(globalBestCost);
        return globalBest;
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
