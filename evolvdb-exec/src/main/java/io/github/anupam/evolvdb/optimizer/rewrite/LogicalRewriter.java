package io.github.anupam.evolvdb.optimizer.rewrite;

import io.github.anupam.evolvdb.planner.logical.LogicalPlan;
import io.github.anupam.evolvdb.optimizer.stats.StatsProvider;

/** Orchestrates a sequence of logical rewrite passes (fixpoint where needed). */
public final class LogicalRewriter {
    private final PredicatePushdownRule pushdown = new PredicatePushdownRule();
    private final ProjectionPruningRule pruning = new ProjectionPruningRule();
    private final JoinReorderingRule reordering;

    public LogicalRewriter() { this(null); }
    public LogicalRewriter(StatsProvider stats) {
        this.reordering = new JoinReorderingRule(stats);
    }

    public LogicalPlan rewrite(LogicalPlan plan) {
        // 1) Predicate pushdown (simple fixpoint)
        LogicalPlan cur = plan;
        for (int i = 0; i < 4; i++) { // limited iterations to avoid infinite loops
            LogicalPlan next = pushdown.rewriteBottomUp(cur);
            if (next == cur || next.equals(cur)) { // reference or equals
                cur = next;
                break;
            }
            cur = next;
        }
        // 2) Projection pruning (single bottom-up pass with required-column analysis)
        cur = pruning.prune(cur);
        // 3) Join reordering (greedy left-deep based on stats if available)
        cur = reordering.rewrite(cur);
        return cur;
    }
}
