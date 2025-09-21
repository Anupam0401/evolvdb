package io.github.anupam.evolvdb.optimizer.rewrite;

import io.github.anupam.evolvdb.optimizer.DefaultCostModel;
import io.github.anupam.evolvdb.optimizer.Cost;
import io.github.anupam.evolvdb.optimizer.stats.StatsProvider;
import io.github.anupam.evolvdb.planner.logical.*;
import io.github.anupam.evolvdb.sql.ast.ColumnRef;
import io.github.anupam.evolvdb.sql.ast.ComparisonExpr;
import io.github.anupam.evolvdb.sql.ast.Expr;

import io.github.anupam.evolvdb.types.Schema;
import java.util.*;

/** Greedy left-deep join reordering for INNER joins using basic stats. */
public final class JoinReorderingRule implements LogicalRule {
    private final StatsProvider stats;
    private final DefaultCostModel costModel;

    public JoinReorderingRule(StatsProvider stats) {
        this.stats = stats;
        this.costModel = new DefaultCostModel(stats);
    }

    @Override
    public boolean matches(LogicalPlan plan) { return plan instanceof LogicalJoin j && j.type() == LogicalJoin.JoinType.INNER; }

    @Override
    public LogicalPlan apply(LogicalPlan plan) { return rewrite(plan); }

    public LogicalPlan rewrite(LogicalPlan plan) {
        if (!(plan instanceof LogicalJoin j) || j.type() != LogicalJoin.JoinType.INNER) {
            // Recurse
            List<LogicalPlan> ch = plan.children();
            if (ch.isEmpty()) return plan;
            List<LogicalPlan> rewritten = new ArrayList<>(ch.size());
            for (LogicalPlan c : ch) rewritten.add(rewrite(c));
            return rebuild(plan, rewritten);
        }
        // Flatten join tree: collect leaves and predicates
        List<LogicalPlan> leaves = new ArrayList<>();
        List<Expr> predicates = new ArrayList<>();
        collectJoins(j, leaves, predicates);
        // If only two leaves, nothing to do
        if (leaves.size() <= 2) {
            LogicalPlan l = rewrite(leaves.get(0));
            LogicalPlan r = rewrite(leaves.get(1));
            return new LogicalJoin(l, r, j.type(), j.condition(), j.schema());
        }
        // Greedy left-deep: pick smallest leaf by table stats
        int seedIdx = pickSmallestLeaf(leaves);
        LogicalPlan leftTree = rewrite(leaves.remove(seedIdx));
        Set<Integer> leftSet = new HashSet<>(); leftSet.add(seedIdx); // indices original; not needed further
        List<Expr> remaining = new ArrayList<>(predicates);
        // Build iteratively
        while (!leaves.isEmpty()) {
            int bestIdx = -1; double bestScore = Double.POSITIVE_INFINITY; Expr bestPred = null;
            for (int i = 0; i < leaves.size(); i++) {
                LogicalPlan cand = leaves.get(i);
                // find a conjunct that connects leftTree and cand
                Expr eq = pickEquiPredicate(remaining, leftTree.schema(), cand.schema());
                Cost lc = Cost.of(estimateRows(leftTree), 0, 0);
                Cost rc = Cost.of(estimateRows(cand), 0, 0);
                Cost est;
                if (eq != null) est = costModel.costHashJoin(lc, rc, eq); // prefer hash-join estimate if equi
                else est = costModel.costNestedLoopJoin(lc, rc); // cross-ish
                double score = est.total();
                if (score < bestScore) { bestScore = score; bestIdx = i; bestPred = eq; }
            }
            LogicalPlan next = rewrite(leaves.remove(bestIdx));
            if (bestPred != null) removeOnePredicate(remaining, bestPred);
            leftTree = new LogicalJoin(leftTree, next, LogicalJoin.JoinType.INNER, bestPred, mergeSchemas(leftTree.schema(), next.schema()));
        }
        // Attach any remaining predicates as a filter above
        if (!remaining.isEmpty()) {
            Expr pred = ExprUtils.andAll(remaining);
            return new LogicalFilter(leftTree, pred);
        }
        return leftTree;
    }

    private static LogicalPlan rebuild(LogicalPlan plan, List<LogicalPlan> children) {
        if (plan instanceof LogicalProject p) return new LogicalProject(children.get(0), p.items(), p.schema());
        if (plan instanceof LogicalJoin j) return new LogicalJoin(children.get(0), children.get(1), j.type(), j.condition(), j.schema());
        if (plan instanceof LogicalAggregate a) return new LogicalAggregate(children.get(0), a.groupBy(), a.aggregates(), a.schema());
        return plan;
    }

    private void collectJoins(LogicalJoin j, List<LogicalPlan> leaves, List<Expr> predicates) {
        if (j.left() instanceof LogicalJoin jl && jl.type() == LogicalJoin.JoinType.INNER) collectJoins(jl, leaves, predicates);
        else leaves.add(j.left());
        if (j.right() instanceof LogicalJoin jr && jr.type() == LogicalJoin.JoinType.INNER) collectJoins(jr, leaves, predicates);
        else leaves.add(j.right());
        if (j.condition() != null) predicates.addAll(ExprUtils.splitConjuncts(j.condition()));
    }

    private int pickSmallestLeaf(List<LogicalPlan> leaves) {
        int best = 0;
        double bestRows = Double.POSITIVE_INFINITY;
        for (int i = 0; i < leaves.size(); i++) {
            double rows = estimateRows(leaves.get(i));
            if (rows < bestRows) { bestRows = rows; best = i; }
        }
        return best;
    }

    private double estimateRows(LogicalPlan plan) {
        // Heuristic: if subtree has a scan, return its table row count; else default
        if (plan instanceof LogicalScan s) {
            var ts = (stats == null) ? null : stats.getTableStats(s.tableName());
            return (ts != null && ts.rowCount() > 0) ? ts.rowCount() : costModel.defaultRowCount();
        }
        // For composite nodes, try to find first scan child
        for (LogicalPlan c : plan.children()) {
            double r = estimateRows(c);
            if (r > 0) return r;
        }
        return costModel.defaultRowCount();
    }

    private static Expr pickEquiPredicate(List<Expr> preds, Schema left, Schema right) {
        for (Expr e : preds) {
            if (e instanceof ComparisonExpr ce && ce.op() == ComparisonExpr.Op.EQ) {
                Set<ColumnRef> lrefs = ExprUtils.collectColumnRefs(ce.left());
                Set<ColumnRef> rrefs = ExprUtils.collectColumnRefs(ce.right());
                boolean leftOnly = ExprUtils.schemaContainsAll(left, lrefs) && ExprUtils.schemaContainsAll(right, rrefs);
                boolean rightOnly = ExprUtils.schemaContainsAll(left, rrefs) && ExprUtils.schemaContainsAll(right, lrefs);
                if ((leftOnly || rightOnly) && !lrefs.isEmpty() && !rrefs.isEmpty()) return e;
            }
        }
        return null;
    }

    private static void removeOnePredicate(List<Expr> list, Expr pred) {
        for (int i = 0; i < list.size(); i++) {
            if (list.get(i) == pred || list.get(i).equals(pred)) { list.remove(i); return; }
        }
    }

    private static Schema mergeSchemas(Schema a, Schema b) { return new Schema(concat(a.columns(), b.columns())); }
    private static List<io.github.anupam.evolvdb.types.ColumnMeta> concat(List<io.github.anupam.evolvdb.types.ColumnMeta> x, List<io.github.anupam.evolvdb.types.ColumnMeta> y) {
        List<io.github.anupam.evolvdb.types.ColumnMeta> out = new ArrayList<>(x.size()+y.size());
        out.addAll(x); out.addAll(y); return out;
    }
}
