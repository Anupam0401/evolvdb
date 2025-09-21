package io.github.anupam.evolvdb.optimizer.rewrite;

import io.github.anupam.evolvdb.planner.logical.*;
import io.github.anupam.evolvdb.sql.ast.ColumnRef;
import io.github.anupam.evolvdb.sql.ast.Expr;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** Predicate pushdown for LogicalFilter nodes. Conservative across Projects/Aggregates. */
public final class PredicatePushdownRule implements LogicalRule {

    @Override
    public boolean matches(LogicalPlan plan) {
        return plan instanceof LogicalFilter;
    }

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        return rewriteBottomUp(plan);
    }

    /** Bottom-up rewrite entry point (idempotent if no opportunities). */
    public LogicalPlan rewriteBottomUp(LogicalPlan plan) {
        if (plan instanceof LogicalFilter f) {
            LogicalPlan child = rewriteBottomUp(f.child());
            return pushInto(child, f.predicate());
        }
        List<LogicalPlan> ch = plan.children();
        if (ch.isEmpty()) return plan;
        // rebuild with rewritten children
        List<LogicalPlan> rewritten = new ArrayList<>(ch.size());
        for (LogicalPlan c : ch) rewritten.add(rewriteBottomUp(c));
        return rebuild(plan, rewritten);
    }

    private LogicalPlan rebuild(LogicalPlan plan, List<LogicalPlan> children) {
        if (plan instanceof LogicalProject p) return new LogicalProject(children.get(0), p.items(), p.schema());
        if (plan instanceof LogicalJoin j) return new LogicalJoin(children.get(0), children.get(1), j.type(), j.condition(), j.schema());
        if (plan instanceof LogicalAggregate a) return new LogicalAggregate(children.get(0), a.groupBy(), a.aggregates(), a.schema());
        if (plan instanceof LogicalInsert) return plan; // no children
        return plan; // scan or others
    }

    private LogicalPlan pushInto(LogicalPlan child, Expr predicate) {
        if (child instanceof LogicalJoin j) {
            // Split conjuncts by column refs
            List<Expr> conjuncts = ExprUtils.splitConjuncts(predicate);
            List<Expr> leftOnly = new ArrayList<>();
            List<Expr> rightOnly = new ArrayList<>();
            List<Expr> remain = new ArrayList<>();

            for (Expr c : conjuncts) {
                Set<ColumnRef> refs = ExprUtils.collectColumnRefs(c);
                boolean inLeft = ExprUtils.schemaContainsAll(j.left().schema(), refs);
                boolean inRight = ExprUtils.schemaContainsAll(j.right().schema(), refs);
                if (inLeft && !inRight) leftOnly.add(c);
                else if (!inLeft && inRight) rightOnly.add(c);
                else remain.add(c);
            }

            LogicalPlan newLeft = rewriteBottomUp(j.left());
            LogicalPlan newRight = rewriteBottomUp(j.right());
            if (!leftOnly.isEmpty()) newLeft = new LogicalFilter(newLeft, ExprUtils.andAll(leftOnly));
            if (!rightOnly.isEmpty()) newRight = new LogicalFilter(newRight, ExprUtils.andAll(rightOnly));
            LogicalJoin rebuilt = new LogicalJoin(newLeft, newRight, j.type(), j.condition(), j.schema());
            if (remain.isEmpty()) return rebuilt;
            return new LogicalFilter(rebuilt, ExprUtils.andAll(remain));
        }
        if (child instanceof LogicalProject p) {
            // Conservative: keep filter above project by default
            LogicalPlan c = rewriteBottomUp(p.child());
            LogicalProject rebuilt = new LogicalProject(c, p.items(), p.schema());
            return new LogicalFilter(rebuilt, predicate);
        }
        if (child instanceof LogicalAggregate a) {
            // Keep filter above aggregate to preserve semantics
            LogicalPlan c = rewriteBottomUp(a.child());
            LogicalAggregate rebuilt = new LogicalAggregate(c, a.groupBy(), a.aggregates(), a.schema());
            return new LogicalFilter(rebuilt, predicate);
        }
        // Scan or others: cannot push further, attach here
        return new LogicalFilter(child, predicate);
    }
}
