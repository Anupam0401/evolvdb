package io.github.anupam.evolvdb.exec;

import io.github.anupam.evolvdb.exec.op.*;
import io.github.anupam.evolvdb.planner.logical.*;
import io.github.anupam.evolvdb.exec.plan.PhysicalPlan;
import io.github.anupam.evolvdb.optimizer.*;
import io.github.anupam.evolvdb.optimizer.rewrite.LogicalRewriter;

import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.List;

/** Lowers a logical plan into a tree of Volcano operators. */
public final class PhysicalPlanner {

    public PhysicalOperator plan(LogicalPlan logical, ExecContext ctx) {
        if (ctx.useOptimizer()) {
            // Pre-optimization logical rewrites (predicate pushdown, projection pruning, join reordering)
            logical = new LogicalRewriter(ctx.stats()).rewrite(logical);
            VolcanoOptimizer opt = new VolcanoOptimizer(new DefaultCostModel(ctx.stats()), defaultRules(), ctx.useMemo());
            PhysicalPlan best = opt.optimize(logical, ctx);
            return best.create(ctx);
        }
        if (logical instanceof LogicalScan s) {
            return new SeqScanExec(ctx.catalog(), s.tableName());
        }
        if (logical instanceof LogicalFilter f) {
            PhysicalOperator c = plan(f.child(), ctx);
            return new FilterExec(c, f.predicate());
        }
        if (logical instanceof LogicalProject p) {
            PhysicalOperator c = plan(p.child(), ctx);
            return new ProjectExec(c, p.items(), p.schema());
        }
        if (logical instanceof LogicalJoin j) {
            PhysicalOperator l = plan(j.left(), ctx);
            PhysicalOperator r = plan(j.right(), ctx);
            Set<String> lq = collectQualifiers(j.left());
            Set<String> rq = collectQualifiers(j.right());
            return new NestedLoopJoinExec(l, r, j.condition(), j.schema(), lq, rq);
        }
        if (logical instanceof LogicalAggregate a) {
            PhysicalOperator c = plan(a.child(), ctx);
            return new AggregateExec(c, a.groupBy(), a.aggregates(), a.schema());
        }
        if (logical instanceof LogicalInsert i) {
            return new InsertExec(ctx.catalog(), i);
        }
        throw new IllegalArgumentException("Unsupported logical node: " + logical.getClass().getSimpleName());
    }

    private List<PhysicalRule> defaultRules() {
        return List.of(
                new Rules.ScanRule(),
                new Rules.FilterRule(),
                new Rules.ProjectRule(),
                new Rules.JoinRule(),
                new Rules.AggregateRule(),
                new Rules.InsertRule()
        );
    }

    private Set<String> collectQualifiers(LogicalPlan plan) {
        Set<String> out = new HashSet<>();
        collect(plan, out);
        return out;
    }

    private void collect(LogicalPlan plan, Set<String> out) {
        if (plan instanceof LogicalScan s) {
            String alias = s.alias();
            if (alias != null && !alias.isBlank()) out.add(alias.toLowerCase(Locale.ROOT));
            else out.add(s.tableName().toLowerCase(Locale.ROOT));
        }
        for (LogicalPlan ch : plan.children()) collect(ch, out);
    }
}
