package io.github.anupam.evolvdb.exec;

import io.github.anupam.evolvdb.exec.op.*;
import io.github.anupam.evolvdb.planner.logical.*;

import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/** Lowers a logical plan into a tree of Volcano operators. */
public final class PhysicalPlanner {

    public PhysicalOperator plan(LogicalPlan logical, ExecContext ctx) {
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
        if (logical instanceof LogicalInsert) {
            throw new UnsupportedOperationException("INSERT execution not implemented in exec yet");
        }
        throw new IllegalArgumentException("Unsupported logical node: " + logical.getClass().getSimpleName());
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
