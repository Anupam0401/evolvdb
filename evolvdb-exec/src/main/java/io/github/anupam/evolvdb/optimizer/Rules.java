package io.github.anupam.evolvdb.optimizer;

import io.github.anupam.evolvdb.exec.ExecContext;
import io.github.anupam.evolvdb.exec.plan.*;
import io.github.anupam.evolvdb.planner.logical.*;
import io.github.anupam.evolvdb.sql.ast.*;

import java.util.*;

/** Collection of simple physical transformation rules. */
public final class Rules {
    private Rules() {}

    // Scan
    public static final class ScanRule implements PhysicalRule {
        @Override public boolean matches(LogicalPlan logical) { return logical instanceof LogicalScan; }
        @Override public List<PhysicalPlan> apply(LogicalPlan logical, List<PhysicalPlan> optimizedChildren, ExecContext ctx) {
            LogicalScan s = (LogicalScan) logical;
            return List.of(new SeqScanPlan(s.tableName(), s.schema()));
        }
    }

    // Filter
    public static final class FilterRule implements PhysicalRule {
        @Override public boolean matches(LogicalPlan logical) { return logical instanceof LogicalFilter; }
        @Override public List<PhysicalPlan> apply(LogicalPlan logical, List<PhysicalPlan> optimizedChildren, ExecContext ctx) {
            LogicalFilter f = (LogicalFilter) logical;
            PhysicalPlan c = optimizedChildren.get(0);
            return List.of(new FilterPlan(c, f.predicate()));
        }
    }

    // Project
    public static final class ProjectRule implements PhysicalRule {
        @Override public boolean matches(LogicalPlan logical) { return logical instanceof LogicalProject; }
        @Override public List<PhysicalPlan> apply(LogicalPlan logical, List<PhysicalPlan> optimizedChildren, ExecContext ctx) {
            LogicalProject p = (LogicalProject) logical;
            PhysicalPlan c = optimizedChildren.get(0);
            return List.of(new ProjectPlan(c, p.items(), p.schema()));
        }
    }

    // Aggregate
    public static final class AggregateRule implements PhysicalRule {
        @Override public boolean matches(LogicalPlan logical) { return logical instanceof LogicalAggregate; }
        @Override public List<PhysicalPlan> apply(LogicalPlan logical, List<PhysicalPlan> optimizedChildren, ExecContext ctx) {
            LogicalAggregate a = (LogicalAggregate) logical;
            PhysicalPlan c = optimizedChildren.get(0);
            return List.of(new AggregatePlan(c, a.groupBy(), a.aggregates(), a.schema()));
        }
    }

    // Join (produces multiple alternatives; baseline NLJ + placeholders)
    public static final class JoinRule implements PhysicalRule {
        @Override public boolean matches(LogicalPlan logical) { return logical instanceof LogicalJoin; }
        @Override public List<PhysicalPlan> apply(LogicalPlan logical, List<PhysicalPlan> optimizedChildren, ExecContext ctx) {
            LogicalJoin j = (LogicalJoin) logical;
            PhysicalPlan l = optimizedChildren.get(0);
            PhysicalPlan r = optimizedChildren.get(1);
            Set<String> lq = collectQualifiers(j.left());
            Set<String> rq = collectQualifiers(j.right());
            List<PhysicalPlan> alts = new ArrayList<>();
            alts.add(new NestedLoopJoinPlan(l, r, j.condition(), j.schema(), lq, rq));
            // Only add hash/sort-merge for equi-joins: ColumnRef = ColumnRef
            Expr cond = j.condition();
            if (cond instanceof ComparisonExpr ce && ce.op() == ComparisonExpr.Op.EQ &&
                    ce.left() instanceof ColumnRef && ce.right() instanceof ColumnRef) {
                alts.add(new HashJoinPlan(l, r, ce.left(), ce.right(), j.schema(), lq, rq));
                alts.add(new SortMergeJoinPlan(l, r, ce.left(), ce.right(), j.schema(), lq, rq));
            }
            return alts;
        }

        private static Set<String> collectQualifiers(LogicalPlan plan) {
            Set<String> out = new HashSet<>();
            collect(plan, out);
            return out;
        }
        private static void collect(LogicalPlan plan, Set<String> out) {
            if (plan instanceof LogicalScan s) {
                String alias = s.alias();
                if (alias != null && !alias.isBlank()) out.add(alias.toLowerCase(Locale.ROOT));
                else out.add(s.tableName().toLowerCase(Locale.ROOT));
            }
            for (LogicalPlan ch : plan.children()) collect(ch, out);
        }
    }

    // Insert
    public static final class InsertRule implements PhysicalRule {
        @Override public boolean matches(LogicalPlan logical) { return logical instanceof LogicalInsert; }
        @Override public List<PhysicalPlan> apply(LogicalPlan logical, List<PhysicalPlan> optimizedChildren, ExecContext ctx) {
            LogicalInsert i = (LogicalInsert) logical;
            return List.of(new InsertPlan(i));
        }
    }
}
