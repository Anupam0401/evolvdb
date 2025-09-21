package io.github.anupam.evolvdb.optimizer;

import io.github.anupam.evolvdb.exec.ExecContext;
import io.github.anupam.evolvdb.exec.plan.AggregatePlan;
import io.github.anupam.evolvdb.exec.plan.FilterPlan;
import io.github.anupam.evolvdb.exec.plan.HashJoinPlan;
import io.github.anupam.evolvdb.exec.plan.InsertPlan;
import io.github.anupam.evolvdb.exec.plan.NestedLoopJoinPlan;
import io.github.anupam.evolvdb.exec.plan.PhysicalPlan;
import io.github.anupam.evolvdb.exec.plan.ProjectPlan;
import io.github.anupam.evolvdb.exec.plan.SeqScanPlan;
import io.github.anupam.evolvdb.exec.plan.SortMergeJoinPlan;
import io.github.anupam.evolvdb.optimizer.rewrite.ExprUtils;
import io.github.anupam.evolvdb.planner.logical.LogicalAggregate;
import io.github.anupam.evolvdb.planner.logical.LogicalFilter;
import io.github.anupam.evolvdb.planner.logical.LogicalInsert;
import io.github.anupam.evolvdb.planner.logical.LogicalJoin;
import io.github.anupam.evolvdb.planner.logical.LogicalPlan;
import io.github.anupam.evolvdb.planner.logical.LogicalProject;
import io.github.anupam.evolvdb.planner.logical.LogicalScan;
import io.github.anupam.evolvdb.sql.ast.ColumnRef;
import io.github.anupam.evolvdb.sql.ast.ComparisonExpr;
import io.github.anupam.evolvdb.sql.ast.Expr;
import io.github.anupam.evolvdb.types.ColumnMeta;
import io.github.anupam.evolvdb.types.Schema;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/** Collection of simple physical transformation rules. */
public final class Rules {
    private Rules() {}

    // Scan
    public static final class ScanRule implements PhysicalRule {
        @Override public boolean matches(LogicalPlan logical) { return logical instanceof LogicalScan; }
        @Override public List<PhysicalPlan> apply(
            LogicalPlan logical,
            List<PhysicalPlan> optimizedChildren,
            ExecContext ctx
        ) {
            LogicalScan s = (LogicalScan) logical;
            return List.of(new SeqScanPlan(s.tableName(), s.schema()));
        }

        private static List<ColumnMeta> mergeCols(Schema l, Schema r) {
            ArrayList<ColumnMeta> cols = new ArrayList<>(l.size() + r.size());
            cols.addAll(l.columns());
            cols.addAll(r.columns());
            return cols;
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
            // Out schema must match children schemas (after pruning), so merge at physical layer
            Schema outSchema = new Schema(ScanRule.mergeCols(l.schema(), r.schema()));
            List<PhysicalPlan> alts = new ArrayList<>();
            alts.add(new NestedLoopJoinPlan(l, r, j.condition(), outSchema, lq, rq));
            // Only add hash/sort-merge for equi-joins: ColumnRef = ColumnRef
            Expr cond = j.condition();
            if (cond instanceof ComparisonExpr ce && ce.op() == ComparisonExpr.Op.EQ &&
                    ce.left() instanceof ColumnRef && ce.right() instanceof ColumnRef) {
                // Align keys to current children: leftKey must come from 'l', rightKey from 'r'
                var lrefs = ExprUtils.collectColumnRefs(ce.left());
                var rrefs = ExprUtils.collectColumnRefs(ce.right());
                boolean leftHasLeft = ExprUtils.schemaContainsAll(l.schema(), lrefs) && ExprUtils.schemaContainsAll(r.schema(), rrefs);
                boolean leftHasRight = ExprUtils.schemaContainsAll(l.schema(), rrefs) && ExprUtils.schemaContainsAll(r.schema(), lrefs);
                if (leftHasLeft) {
                    alts.add(new HashJoinPlan(l, r, ce.left(), ce.right(), outSchema, lq, rq));
                    alts.add(new SortMergeJoinPlan(l, r, ce.left(), ce.right(), outSchema, lq, rq));
                } else if (leftHasRight) {
                    alts.add(new HashJoinPlan(l, r, ce.right(), ce.left(), outSchema, lq, rq));
                    alts.add(new SortMergeJoinPlan(l, r, ce.right(), ce.left(), outSchema, lq, rq));
                }
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
