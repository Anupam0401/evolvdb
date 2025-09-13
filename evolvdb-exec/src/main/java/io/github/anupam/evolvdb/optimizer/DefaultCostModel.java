package io.github.anupam.evolvdb.optimizer;

import io.github.anupam.evolvdb.types.Schema;
import io.github.anupam.evolvdb.optimizer.stats.StatsProvider;
import io.github.anupam.evolvdb.optimizer.stats.TableStats;
import io.github.anupam.evolvdb.sql.ast.Expr;

/** Naive cost model with simple heuristics. */
public final class DefaultCostModel implements CostModel {
    private final StatsProvider stats;
    private final double defaultRows;
    private final double filterSel;
    private final double joinSel;

    public DefaultCostModel() { this(null, 1000, 0.1, 0.25); }
    public DefaultCostModel(StatsProvider stats) { this(stats, 1000, 0.1, 0.25); }
    public DefaultCostModel(StatsProvider stats, double defaultRows, double filterSel, double joinSel) {
        this.stats = stats;
        this.defaultRows = defaultRows;
        this.filterSel = filterSel;
        this.joinSel = joinSel;
    }

    @Override public double defaultRowCount() { return defaultRows; }
    @Override public double filterSelectivity() { return filterSel; }
    @Override public double joinSelectivity() { return joinSel; }

    @Override
    public Cost costSeqScan(String tableName, Schema schema) {
        double rows = defaultRows; // fallback
        if (stats != null) {
            TableStats ts = stats.getTableStats(tableName);
            if (ts != null && ts.rowCount() > 0) rows = ts.rowCount();
        }
        double cpu = rows;         // one unit per row
        double io = Math.max(1, rows / 100.0);
        return Cost.of(rows, cpu, io);
    }

    @Override
    public Cost costFilter(Cost child) {
        double rows = child.rowCount() * filterSel;
        // Add a small per-row predicate evaluation cost but keep total below scan in naive model
        double cpu = child.cpu() + child.rowCount() * 0.1;
        double io = child.io();
        return Cost.of(rows, cpu, io);
    }

    @Override
    public Cost costProject(Cost child) {
        double rows = child.rowCount();
        double cpu = child.cpu() + rows * 0.1; // light expression work
        double io = child.io();
        return Cost.of(rows, cpu, io);
    }

    @Override
    public Cost costNestedLoopJoin(Cost left, Cost right) {
        double rows = left.rowCount() * right.rowCount() * joinSel;
        double cpu = left.rowCount() * right.rowCount();
        double io = left.io() + right.io();
        return Cost.of(rows, cpu, io);
    }

    @Override
    public Cost costNestedLoopJoin(Cost left, Cost right, Expr predicate) {
        return costNestedLoopJoin(left, right);
    }

    @Override
    public Cost costHashJoin(Cost left, Cost right) {
        // Build + probe costs ~ linear in inputs
        double rows = Math.min(left.rowCount(), right.rowCount()) * joinSel;
        double cpu = left.rowCount() + right.rowCount();
        double io = left.io() + right.io();
        return Cost.of(rows, cpu, io);
    }

    @Override
    public Cost costHashJoin(Cost left, Cost right, Expr predicate) {
        return costHashJoin(left, right);
    }

    @Override
    public Cost costSortMergeJoin(Cost left, Cost right) {
        // Sort both sides O(n log n) + merge linear
        double n = left.rowCount();
        double m = right.rowCount();
        double rows = Math.min(n, m) * joinSel;
        double cpu = n * Math.log(Math.max(1, n)) + m * Math.log(Math.max(1, m)) + n + m;
        double io = left.io() + right.io();
        return Cost.of(rows, cpu, io);
    }

    @Override
    public Cost costSortMergeJoin(Cost left, Cost right, Expr predicate) {
        return costSortMergeJoin(left, right);
    }

    @Override
    public Cost costAggregate(Cost child) {
        double rows = Math.max(1, child.rowCount() * 0.1); // coarse
        double cpu = child.cpu() + child.rowCount();
        double io = child.io();
        return Cost.of(rows, cpu, io);
    }

    @Override
    public Cost costInsert(int rows) {
        double r = rows;
        double cpu = rows;
        double io = Math.max(1, rows / 100.0);
        return Cost.of(r, cpu, io);
    }
}
