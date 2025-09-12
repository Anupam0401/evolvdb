package io.github.anupam.evolvdb.optimizer;

import io.github.anupam.evolvdb.types.Schema;

/** Naive cost model with simple heuristics. */
public final class DefaultCostModel implements CostModel {
    private final double defaultRows;
    private final double filterSel;
    private final double joinSel;

    public DefaultCostModel() { this(1000, 0.1, 0.25); }
    public DefaultCostModel(double defaultRows, double filterSel, double joinSel) {
        this.defaultRows = defaultRows;
        this.filterSel = filterSel;
        this.joinSel = joinSel;
    }

    @Override public double defaultRowCount() { return defaultRows; }
    @Override public double filterSelectivity() { return filterSel; }
    @Override public double joinSelectivity() { return joinSel; }

    @Override
    public Cost costSeqScan(String tableName, Schema schema) {
        double rows = defaultRows; // no stats yet
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
    public Cost costHashJoin(Cost left, Cost right) {
        // For now, make it more expensive than NLJ so we pick NLJ (placeholder not implemented)
        Cost nlj = costNestedLoopJoin(left, right);
        return nlj.scale(2.0);
    }

    @Override
    public Cost costSortMergeJoin(Cost left, Cost right) {
        Cost nlj = costNestedLoopJoin(left, right);
        return nlj.scale(3.0);
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
