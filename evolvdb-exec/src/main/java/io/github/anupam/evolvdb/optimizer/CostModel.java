package io.github.anupam.evolvdb.optimizer;

import io.github.anupam.evolvdb.types.Schema;

/** Abstraction for estimating cost and output row counts. */
public interface CostModel {
    // Tunables
    double defaultRowCount();
    double filterSelectivity();
    double joinSelectivity();

    // Operator costs
    Cost costSeqScan(String tableName, Schema schema);
    Cost costFilter(Cost child);
    Cost costProject(Cost child);
    Cost costNestedLoopJoin(Cost left, Cost right);
    Cost costHashJoin(Cost left, Cost right);
    Cost costSortMergeJoin(Cost left, Cost right);
    Cost costAggregate(Cost child);
    Cost costInsert(int rows);
}
