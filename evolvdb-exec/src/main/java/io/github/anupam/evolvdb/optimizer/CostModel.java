package io.github.anupam.evolvdb.optimizer;

import io.github.anupam.evolvdb.types.Schema;
import io.github.anupam.evolvdb.sql.ast.Expr;

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
    default Cost costNestedLoopJoin(Cost left, Cost right) { return costNestedLoopJoin(left, right, null); }
    default Cost costHashJoin(Cost left, Cost right) { return costHashJoin(left, right, null); }
    default Cost costSortMergeJoin(Cost left, Cost right) { return costSortMergeJoin(left, right, null); }
    Cost costNestedLoopJoin(Cost left, Cost right, Expr predicate);
    Cost costHashJoin(Cost left, Cost right, Expr predicate);
    Cost costSortMergeJoin(Cost left, Cost right, Expr predicate);
    Cost costAggregate(Cost child);
    Cost costInsert(int rows);
}
