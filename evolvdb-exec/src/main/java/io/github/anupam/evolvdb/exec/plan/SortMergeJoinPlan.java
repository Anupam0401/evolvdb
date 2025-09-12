package io.github.anupam.evolvdb.exec.plan;

import io.github.anupam.evolvdb.exec.ExecContext;
import io.github.anupam.evolvdb.exec.op.PhysicalOperator;
import io.github.anupam.evolvdb.optimizer.Cost;
import io.github.anupam.evolvdb.optimizer.CostModel;
import io.github.anupam.evolvdb.sql.ast.Expr;
import io.github.anupam.evolvdb.types.Schema;

import java.util.List;

/** Placeholder SortMergeJoin plan; create() not implemented yet. */
public final class SortMergeJoinPlan implements PhysicalPlan {
    private final PhysicalPlan left;
    private final PhysicalPlan right;
    private final Expr predicate;
    private final Schema outSchema;

    public SortMergeJoinPlan(PhysicalPlan left, PhysicalPlan right, Expr predicate, Schema outSchema) {
        this.left = left;
        this.right = right;
        this.predicate = predicate;
        this.outSchema = outSchema;
    }

    @Override public Schema schema() { return outSchema; }
    @Override public List<PhysicalPlan> children() { return List.of(left, right); }

    @Override
    public PhysicalOperator create(ExecContext context) {
        throw new UnsupportedOperationException("SortMergeJoinExec not implemented yet");
    }

    @Override
    public Cost estimate(CostModel model) {
        return model.costSortMergeJoin(left.estimate(model), right.estimate(model));
    }
}
