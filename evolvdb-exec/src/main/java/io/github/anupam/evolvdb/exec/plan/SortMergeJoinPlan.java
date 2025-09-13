package io.github.anupam.evolvdb.exec.plan;

import io.github.anupam.evolvdb.exec.ExecContext;
import io.github.anupam.evolvdb.exec.op.PhysicalOperator;
import io.github.anupam.evolvdb.exec.op.SortMergeJoinExec;
import io.github.anupam.evolvdb.optimizer.Cost;
import io.github.anupam.evolvdb.optimizer.CostModel;
import io.github.anupam.evolvdb.sql.ast.Expr;
import io.github.anupam.evolvdb.types.Schema;

import java.util.List;
import java.util.HashSet;
import java.util.Set;

/** SortMergeJoin plan. */
public final class SortMergeJoinPlan implements PhysicalPlan {
    private final PhysicalPlan left;
    private final PhysicalPlan right;
    private final Expr leftKey;
    private final Expr rightKey;
    private final Schema outSchema;
    private final Set<String> leftQuals;
    private final Set<String> rightQuals;

    public SortMergeJoinPlan(PhysicalPlan left, PhysicalPlan right, Expr leftKey, Expr rightKey, Schema outSchema, Set<String> leftQuals, Set<String> rightQuals) {
        this.left = left;
        this.right = right;
        this.leftKey = leftKey;
        this.rightKey = rightKey;
        this.outSchema = outSchema;
        this.leftQuals = leftQuals == null ? Set.of() : new HashSet<>(leftQuals);
        this.rightQuals = rightQuals == null ? Set.of() : new HashSet<>(rightQuals);
    }

    @Override public Schema schema() { return outSchema; }
    @Override public List<PhysicalPlan> children() { return List.of(left, right); }

    @Override
    public PhysicalOperator create(ExecContext context) {
        return new SortMergeJoinExec(left.create(context), right.create(context), leftKey, rightKey, outSchema, leftQuals, rightQuals);
    }

    @Override
    public Cost estimate(CostModel model) {
        return model.costSortMergeJoin(left.estimate(model), right.estimate(model));
    }
}
