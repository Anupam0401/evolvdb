package io.github.anupam.evolvdb.exec.plan;

import io.github.anupam.evolvdb.exec.ExecContext;
import io.github.anupam.evolvdb.exec.op.HashJoinExec;
import io.github.anupam.evolvdb.exec.op.PhysicalOperator;
import io.github.anupam.evolvdb.optimizer.Cost;
import io.github.anupam.evolvdb.optimizer.CostModel;
import io.github.anupam.evolvdb.sql.ast.Expr;
import io.github.anupam.evolvdb.types.Schema;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Placeholder HashJoin plan; create() not implemented yet. */
public final class HashJoinPlan implements PhysicalPlan {
    private final PhysicalPlan left;
    private final PhysicalPlan right;
    private final Expr leftKey;
    private final Expr rightKey;
    private final Schema outSchema;
    private final Set<String> leftQuals;
    private final Set<String> rightQuals;

    public HashJoinPlan(PhysicalPlan left, PhysicalPlan right, Expr leftKey, Expr rightKey, Schema outSchema, Set<String> leftQuals, Set<String> rightQuals) {
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
        return new HashJoinExec(left.create(context), right.create(context), leftKey, rightKey, outSchema, leftQuals, rightQuals);
    }

    @Override
    public Cost estimate(CostModel model) {
        return model.costHashJoin(left.estimate(model), right.estimate(model));
    }
}
