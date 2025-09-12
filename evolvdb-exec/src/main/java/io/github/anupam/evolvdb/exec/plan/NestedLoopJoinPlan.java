package io.github.anupam.evolvdb.exec.plan;

import io.github.anupam.evolvdb.exec.ExecContext;
import io.github.anupam.evolvdb.exec.op.NestedLoopJoinExec;
import io.github.anupam.evolvdb.exec.op.PhysicalOperator;
import io.github.anupam.evolvdb.optimizer.Cost;
import io.github.anupam.evolvdb.optimizer.CostModel;
import io.github.anupam.evolvdb.sql.ast.Expr;
import io.github.anupam.evolvdb.types.Schema;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class NestedLoopJoinPlan implements PhysicalPlan {
    private final PhysicalPlan left;
    private final PhysicalPlan right;
    private final Expr predicate; // nullable
    private final Schema outSchema;
    private final Set<String> leftQuals;
    private final Set<String> rightQuals;

    public NestedLoopJoinPlan(PhysicalPlan left,
                              PhysicalPlan right,
                              Expr predicate,
                              Schema outSchema,
                              Set<String> leftQuals,
                              Set<String> rightQuals) {
        this.left = left;
        this.right = right;
        this.predicate = predicate;
        this.outSchema = outSchema;
        this.leftQuals = leftQuals == null ? Set.of() : new HashSet<>(leftQuals);
        this.rightQuals = rightQuals == null ? Set.of() : new HashSet<>(rightQuals);
    }

    @Override public Schema schema() { return outSchema; }
    @Override public List<PhysicalPlan> children() { return List.of(left, right); }

    @Override
    public PhysicalOperator create(ExecContext context) {
        PhysicalOperator l = left.create(context);
        PhysicalOperator r = right.create(context);
        return new NestedLoopJoinExec(l, r, predicate, outSchema, leftQuals, rightQuals);
    }

    @Override
    public Cost estimate(CostModel model) {
        return model.costNestedLoopJoin(left.estimate(model), right.estimate(model));
    }
}
