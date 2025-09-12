package io.github.anupam.evolvdb.exec.plan;

import io.github.anupam.evolvdb.exec.ExecContext;
import io.github.anupam.evolvdb.exec.op.FilterExec;
import io.github.anupam.evolvdb.exec.op.PhysicalOperator;
import io.github.anupam.evolvdb.optimizer.Cost;
import io.github.anupam.evolvdb.optimizer.CostModel;
import io.github.anupam.evolvdb.sql.ast.Expr;
import io.github.anupam.evolvdb.types.Schema;

import java.util.List;

public final class FilterPlan implements PhysicalPlan {
    private final PhysicalPlan child;
    private final Expr predicate;

    public FilterPlan(PhysicalPlan child, Expr predicate) {
        this.child = child;
        this.predicate = predicate;
    }

    @Override public Schema schema() { return child.schema(); }
    @Override public List<PhysicalPlan> children() { return List.of(child); }

    @Override
    public PhysicalOperator create(ExecContext context) {
        return new FilterExec(child.create(context), predicate);
    }

    @Override
    public Cost estimate(CostModel model) {
        return model.costFilter(child.estimate(model));
    }
}
