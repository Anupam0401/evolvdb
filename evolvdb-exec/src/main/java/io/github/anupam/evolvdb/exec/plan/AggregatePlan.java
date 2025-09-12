package io.github.anupam.evolvdb.exec.plan;

import io.github.anupam.evolvdb.exec.ExecContext;
import io.github.anupam.evolvdb.exec.op.AggregateExec;
import io.github.anupam.evolvdb.exec.op.PhysicalOperator;
import io.github.anupam.evolvdb.optimizer.Cost;
import io.github.anupam.evolvdb.optimizer.CostModel;
import io.github.anupam.evolvdb.planner.logical.ProjectItem;
import io.github.anupam.evolvdb.sql.ast.Expr;
import io.github.anupam.evolvdb.types.Schema;

import java.util.List;

public final class AggregatePlan implements PhysicalPlan {
    private final PhysicalPlan child;
    private final List<Expr> groupBy;
    private final List<ProjectItem> aggregates;
    private final Schema outSchema;

    public AggregatePlan(PhysicalPlan child, List<Expr> groupBy, List<ProjectItem> aggregates, Schema outSchema) {
        this.child = child;
        this.groupBy = List.copyOf(groupBy);
        this.aggregates = List.copyOf(aggregates);
        this.outSchema = outSchema;
    }

    @Override public Schema schema() { return outSchema; }
    @Override public List<PhysicalPlan> children() { return List.of(child); }

    @Override
    public PhysicalOperator create(ExecContext context) {
        return new AggregateExec(child.create(context), groupBy, aggregates, outSchema);
    }

    @Override
    public Cost estimate(CostModel model) {
        return model.costAggregate(child.estimate(model));
    }
}
