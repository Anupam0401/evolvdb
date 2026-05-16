package io.github.anupam.evolvdb.exec.plan;

import java.util.List;

import io.github.anupam.evolvdb.exec.ExecContext;
import io.github.anupam.evolvdb.exec.op.PhysicalOperator;
import io.github.anupam.evolvdb.exec.op.UpdateExec;
import io.github.anupam.evolvdb.optimizer.Cost;
import io.github.anupam.evolvdb.optimizer.CostModel;
import io.github.anupam.evolvdb.planner.logical.LogicalUpdate;
import io.github.anupam.evolvdb.types.ColumnMeta;
import io.github.anupam.evolvdb.types.Schema;
import io.github.anupam.evolvdb.types.Type;

public final class UpdatePlan implements PhysicalPlan {
    private final PhysicalPlan child;
    private final LogicalUpdate update;

    public UpdatePlan(PhysicalPlan child, LogicalUpdate update) {
        this.child = child;
        this.update = update;
    }

    @Override
    public Schema schema() {
        return new Schema(List.of(new ColumnMeta("updated_count", Type.INT, null)));
    }

    @Override
    public List<PhysicalPlan> children() {
        return List.of(child);
    }

    @Override
    public PhysicalOperator create(ExecContext context) {
        PhysicalOperator childOp = child.create(context);
        return new UpdateExec(childOp, context.catalog(), update);
    }

    @Override
    public Cost estimate(CostModel model) {
        Cost childCost = child.estimate(model);
        return model.costUpdate(childCost.rowCount());
    }
}
