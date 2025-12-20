package io.github.anupam.evolvdb.exec.plan;

import java.util.List;

import io.github.anupam.evolvdb.exec.ExecContext;
import io.github.anupam.evolvdb.exec.op.DeleteExec;
import io.github.anupam.evolvdb.exec.op.PhysicalOperator;
import io.github.anupam.evolvdb.optimizer.Cost;
import io.github.anupam.evolvdb.optimizer.CostModel;
import io.github.anupam.evolvdb.planner.logical.LogicalDelete;
import io.github.anupam.evolvdb.types.ColumnMeta;
import io.github.anupam.evolvdb.types.Schema;
import io.github.anupam.evolvdb.types.Type;

public final class DeletePlan implements PhysicalPlan {
    private final PhysicalPlan child;
    private final LogicalDelete delete;

    public DeletePlan(PhysicalPlan child, LogicalDelete delete) {
        this.child = child;
        this.delete = delete;
    }

    @Override
    public Schema schema() {
        return new Schema(List.of(new ColumnMeta("deleted_count", Type.INT, null)));
    }

    @Override
    public List<PhysicalPlan> children() {
        return List.of(child);
    }

    @Override
    public PhysicalOperator create(ExecContext context) {
        PhysicalOperator childOp = child.create(context);
        return new DeleteExec(childOp, context.catalog(), delete);
    }

    @Override
    public Cost estimate(CostModel model) {
        Cost childCost = child.estimate(model);
        return model.costDelete(childCost.rowCount());
    }
}
