package io.github.anupam.evolvdb.exec.plan;

import io.github.anupam.evolvdb.exec.ExecContext;
import io.github.anupam.evolvdb.exec.op.InsertExec;
import io.github.anupam.evolvdb.exec.op.PhysicalOperator;
import io.github.anupam.evolvdb.optimizer.Cost;
import io.github.anupam.evolvdb.optimizer.CostModel;
import io.github.anupam.evolvdb.planner.logical.LogicalInsert;
import io.github.anupam.evolvdb.types.ColumnMeta;
import io.github.anupam.evolvdb.types.Schema;
import io.github.anupam.evolvdb.types.Type;

import java.util.List;

public final class InsertPlan implements PhysicalPlan {
    private final LogicalInsert insert;

    public InsertPlan(LogicalInsert insert) {
        this.insert = insert;
    }

    @Override
    public Schema schema() {
        return new Schema(List.of(new ColumnMeta("inserted_count", Type.INT, null)));
    }

    @Override
    public List<PhysicalPlan> children() { return List.of(); }

    @Override
    public PhysicalOperator create(ExecContext context) {
        return new InsertExec(context.catalog(), insert);
    }

    @Override
    public Cost estimate(CostModel model) {
        return model.costInsert(insert.rows().size());
    }
}
