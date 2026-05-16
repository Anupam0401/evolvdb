package io.github.anupam.evolvdb.exec.plan;

import java.util.List;

import io.github.anupam.evolvdb.exec.ExecContext;
import io.github.anupam.evolvdb.exec.op.PhysicalOperator;
import io.github.anupam.evolvdb.exec.op.SeqScanWithRidExec;
import io.github.anupam.evolvdb.optimizer.Cost;
import io.github.anupam.evolvdb.optimizer.CostModel;
import io.github.anupam.evolvdb.types.Schema;

/**
 * Physical plan for a sequential scan that tracks RecordIds. Used as the leaf operator inside
 * UPDATE/DELETE plans so that DML operators can identify which physical record to modify.
 */
public final class SeqScanWithRidPlan implements PhysicalPlan {
    private final String tableName;
    private final Schema schema;

    public SeqScanWithRidPlan(String tableName, Schema schema) {
        this.tableName = tableName;
        this.schema = schema;
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public List<PhysicalPlan> children() {
        return List.of();
    }

    @Override
    public PhysicalOperator create(ExecContext context) {
        return new SeqScanWithRidExec(context.catalog(), tableName);
    }

    @Override
    public Cost estimate(CostModel model) {
        return model.costSeqScan(tableName, schema);
    }
}
