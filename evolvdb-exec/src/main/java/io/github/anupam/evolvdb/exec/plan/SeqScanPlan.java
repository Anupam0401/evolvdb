package io.github.anupam.evolvdb.exec.plan;

import io.github.anupam.evolvdb.exec.ExecContext;
import io.github.anupam.evolvdb.exec.op.PhysicalOperator;
import io.github.anupam.evolvdb.exec.op.SeqScanExec;
import io.github.anupam.evolvdb.optimizer.Cost;
import io.github.anupam.evolvdb.optimizer.CostModel;
import io.github.anupam.evolvdb.types.Schema;

import java.util.List;

public final class SeqScanPlan implements PhysicalPlan {
    private final String tableName;
    private final Schema schema;

    public SeqScanPlan(String tableName, Schema schema) {
        this.tableName = tableName;
        this.schema = schema;
    }

    @Override public Schema schema() { return schema; }
    @Override public List<PhysicalPlan> children() { return List.of(); }

    @Override
    public PhysicalOperator create(ExecContext context) {
        return new SeqScanExec(context.catalog(), tableName);
    }

    @Override
    public Cost estimate(CostModel model) {
        return model.costSeqScan(tableName, schema);
    }
}
