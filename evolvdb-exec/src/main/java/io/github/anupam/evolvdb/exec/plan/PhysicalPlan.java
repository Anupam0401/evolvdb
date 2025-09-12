package io.github.anupam.evolvdb.exec.plan;

import io.github.anupam.evolvdb.exec.ExecContext;
import io.github.anupam.evolvdb.exec.op.PhysicalOperator;
import io.github.anupam.evolvdb.types.Schema;
import io.github.anupam.evolvdb.optimizer.Cost;
import io.github.anupam.evolvdb.optimizer.CostModel;

import java.util.List;

/** Physical plan node. Builds a Volcano operator at runtime. */
public interface PhysicalPlan {
    Schema schema();
    List<PhysicalPlan> children();
    PhysicalOperator create(ExecContext context);

    /** Estimate the cumulative cost of this subtree under the given cost model. */
    Cost estimate(CostModel model);
    /** Convenience accessor: estimated output rows for this subtree. */
    default double estimatedRowCount(CostModel model) { return estimate(model).rowCount(); }
}
