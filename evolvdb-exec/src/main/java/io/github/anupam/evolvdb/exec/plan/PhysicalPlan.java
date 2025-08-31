package io.github.anupam.evolvdb.exec.plan;

import io.github.anupam.evolvdb.exec.ExecContext;
import io.github.anupam.evolvdb.exec.op.PhysicalOperator;
import io.github.anupam.evolvdb.types.Schema;

import java.util.List;

/** Physical plan node. Builds a Volcano operator at runtime. */
public interface PhysicalPlan {
    Schema schema();
    List<PhysicalPlan> children();
    PhysicalOperator create(ExecContext context);
}
