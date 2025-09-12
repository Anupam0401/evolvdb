package io.github.anupam.evolvdb.exec.plan;

import io.github.anupam.evolvdb.exec.ExecContext;
import io.github.anupam.evolvdb.exec.op.PhysicalOperator;
import io.github.anupam.evolvdb.exec.op.ProjectExec;
import io.github.anupam.evolvdb.optimizer.Cost;
import io.github.anupam.evolvdb.optimizer.CostModel;
import io.github.anupam.evolvdb.planner.logical.ProjectItem;
import io.github.anupam.evolvdb.types.Schema;

import java.util.List;

public final class ProjectPlan implements PhysicalPlan {
    private final PhysicalPlan child;
    private final List<ProjectItem> items;
    private final Schema outSchema;

    public ProjectPlan(PhysicalPlan child, List<ProjectItem> items, Schema outSchema) {
        this.child = child;
        this.items = List.copyOf(items);
        this.outSchema = outSchema;
    }

    @Override public Schema schema() { return outSchema; }
    @Override public List<PhysicalPlan> children() { return List.of(child); }

    @Override
    public PhysicalOperator create(ExecContext context) {
        return new ProjectExec(child.create(context), items, outSchema);
    }

    @Override
    public Cost estimate(CostModel model) {
        return model.costProject(child.estimate(model));
    }
}
