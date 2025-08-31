package io.github.anupam.evolvdb.exec.op;

import io.github.anupam.evolvdb.exec.expr.ExprEvaluator;
import io.github.anupam.evolvdb.planner.logical.ProjectItem;
import io.github.anupam.evolvdb.types.Schema;
import io.github.anupam.evolvdb.types.Tuple;

import java.util.ArrayList;
import java.util.List;

/** Computes projection expressions and produces tuples with the given output schema. */
public final class ProjectExec implements PhysicalOperator {
    private final PhysicalOperator child;
    private final List<ProjectItem> items;
    private final Schema outSchema;
    private final ExprEvaluator evaluator = new ExprEvaluator();

    public ProjectExec(PhysicalOperator child, List<ProjectItem> items, Schema outSchema) {
        this.child = child;
        this.items = List.copyOf(items);
        this.outSchema = outSchema;
    }

    @Override public void open() throws Exception { child.open(); }

    @Override
    public Tuple next() throws Exception {
        Tuple t = child.next();
        if (t == null) return null;
        List<Object> out = new ArrayList<>(items.size());
        for (ProjectItem it : items) {
            Object v = evaluator.eval(it.expr(), t, child.schema());
            out.add(v);
        }
        return new Tuple(outSchema, out);
    }

    @Override public void close() throws Exception { child.close(); }
    @Override public Schema schema() { return outSchema; }
}
