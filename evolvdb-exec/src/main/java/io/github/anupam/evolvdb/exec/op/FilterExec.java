package io.github.anupam.evolvdb.exec.op;

import io.github.anupam.evolvdb.exec.expr.ExprEvaluator;
import io.github.anupam.evolvdb.sql.ast.Expr;
import io.github.anupam.evolvdb.types.Schema;
import io.github.anupam.evolvdb.types.Tuple;

/** Filters tuples from child using a boolean predicate. */
public final class FilterExec implements PhysicalOperator {
    private final PhysicalOperator child;
    private final Expr predicate;
    private final ExprEvaluator evaluator = new ExprEvaluator();

    public FilterExec(PhysicalOperator child, Expr predicate) {
        this.child = child;
        this.predicate = predicate;
    }

    @Override public void open() throws Exception { child.open(); }

    @Override
    public Tuple next() throws Exception {
        for (;;) {
            Tuple t = child.next();
            if (t == null) return null;
            Object v = evaluator.eval(predicate, t, child.schema());
            if (Boolean.TRUE.equals(v)) return t;
        }
    }

    @Override public void close() throws Exception { child.close(); }

    @Override public Schema schema() { return child.schema(); }
}
