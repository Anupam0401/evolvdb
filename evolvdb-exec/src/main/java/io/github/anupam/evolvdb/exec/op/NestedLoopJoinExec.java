package io.github.anupam.evolvdb.exec.op;

import io.github.anupam.evolvdb.exec.expr.ExprEvaluator;
import io.github.anupam.evolvdb.sql.ast.Expr;
import io.github.anupam.evolvdb.types.Schema;
import io.github.anupam.evolvdb.types.Tuple;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Baseline nested-loop join (inner) with optional predicate. Buffers right side in memory. */
public final class NestedLoopJoinExec implements PhysicalOperator {
    private final PhysicalOperator left;
    private final PhysicalOperator right;
    private final Expr predicate; // nullable for cross join
    private final Schema outSchema;
    private final Set<String> leftQuals;
    private final Set<String> rightQuals;

    private final ExprEvaluator evaluator = new ExprEvaluator();

    private final List<Tuple> rightRows = new ArrayList<>();
    private Tuple curLeft;
    private int rightIdx;

    public NestedLoopJoinExec(PhysicalOperator left,
                              PhysicalOperator right,
                              Expr predicate,
                              Schema outSchema,
                              Set<String> leftQuals,
                              Set<String> rightQuals) {
        this.left = left;
        this.right = right;
        this.predicate = predicate;
        this.outSchema = outSchema;
        this.leftQuals = (leftQuals == null ? Set.of() : new HashSet<>(leftQuals));
        this.rightQuals = (rightQuals == null ? Set.of() : new HashSet<>(rightQuals));
    }

    @Override
    public void open() throws Exception {
        left.open();
        right.open();
        // buffer entire right side
        for (Tuple t = right.next(); t != null; t = right.next()) rightRows.add(t);
        right.close();
        curLeft = left.next();
        rightIdx = 0;
    }

    @Override
    public Tuple next() throws Exception {
        while (curLeft != null) {
            while (rightIdx < rightRows.size()) {
                Tuple r = rightRows.get(rightIdx++);
                if (predicate == null || Boolean.TRUE.equals(
                        evaluator.eval(predicate, curLeft, left.schema(), r, right.schema(), leftQuals, rightQuals))) {
                    List<Object> vals = new ArrayList<>(left.schema().size() + right.schema().size());
                    vals.addAll(curLeft.values());
                    vals.addAll(r.values());
                    return new Tuple(outSchema, vals);
                }
            }
            // advance left and reset right
            curLeft = left.next();
            rightIdx = 0;
        }
        return null;
    }

    @Override
    public void close() throws Exception {
        left.close();
        rightRows.clear();
        curLeft = null;
        rightIdx = 0;
    }

    @Override public Schema schema() { return outSchema; }
}
