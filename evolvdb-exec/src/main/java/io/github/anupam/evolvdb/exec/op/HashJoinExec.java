package io.github.anupam.evolvdb.exec.op;

import io.github.anupam.evolvdb.exec.expr.ExprEvaluator;
import io.github.anupam.evolvdb.sql.ast.Expr;
import io.github.anupam.evolvdb.types.Schema;
import io.github.anupam.evolvdb.types.Tuple;

import java.util.*;

/** Simple in-memory hash join (inner, equi-join). Builds a hash table on the right side. */
public final class HashJoinExec implements PhysicalOperator {
    private final PhysicalOperator left;
    private final PhysicalOperator right;
    private final Expr leftKey;
    private final Expr rightKey;
    private final Schema outSchema;
    private final Set<String> leftQuals;
    private final Set<String> rightQuals;

    private final ExprEvaluator evaluator = new ExprEvaluator();

    private final Map<Object, List<Tuple>> hash = new HashMap<>();
    private Tuple curLeft;
    private Iterator<Tuple> matchIter;

    public HashJoinExec(
        PhysicalOperator left,
        PhysicalOperator right,
        Expr leftKey,
        Expr rightKey,
        Schema outSchema,
        Set<String> leftQuals,
        Set<String> rightQuals
    ) {
        this.left = left;
        this.right = right;
        this.leftKey = leftKey;
        this.rightKey = rightKey;
        this.outSchema = outSchema;
        this.leftQuals = leftQuals == null ? Set.of() : new HashSet<>(leftQuals);
        this.rightQuals = rightQuals == null ? Set.of() : new HashSet<>(rightQuals);
    }

    @Override
    public void open() throws Exception {
        left.open();
        right.open();
        // Build hash on right
        for (Tuple t = right.next(); t != null; t = right.next()) {
            Object k = evaluator.eval(rightKey, t, right.schema());
            hash.computeIfAbsent(k, kk -> new ArrayList<>()).add(t);
        }
        right.close();
        curLeft = left.next();
        matchIter = null;
    }

    @Override
    public Tuple next() throws Exception {
        while (curLeft != null) {
            if (matchIter == null) {
                Object lk = evaluator.eval(leftKey, curLeft, left.schema());
                matchIter = hash.getOrDefault(lk, List.of()).iterator();
                if (!matchIter.hasNext()) {
                    curLeft = left.next();
                    continue;
                }
            }
            if (!matchIter.hasNext()) {
                // Exhausted matches for current left row; advance left
                curLeft = left.next();
                matchIter = null;
                continue;
            }
            Tuple r = matchIter.next();
            List<Object> vals = new ArrayList<>(left.schema().size() + right.schema().size());
            vals.addAll(curLeft.values());
            vals.addAll(r.values());
            return new Tuple(outSchema, vals);
        }
        return null;
    }

    @Override
    public void close() throws Exception {
        left.close();
        hash.clear();
        curLeft = null;
        matchIter = null;
    }

    @Override
    public Schema schema() { return outSchema; }
}
