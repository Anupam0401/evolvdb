package io.github.anupam.evolvdb.exec.op;

import io.github.anupam.evolvdb.exec.expr.ExprEvaluator;
import io.github.anupam.evolvdb.sql.ast.Expr;
import io.github.anupam.evolvdb.types.Schema;
import io.github.anupam.evolvdb.types.Tuple;

import java.util.*;

/** In-memory sort-merge join for inner equi-join on a single key. */
public final class SortMergeJoinExec implements PhysicalOperator {
    private final PhysicalOperator left;
    private final PhysicalOperator right;
    private final Expr leftKey;
    private final Expr rightKey;
    private final Schema outSchema;
    private final Set<String> leftQuals;
    private final Set<String> rightQuals;

    private final ExprEvaluator evaluator = new ExprEvaluator();

    private List<Tuple> lrows;
    private List<Tuple> rrows;
    private int li;
    private int ri;
    private List<Tuple> currentMatches; // all right tuples matching current left key
    private int matchIndex;

    public SortMergeJoinExec(PhysicalOperator left,
                             PhysicalOperator right,
                             Expr leftKey,
                             Expr rightKey,
                             Schema outSchema,
                             Set<String> leftQuals,
                             Set<String> rightQuals) {
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
        lrows = new ArrayList<>();
        rrows = new ArrayList<>();
        for (Tuple t = left.next(); t != null; t = left.next()) lrows.add(t);
        for (Tuple t = right.next(); t != null; t = right.next()) rrows.add(t);
        right.close();
        // Sort both sides by their respective keys
        Comparator<Tuple> lcmp = Comparator.comparing(o -> (Comparable) keyOfLeft(o));
        Comparator<Tuple> rcmp = Comparator.comparing(o -> (Comparable) keyOfRight(o));
        lrows.sort(lcmp);
        rrows.sort(rcmp);
        li = 0; ri = 0;
        currentMatches = List.of();
        matchIndex = 0;
    }

    private Object keyOfLeft(Tuple t) {
        return evaluator.eval(leftKey, t, left.schema(), null, null, leftQuals, rightQuals);
    }
    private Object keyOfRight(Tuple t) {
        return evaluator.eval(rightKey, null, null, t, right.schema(), leftQuals, rightQuals);
    }

    @Override
    public Tuple next() throws Exception {
        while (true) {
            if (currentMatches != null && matchIndex < currentMatches.size()) {
                Tuple r = currentMatches.get(matchIndex++);
                Tuple l = lrows.get(li);
                List<Object> vals = new ArrayList<>(left.schema().size() + right.schema().size());
                vals.addAll(l.values());
                vals.addAll(r.values());
                return new Tuple(outSchema, vals);
            }
            if (li >= lrows.size() || ri >= rrows.size()) return null;
            // Advance merge pointers to next equal key range
            Object lk = keyOfLeft(lrows.get(li));
            Object rk = keyOfRight(rrows.get(ri));
            int cmp = compareKeys(lk, rk);
            if (cmp < 0) { li++; continue; }
            if (cmp > 0) { ri++; continue; }
            // Collect all rights with key rk
            Object key = rk;
            List<Tuple> rights = new ArrayList<>();
            int rj = ri;
            while (rj < rrows.size() && compareKeys(key, keyOfRight(rrows.get(rj))) == 0) {
                rights.add(rrows.get(rj++));
            }
            // For current left key range, we will emit all combinations with rights
            Object curKey = lk;
            // Prepare currentMatches for first left row with this key
            currentMatches = rights;
            matchIndex = 0;
            // After we exhaust matches for this left row, we may have additional left rows with same key
            // so we keep ri at rj and we will move li forward as we iterate.
            // However, to keep logic simple, we will emit for each left row separately by keeping rights list.
            // Move li forward only when currentMatches are exhausted and next left key differs.
            // For now, return first pair; subsequent calls will keep using currentMatches and same li until exhausted.
        }
    }

    @Override
    public void close() throws Exception {
        left.close();
        lrows = null; rrows = null;
        currentMatches = null;
    }

    @Override
    public Schema schema() { return outSchema; }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private int compareKeys(Object a, Object b) {
        if (a == null && b == null) return 0;
        if (a == null) return -1;
        if (b == null) return 1;
        if (a instanceof Comparable ca && b.getClass().isAssignableFrom(a.getClass())) {
            return ca.compareTo(b);
        }
        // Fallback to string compare
        return String.valueOf(a).compareTo(String.valueOf(b));
    }
}
