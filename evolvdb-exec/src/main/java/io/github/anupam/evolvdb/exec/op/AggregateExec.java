package io.github.anupam.evolvdb.exec.op;

import io.github.anupam.evolvdb.exec.expr.ExprEvaluator;
import io.github.anupam.evolvdb.planner.logical.ProjectItem;
import io.github.anupam.evolvdb.sql.ast.Expr;
import io.github.anupam.evolvdb.sql.ast.FuncCall;
import io.github.anupam.evolvdb.types.Schema;
import io.github.anupam.evolvdb.types.Tuple;

import java.util.*;

/** Naive GROUP BY aggregate: buffers all groups and computes aggregates. */
public final class AggregateExec implements PhysicalOperator {
    private final PhysicalOperator child;
    private final List<Expr> groupBy;
    private final List<ProjectItem> outputs; // mix of group exprs and aggregates
    private final Schema outSchema;

    private final ExprEvaluator evaluator = new ExprEvaluator();

    private Iterator<Tuple> resultIter;

    public AggregateExec(PhysicalOperator child, List<Expr> groupBy, List<ProjectItem> outputs, Schema outSchema) {
        this.child = child;
        this.groupBy = List.copyOf(groupBy);
        this.outputs = List.copyOf(outputs);
        this.outSchema = outSchema;
    }

    @Override
    public void open() throws Exception {
        child.open();
        Map<List<Object>, GroupState> groups = new LinkedHashMap<>();
        for (Tuple t = child.next(); t != null; t = child.next()) {
            List<Object> key = new ArrayList<>(groupBy.size());
            for (Expr g : groupBy) {
                key.add(evaluator.eval(g, t, child.schema()));
            }
            List<Object> mapKey = List.copyOf(key);
            GroupState st = groups.get(mapKey);
            if (st == null) {
                st = new GroupState(t);
                groups.put(mapKey, st);
            }
            st.update(t);
        }
        child.close();
        // build result tuples
        List<Tuple> results = new ArrayList<>(groups.size());
        for (GroupState st : groups.values()) {
            List<Object> row = new ArrayList<>(outputs.size());
            for (int i = 0; i < outputs.size(); i++) {
                ProjectItem it = outputs.get(i);
                if (it.expr() instanceof FuncCall fc) {
                    row.add(st.evalAgg(i, fc));
                } else {
                    row.add(evaluator.eval(it.expr(), st.sample, child.schema()));
                }
            }
            results.add(new Tuple(outSchema, row));
        }
        resultIter = results.iterator();
    }

    @Override
    public Tuple next() {
        if (resultIter == null || !resultIter.hasNext()) return null;
        return resultIter.next();
    }

    @Override
    public void close() {
        resultIter = null;
    }

    @Override
    public Schema schema() { return outSchema; }

    private final class GroupState {
        final Tuple sample;
        final Map<Integer, AggState> aggs = new HashMap<>(); // output index -> state

        GroupState(Tuple sample) { this.sample = sample; }

        void update(Tuple t) {
            for (int i = 0; i < outputs.size(); i++) {
                ProjectItem it = outputs.get(i);
                if (it.expr() instanceof FuncCall fc) {
                    AggState s = aggs.computeIfAbsent(i, k -> createAgg(fc));
                    Object v = null;
                    if (!fc.starArg()) {
                        if (fc.args().size() != 1) throw new IllegalArgumentException("Aggregate arg count");
                        v = evaluator.eval(fc.args().get(0), t, child.schema());
                    }
                    s.add(v);
                }
            }
        }

        Object evalAgg(int idx, FuncCall fc) {
            AggState s = aggs.get(idx);
            if (s == null) {
                s = createAgg(fc);
                aggs.put(idx, s);
            }
            return s.result();
        }

        AggState createAgg(FuncCall fc) {
            String fn = fc.name().toUpperCase(Locale.ROOT);
            return switch (fn) {
                case "COUNT" -> new CountAgg();
                case "SUM" -> new SumAgg();
                case "AVG" -> new AvgAgg();
                case "MIN" -> new MinMaxAgg(true);
                case "MAX" -> new MinMaxAgg(false);
                default -> throw new IllegalArgumentException("Unknown agg: " + fc.name());
            };
        }
    }

    private interface AggState { void add(Object v); Object result(); }

    private static final class CountAgg implements AggState {
        long c = 0;
        @Override public void add(Object v) { c++; }
        @Override public Object result() { return Long.valueOf(c); }
    }

    private static final class SumAgg implements AggState {
        boolean f = false; double sf = 0; long sl = 0;
        @Override public void add(Object v) {
            if (v instanceof Float) { f = true; sf += (Float) v; }
            else if (v instanceof Long) sl += (Long) v;
            else if (v instanceof Integer) sl += (Integer) v;
            else if (v == null) { /* ignore */ }
            else throw new IllegalArgumentException("SUM unsupported type: " + v);
        }
        @Override public Object result() { return f ? Float.valueOf((float) sf) : Long.valueOf(sl); }
    }

    private static final class AvgAgg implements AggState {
        double sum = 0; long cnt = 0;
        @Override public void add(Object v) {
            if (v instanceof Float) { sum += ((Float) v).doubleValue(); cnt++; }
            else if (v instanceof Long) { sum += ((Long) v).doubleValue(); cnt++; }
            else if (v instanceof Integer) { sum += ((Integer) v).doubleValue(); cnt++; }
            else if (v == null) { /* ignore */ }
            else throw new IllegalArgumentException("AVG unsupported type: " + v);
        }
        @Override public Object result() { return Float.valueOf((float) (sum / (cnt == 0 ? 1 : cnt))); }
    }

    private static final class MinMaxAgg implements AggState {
        final boolean isMin;
        Object cur = null;
        MinMaxAgg(boolean isMin) { this.isMin = isMin; }
        @Override public void add(Object v) {
            if (v == null) return;
            if (cur == null) { cur = v; return; }
            int c = compare(v, cur);
            if ((isMin && c < 0) || (!isMin && c > 0)) cur = v;
        }
        @Override public Object result() { return cur; }
    }

    private static int compare(Object l, Object r) {
        if (l instanceof Float || r instanceof Float) {
            float lf = toFloat(l); float rf = toFloat(r);
            return Float.compare(lf, rf);
        }
        if (l instanceof Long || r instanceof Long) {
            long ll = toLong(l); long rl = toLong(r);
            return Long.compare(ll, rl);
        }
        if (l instanceof Integer || r instanceof Integer) {
            int li = toInt(l); int ri = toInt(r);
            return Integer.compare(li, ri);
        }
        if (l instanceof Boolean && r instanceof Boolean) {
            return Boolean.compare((Boolean) l, (Boolean) r);
        }
        return String.valueOf(l).compareTo(String.valueOf(r));
    }
    private static int toInt(Object o) { if (o instanceof Integer i) return i; if (o instanceof Long l) return (int)(long)l; if (o instanceof Float f) return (int)(float)f; throw new IllegalArgumentException(); }
    private static long toLong(Object o) { if (o instanceof Integer i) return i.longValue(); if (o instanceof Long l) return l; if (o instanceof Float f) return (long)f.floatValue(); throw new IllegalArgumentException(); }
    private static float toFloat(Object o) { if (o instanceof Integer i) return i.floatValue(); if (o instanceof Long l) return l.floatValue(); if (o instanceof Float f) return f; throw new IllegalArgumentException(); }
}
