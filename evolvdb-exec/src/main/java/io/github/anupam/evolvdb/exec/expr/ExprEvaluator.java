package io.github.anupam.evolvdb.exec.expr;

import io.github.anupam.evolvdb.sql.ast.*;
import io.github.anupam.evolvdb.types.ColumnMeta;
import io.github.anupam.evolvdb.types.Schema;
import io.github.anupam.evolvdb.types.Tuple;

import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/** Evaluates SQL AST expressions against tuples at runtime. */
public final class ExprEvaluator {

    public Object eval(Expr expr, Tuple tuple, Schema schema) {
        return eval(expr, tuple, schema, null, null, Set.of(), Set.of());
    }

    public Object eval(Expr expr,
                       Tuple left, Schema leftSchema,
                       Tuple right, Schema rightSchema,
                       Set<String> leftQuals, Set<String> rightQuals) {
        Objects.requireNonNull(expr, "expr");
        if (expr instanceof Literal lit) return lit.value();
        if (expr instanceof ColumnRef cr) {
            return resolveColumnValue(cr, left, leftSchema, right, rightSchema, leftQuals, rightQuals);
        }
        if (expr instanceof BinaryExpr be) {
            Object l = eval(be.left(), left, leftSchema, right, rightSchema, leftQuals, rightQuals);
            Object r = eval(be.right(), left, leftSchema, right, rightSchema, leftQuals, rightQuals);
            return evalBinary(be.op(), l, r);
        }
        if (expr instanceof ComparisonExpr ce) {
            Object l = eval(ce.left(), left, leftSchema, right, rightSchema, leftQuals, rightQuals);
            Object r = eval(ce.right(), left, leftSchema, right, rightSchema, leftQuals, rightQuals);
            return evalComparison(ce.op(), l, r);
        }
        if (expr instanceof LogicalExpr le) {
            Object lv = eval(le.left(), left, leftSchema, right, rightSchema, leftQuals, rightQuals);
            if (le.op() == LogicalExpr.Op.NOT) return !(Boolean) asBoolean(lv);
            Object rv = le.right() != null ? eval(le.right(), left, leftSchema, right, rightSchema, leftQuals, rightQuals) : null;
            return switch (le.op()) {
                case AND -> (Boolean) asBoolean(lv) && (Boolean) asBoolean(rv);
                case OR -> (Boolean) asBoolean(lv) || (Boolean) asBoolean(rv);
                case NOT -> throw new IllegalStateException("unreachable");
            };
        }
        if (expr instanceof FuncCall) {
            throw new IllegalStateException("Scalar evaluation of aggregates not supported here");
        }
        throw new IllegalArgumentException("Unsupported expression: " + expr.getClass().getSimpleName());
    }

    private Object resolveColumnValue(ColumnRef cr,
                                      Tuple left, Schema leftSchema,
                                      Tuple right, Schema rightSchema,
                                      Set<String> leftQuals, Set<String> rightQuals) {
        String col = cr.column();
        String tbl = cr.table();
        if (right != null && rightSchema != null) {
            // join context
            if (tbl != null) {
                String q = tbl.toLowerCase(Locale.ROOT);
                if (leftQuals.contains(q)) {
                    Integer idx = resolveIndex(leftSchema, tbl, col);
                    if (idx == null) idx = resolveIndex(leftSchema, null, col);
                    if (idx == null) throw err(cr, "Unknown column: " + tbl + "." + col);
                    return left.get(idx);
                } else if (rightQuals.contains(q)) {
                    Integer idx = resolveIndex(rightSchema, tbl, col);
                    if (idx == null) idx = resolveIndex(rightSchema, null, col);
                    if (idx == null) throw err(cr, "Unknown column: " + tbl + "." + col);
                    return right.get(idx);
                } else {
                    throw err(cr, "Unknown table qualifier: " + tbl);
                }
            } else {
                Integer li = resolveIndex(leftSchema, null, col);
                Integer ri = resolveIndex(rightSchema, null, col);
                if (li != null && ri != null) throw err(cr, "Ambiguous column: " + col);
                if (li != null) return left.get(li);
                if (ri != null) return right.get(ri);
                // try qualified names embedded in schema
                li = resolveIndex(leftSchema, "", col);
                if (li != null) return left.get(li);
                ri = resolveIndex(rightSchema, "", col);
                if (ri != null) return right.get(ri);
                throw err(cr, "Unknown column: " + col);
            }
        } else {
            // single-tuple context
            if (tbl != null) {
                Integer idx = resolveIndex(leftSchema, tbl, col);
                if (idx == null) idx = resolveIndex(leftSchema, null, col); // fallback for schemas with unqualified names
                if (idx == null) throw err(cr, "Unknown column: " + tbl + "." + col);
                return left.get(idx);
            } else {
                Integer idx = resolveIndex(leftSchema, null, col);
                if (idx == null) idx = resolveIndex(leftSchema, "", col);
                if (idx == null) throw err(cr, "Unknown column: " + col);
                return left.get(idx);
            }
        }
    }

    private static Integer resolveIndex(Schema schema, String qualifierOrNull, String column) {
        String want = qualifierOrNull == null ? column : (qualifierOrNull.isEmpty() ? column : qualifierOrNull + "." + column);
        String wantLc = want.toLowerCase(Locale.ROOT);
        for (int i = 0; i < schema.size(); i++) {
            ColumnMeta cm = schema.columns().get(i);
            if (cm.name().toLowerCase(Locale.ROOT).equals(wantLc)) return i;
        }
        if (qualifierOrNull != null && !qualifierOrNull.isEmpty()) return null;
        // try match ignoring qualifier if schema contains qualified names
        for (int i = 0; i < schema.size(); i++) {
            ColumnMeta cm = schema.columns().get(i);
            String n = cm.name();
            int dot = n.indexOf('.');
            if (dot > 0) n = n.substring(dot + 1);
            if (n.equalsIgnoreCase(column)) return i;
        }
        return null;
    }

    private static Boolean asBoolean(Object o) {
        if (!(o instanceof Boolean b)) throw new IllegalArgumentException("Expected BOOLEAN, got " + o);
        return b;
    }

    private static Object evalBinary(BinaryExpr.Op op, Object l, Object r) {
        // Handle CONCAT specially - it always produces a string
        if (op == BinaryExpr.Op.CONCAT) {
            return toStringLike(l) + toStringLike(r);
        }
        
        // Numeric operations
        if (l instanceof Float || r instanceof Float) {
            float lf = toFloat(l);
            float rf = toFloat(r);
            return switch (op) {
                case ADD -> lf + rf;
                case SUB -> lf - rf;
                case MUL -> lf * rf;
                case DIV -> lf / rf;
                case CONCAT -> throw new IllegalStateException("unreachable");
            };
        }
        if (l instanceof Long || r instanceof Long) {
            long ll = toLong(l);
            long rl = toLong(r);
            return switch (op) {
                case ADD -> ll + rl;
                case SUB -> ll - rl;
                case MUL -> ll * rl;
                case DIV -> ll / rl;
                case CONCAT -> throw new IllegalStateException("unreachable");
            };
        }
        if (l instanceof Integer || r instanceof Integer) {
            int li = toInt(l);
            int ri = toInt(r);
            return switch (op) {
                case ADD -> li + ri;
                case SUB -> li - ri;
                case MUL -> li * ri;
                case DIV -> li / ri;
                case CONCAT -> throw new IllegalStateException("unreachable");
            };
        }
        throw new IllegalArgumentException("Unsupported binary types: " + l + ", " + r);
    }

    private static Object evalComparison(ComparisonExpr.Op op, Object l, Object r) {
        int c = compare(l, r);
        return switch (op) {
            case EQ -> c == 0;
            case NEQ -> c != 0;
            case LT -> c < 0;
            case LTE -> c <= 0;
            case GT -> c > 0;
            case GTE -> c >= 0;
        };
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
        return toStringLike(l).compareTo(toStringLike(r));
    }

    private static int toInt(Object o) {
        if (o instanceof Integer i) return i;
        if (o instanceof Long l) return (int) (long) l;
        if (o instanceof Float f) return (int) (float) f;
        throw new IllegalArgumentException("Not a number: " + o);
    }
    private static long toLong(Object o) {
        if (o instanceof Integer i) return i.longValue();
        if (o instanceof Long l) return l;
        if (o instanceof Float f) return (long) f.floatValue();
        throw new IllegalArgumentException("Not a number: " + o);
    }
    private static float toFloat(Object o) {
        if (o instanceof Integer i) return i.floatValue();
        if (o instanceof Long l) return l.floatValue();
        if (o instanceof Float f) return f;
        throw new IllegalArgumentException("Not a number: " + o);
    }
    private static String toStringLike(Object o) { return String.valueOf(o); }

    private static IllegalArgumentException err(Expr e, String msg) {
        return new IllegalArgumentException(msg + " at " + e.pos().line() + ":" + e.pos().column());
    }
}
