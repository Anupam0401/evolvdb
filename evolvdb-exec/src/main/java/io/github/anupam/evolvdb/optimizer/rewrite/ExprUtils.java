package io.github.anupam.evolvdb.optimizer.rewrite;

import io.github.anupam.evolvdb.sql.ast.*;
import io.github.anupam.evolvdb.types.ColumnMeta;
import io.github.anupam.evolvdb.types.Schema;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/** Expression utilities for rewrite rules. */
public final class ExprUtils {
    private ExprUtils() {}

    /** Collects all ColumnRef nodes within the expression. */
    public static Set<ColumnRef> collectColumnRefs(Expr e) {
        Set<ColumnRef> out = new HashSet<>();
        collect(e, out);
        return out;
    }

    private static void collect(Expr e, Set<ColumnRef> out) {
        if (e instanceof ColumnRef c) {
            out.add(c);
            return;
        }
        if (e instanceof BinaryExpr b) {
            collect(b.left(), out); collect(b.right(), out);
            return;
        }
        if (e instanceof ComparisonExpr c) {
            collect(c.left(), out); collect(c.right(), out);
            return;
        }
        if (e instanceof LogicalExpr l) {
            collect(l.left(), out);
            if (l.right() != null) collect(l.right(), out);
            return;
        }
        if (e instanceof FuncCall f) {
            for (Expr a : f.args()) collect(a, out);
            return;
        }
        if (e instanceof Literal) {
            return;
        }
        // default: no refs
    }

    /** Splits a predicate into AND-conjuncts. */
    public static List<Expr> splitConjuncts(Expr pred) {
        List<Expr> out = new ArrayList<>();
        splitAnd(pred, out);
        return out;
    }

    private static void splitAnd(Expr e, List<Expr> out) {
        if (e instanceof LogicalExpr l && l.op() == LogicalExpr.Op.AND) {
            splitAnd(l.left(), out);
            splitAnd(l.right(), out);
        } else {
            out.add(e);
        }
    }

    /** Combines a list of conjuncts using AND. If empty, returns a TRUE literal. */
    public static Expr andAll(List<Expr> conjuncts) {
        if (conjuncts.isEmpty()) return new Literal(new SourcePos(1,1), Boolean.TRUE);
        Expr cur = conjuncts.get(0);
        for (int i = 1; i < conjuncts.size(); i++) {
            cur = new LogicalExpr(cur.pos(), LogicalExpr.Op.AND, cur, conjuncts.get(i));
        }
        return cur;
    }

    /** Returns true if schema contains all columns referenced by the set. */
    public static boolean schemaContainsAll(Schema schema, Set<ColumnRef> refs) {
        for (ColumnRef c : refs) {
            if (!schemaContains(schema, c)) return false;
        }
        return true;
    }

    /** Returns true if schema contains a column referenced by the ColumnRef. */
    public static boolean schemaContains(Schema schema, ColumnRef ref) {
        String table = ref.table();
        String col = ref.column();
        String wantQualified = (table == null ? null : (table + "." + col).toLowerCase(Locale.ROOT));
        String wantSuffix = "." + col.toLowerCase(Locale.ROOT);
        String wantCol = col.toLowerCase(Locale.ROOT);
        for (ColumnMeta cm : schema.columns()) {
            String name = cm.name().toLowerCase(Locale.ROOT);
            if (wantQualified != null && name.equals(wantQualified)) return true;
            if (wantQualified != null && name.endsWith(wantSuffix)) return true; // table may differ but suffix matches
            if (name.equals(wantCol)) return true; // unqualified match
        }
        return false;
    }
}
