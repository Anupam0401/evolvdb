package io.github.anupam.evolvdb.planner.rules;

import io.github.anupam.evolvdb.planner.logical.LogicalFilter;
import io.github.anupam.evolvdb.planner.logical.LogicalPlan;
import io.github.anupam.evolvdb.sql.ast.Expr;
import io.github.anupam.evolvdb.sql.ast.LogicalExpr;

/** Simple predicate simplifications: NOT(NOT x) => x. */
public final class PredicateSimplification implements Rule {
    @Override
    public boolean matches(LogicalPlan plan) {
        if (!(plan instanceof LogicalFilter f)) return false;
        return containsDoubleNegation(f.predicate());
    }

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        LogicalFilter f = (LogicalFilter) plan;
        Expr simplified = simplify(f.predicate());
        if (simplified == f.predicate()) return plan;
        return new LogicalFilter(f.child(), simplified);
    }

    private static boolean containsDoubleNegation(Expr e) {
        if (e instanceof LogicalExpr le && le.op() == LogicalExpr.Op.NOT) {
            return le.left() instanceof LogicalExpr ll && ((LogicalExpr) le.left()).op() == LogicalExpr.Op.NOT;
        }
        if (e instanceof LogicalExpr le) {
            return containsDoubleNegation(le.left()) || (le.right() != null && containsDoubleNegation(le.right()));
        }
        return false;
    }

    private static Expr simplify(Expr e) {
        if (e instanceof LogicalExpr le) {
            if (le.op() == LogicalExpr.Op.NOT && le.left() instanceof LogicalExpr ll && ll.op() == LogicalExpr.Op.NOT) {
                return simplify(ll.left());
            }
            Expr left = simplify(le.left());
            Expr right = le.right() != null ? simplify(le.right()) : null;
            if (left == le.left() && right == le.right()) return e;
            return new LogicalExpr(le.pos(), le.op(), left, right);
        }
        return e;
    }
}
