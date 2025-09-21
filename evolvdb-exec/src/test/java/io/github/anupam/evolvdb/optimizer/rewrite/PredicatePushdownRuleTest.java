package io.github.anupam.evolvdb.optimizer.rewrite;

import io.github.anupam.evolvdb.planner.logical.*;
import io.github.anupam.evolvdb.sql.ast.*;
import io.github.anupam.evolvdb.types.ColumnMeta;
import io.github.anupam.evolvdb.types.Schema;
import io.github.anupam.evolvdb.types.Type;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class PredicatePushdownRuleTest {
    private Schema users() { return new Schema(List.of(new ColumnMeta("id", Type.INT, null), new ColumnMeta("name", Type.STRING, null))); }
    private Schema orders() { return new Schema(List.of(new ColumnMeta("order_id", Type.INT, null), new ColumnMeta("user_id", Type.INT, null), new ColumnMeta("amount", Type.INT, null))); }

    @Test
    void pushes_left_only_predicate_into_left_child() {
        LogicalPlan u = new LogicalScan("users", "u", users());
        LogicalPlan o = new LogicalScan("orders", "o", orders());
        Expr join = new ComparisonExpr(new SourcePos(1,1), ComparisonExpr.Op.EQ,
                new ColumnRef(new SourcePos(1,1), "u", "id"), new ColumnRef(new SourcePos(1,1), "o", "user_id"));
        LogicalPlan j = new LogicalJoin(u, o, LogicalJoin.JoinType.INNER, join, new Schema(List.of(new ColumnMeta("u.id", Type.INT, null))));

        // Filter only on left side: u.name = 'Alice'
        Expr pred = new ComparisonExpr(new SourcePos(1,1), ComparisonExpr.Op.EQ,
                new ColumnRef(new SourcePos(1,1), "u", "name"), new Literal(new SourcePos(1,1), "Alice"));
        LogicalPlan root = new LogicalFilter(j, pred);

        LogicalPlan rewritten = new LogicalRewriter().rewrite(root);
        LogicalJoin rj = findJoin(rewritten);
        assertNotNull(rj, "Join should exist after rewrite");
        assertTrue(containsFilter(rj.left()), "Left side should contain a filter after pushdown");
        // Right side should remain a plain scan or project; must not have a filter in this scenario
        assertFalse(containsFilter(rj.right()), "Right side should not contain a filter for left-only predicate");
    }

    @Test
    void retains_mixed_predicate_above_join() {
        LogicalPlan u = new LogicalScan("users", "u", users());
        LogicalPlan o = new LogicalScan("orders", "o", orders());
        Expr join = new ComparisonExpr(new SourcePos(1,1), ComparisonExpr.Op.EQ,
                new ColumnRef(new SourcePos(1,1), "u", "id"), new ColumnRef(new SourcePos(1,1), "o", "user_id"));
        LogicalPlan j = new LogicalJoin(u, o, LogicalJoin.JoinType.INNER, join, new Schema(List.of(new ColumnMeta("u.id", Type.INT, null))));

        // Predicate references both sides: u.name = 'Alice' AND o.amount > 10
        Expr left = new ComparisonExpr(new SourcePos(1,1), ComparisonExpr.Op.EQ,
                new ColumnRef(new SourcePos(1,1), "u", "name"), new Literal(new SourcePos(1,1), "Alice"));
        Expr right = new ComparisonExpr(new SourcePos(1,1), ComparisonExpr.Op.GT,
                new ColumnRef(new SourcePos(1,1), "o", "amount"), new Literal(new SourcePos(1,1), 10));
        Expr both = new LogicalExpr(new SourcePos(1,1), LogicalExpr.Op.AND, left, right);
        LogicalPlan root = new LogicalFilter(j, both);

        LogicalPlan rewritten = new LogicalRewriter().rewrite(root);
        LogicalJoin rj = findJoin(rewritten);
        assertNotNull(rj, "Join should exist after rewrite");
        assertTrue(containsFilter(rj.left()), "Left conjunct pushed to left side");
        assertTrue(containsFilter(rj.right()), "Right conjunct pushed to right side");
    }

    private static LogicalJoin findJoin(LogicalPlan p) {
        if (p instanceof LogicalJoin j) return j;
        for (LogicalPlan c : p.children()) {
            LogicalJoin r = findJoin(c);
            if (r != null) return r;
        }
        return null;
    }

    private static boolean containsFilter(LogicalPlan p) {
        if (p instanceof LogicalFilter) return true;
        for (LogicalPlan c : p.children()) if (containsFilter(c)) return true;
        return false;
    }
}
