package io.github.anupam.evolvdb.optimizer.memo;

import io.github.anupam.evolvdb.planner.logical.LogicalPlan;
import io.github.anupam.evolvdb.planner.logical.LogicalJoin;
import io.github.anupam.evolvdb.sql.ast.ColumnRef;
import io.github.anupam.evolvdb.sql.ast.ComparisonExpr;
import io.github.anupam.evolvdb.sql.ast.Expr;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;

/** Minimal memo that interns LogicalPlan nodes by identity. */
public final class Memo {
    private final Map<LogicalPlan, Group> groups = new IdentityHashMap<>();

    public Group intern(LogicalPlan logical) {
        Group g = groups.get(logical);
        if (g != null) return g;
        g = new Group();
        groups.put(logical, g);
        // Build child groups in stable child order
        List<Group> childGroups = new ArrayList<>();
        for (LogicalPlan c : logical.children()) {
            childGroups.add(intern(c));
        }
        g.addExpr(new GroupExpr(logical, childGroups));

        // For INNER joins, also add a commuted equivalent expression to support more alternatives
        if (logical instanceof LogicalJoin j && j.type() == LogicalJoin.JoinType.INNER) {
            // Swap condition sides if it is an equi-join on two column refs
            Expr cond = j.condition();
            Expr swapped = cond;
            if (cond instanceof ComparisonExpr ce && ce.op() == ComparisonExpr.Op.EQ &&
                    ce.left() instanceof ColumnRef && ce.right() instanceof ColumnRef) {
                swapped = new ComparisonExpr(ce.pos(), ce.op(), ce.right(), ce.left());
            }
            LogicalPlan alt = new LogicalJoin(j.right(), j.left(), j.type(), swapped, j.schema());
            // Child groups in swapped order
            List<Group> swappedChildren = new ArrayList<>();
            swappedChildren.add(childGroups.get(1));
            swappedChildren.add(childGroups.get(0));
            g.addExpr(new GroupExpr(alt, swappedChildren));
        }
        return g;
    }
}
