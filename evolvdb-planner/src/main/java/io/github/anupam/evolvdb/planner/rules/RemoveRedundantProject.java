package io.github.anupam.evolvdb.planner.rules;

import io.github.anupam.evolvdb.planner.logical.*;
import io.github.anupam.evolvdb.sql.ast.ColumnRef;

/**
 * Remove Project when it is an identity over its child schema: items are ColumnRef of all columns in order,
 * and names equal those columns.
 */
public final class RemoveRedundantProject implements Rule {
    @Override
    public boolean matches(LogicalPlan plan) {
        if (!(plan instanceof LogicalProject p)) return false;
        var childSchema = p.child().schema();
        if (p.items().size() != childSchema.size()) return false;
        for (int i = 0; i < p.items().size(); i++) {
            var it = p.items().get(i);
            if (!(it.expr() instanceof ColumnRef cr)) return false;
            var col = childSchema.columns().get(i);
            if (!col.name().equalsIgnoreCase(cr.column())) return false;
            if (!col.name().equalsIgnoreCase(it.name())) return false;
        }
        return true;
    }

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        return ((LogicalProject) plan).child();
    }
}
