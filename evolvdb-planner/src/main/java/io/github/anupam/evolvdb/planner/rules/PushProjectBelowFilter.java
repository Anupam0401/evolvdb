package io.github.anupam.evolvdb.planner.rules;

import io.github.anupam.evolvdb.planner.logical.*;
import io.github.anupam.evolvdb.sql.ast.*;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * If Project(Filter(child)) and all project items are ColumnRef, and the filter predicate
 * only references columns preserved by project, push Project below Filter: Filter(Project(child)).
 */
public final class PushProjectBelowFilter implements Rule {
    @Override
    public boolean matches(LogicalPlan plan) {
        if (!(plan instanceof LogicalProject p)) return false;
        if (!(p.child() instanceof LogicalFilter f)) return false;
        if (!allItemsAreColumnRefs(p.items())) return false;
        Set<String> projected = projectedColumns(p.items());
        Set<String> used = referencedColumns(f.predicate());
        return projected.containsAll(used);
    }

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        LogicalProject p = (LogicalProject) plan;
        LogicalFilter f = (LogicalFilter) p.child();
        // Transform: Project(Filter(child)) -> Filter(Project(child))
        LogicalPlan newProject = new LogicalProject(f.child(), p.items(), p.schema());
        return new LogicalFilter(newProject, f.predicate());
    }

    private static boolean allItemsAreColumnRefs(List<ProjectItem> items) {
        for (ProjectItem it : items) if (!(it.expr() instanceof ColumnRef)) return false;
        return true;
    }

    private static Set<String> projectedColumns(List<ProjectItem> items) {
        Set<String> s = new HashSet<>();
        for (ProjectItem it : items) {
            ColumnRef cr = (ColumnRef) it.expr();
            s.add(cr.column().toLowerCase());
        }
        return s;
    }

    private static Set<String> referencedColumns(Expr expr) {
        HashSet<String> s = new HashSet<>();
        collect(expr, s);
        return s;
    }

    private static void collect(Expr e, Set<String> out) {
        if (e instanceof ColumnRef cr) {
            out.add(cr.column().toLowerCase());
        } else if (e instanceof BinaryExpr be) {
            collect(be.left(), out); collect(be.right(), out);
        } else if (e instanceof LogicalExpr le) {
            collect(le.left(), out); if (le.right() != null) collect(le.right(), out);
        } else if (e instanceof ComparisonExpr ce) {
            collect(ce.left(), out); collect(ce.right(), out);
        }
    }
}
