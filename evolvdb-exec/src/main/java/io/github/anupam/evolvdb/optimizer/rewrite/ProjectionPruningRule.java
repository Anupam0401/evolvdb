package io.github.anupam.evolvdb.optimizer.rewrite;

import io.github.anupam.evolvdb.planner.logical.*;
import io.github.anupam.evolvdb.sql.ast.ColumnRef;
import io.github.anupam.evolvdb.sql.ast.Expr;
import io.github.anupam.evolvdb.sql.ast.Literal;
import io.github.anupam.evolvdb.sql.ast.SourcePos;
import io.github.anupam.evolvdb.types.ColumnMeta;
import io.github.anupam.evolvdb.types.Schema;

import java.util.*;

/**
 * Inserts narrowing LogicalProject nodes to eliminate unused columns in subtrees.
 * Conservative: does not change existing Project schemas (avoids type inference).
 */
public final class ProjectionPruningRule implements LogicalRule {

    @Override
    public boolean matches(LogicalPlan plan) { return true; }

    @Override
    public LogicalPlan apply(LogicalPlan plan) { return prune(plan); }

    public LogicalPlan prune(LogicalPlan root) {
        Set<ColumnRef> requiredAtRoot = requiredFromRoot(root);
        return pruneRec(root, requiredAtRoot);
    }

    private Set<ColumnRef> requiredFromRoot(LogicalPlan root) {
        // If root is a project, only its expressions are required; else conservatively require all root columns
        if (root instanceof LogicalProject p) {
            Set<ColumnRef> refs = new HashSet<>();
            for (ProjectItem it : p.items()) refs.addAll(ExprUtils.collectColumnRefs(it.expr()));
            return refs;
        }
        // All columns required
        Set<ColumnRef> all = new HashSet<>();
        for (ColumnMeta cm : root.schema().columns()) {
            all.add(new ColumnRef(new SourcePos(1,1), null, cm.name()));
        }
        return all;
    }

    private LogicalPlan pruneRec(LogicalPlan node, Set<ColumnRef> requiredAbove) {
        if (node instanceof LogicalFilter f) {
            // Required columns include predicate refs
            Set<ColumnRef> here = new HashSet<>(requiredAbove);
            here.addAll(ExprUtils.collectColumnRefs(f.predicate()));
            LogicalPlan child = pruneRec(f.child(), here);
            // After pruning child, insert a narrowing project if beneficial
            LogicalPlan narrowed = maybeProject(child, here);
            return (narrowed == child) ? new LogicalFilter(child, f.predicate()) : new LogicalFilter(narrowed, f.predicate());
        }
        if (node instanceof LogicalProject p) {
            // Determine which outputs are required; compute child requirements from used expressions
            Set<String> requiredNames = new HashSet<>();
            for (ColumnRef c : requiredAbove) requiredNames.add(normalizeName(c));
            List<ProjectItem> kept = new ArrayList<>();
            Set<ColumnRef> childReq = new HashSet<>();
            for (ProjectItem it : p.items()) {
                if (requiredNames.contains(it.name().toLowerCase(Locale.ROOT))) {
                    kept.add(it);
                    childReq.addAll(ExprUtils.collectColumnRefs(it.expr()));
                }
            }
            if (kept.isEmpty()) {
                // Nothing from this project is required: produce a constant TRUE project with one column to keep schema valid
                // But to remain conservative, keep original project
                childReq = new HashSet<>();
                for (ProjectItem it : p.items()) childReq.addAll(ExprUtils.collectColumnRefs(it.expr()));
                LogicalPlan child = pruneRec(p.child(), childReq);
                return new LogicalProject(child, p.items(), p.schema());
            }
            LogicalPlan child = pruneRec(p.child(), childReq);
            return new LogicalProject(child, p.items(), p.schema()); // keep original schema to avoid type recompute
        }
        if (node instanceof LogicalAggregate a) {
            Set<ColumnRef> here = new HashSet<>();
            for (Expr g : a.groupBy()) here.addAll(ExprUtils.collectColumnRefs(g));
            for (ProjectItem it : a.aggregates()) here.addAll(ExprUtils.collectColumnRefs(it.expr()));
            here.addAll(requiredAbove); // downstream may expect outputs
            LogicalPlan child = pruneRec(a.child(), here);
            LogicalPlan narrowed = maybeProject(child, here);
            return (narrowed == child) ? new LogicalAggregate(child, a.groupBy(), a.aggregates(), a.schema())
                    : new LogicalAggregate(narrowed, a.groupBy(), a.aggregates(), a.schema());
        }
        if (node instanceof LogicalJoin j) {
            // Split requiredAbove by side; include join predicate refs
            Set<ColumnRef> leftReq = new HashSet<>();
            Set<ColumnRef> rightReq = new HashSet<>();
            for (ColumnRef c : requiredAbove) {
                if (ExprUtils.schemaContains(j.left().schema(), c)) leftReq.add(c);
                else if (ExprUtils.schemaContains(j.right().schema(), c)) rightReq.add(c);
                else { leftReq.add(c); rightReq.add(c); } // ambiguous -> keep both
            }
            if (j.condition() != null) {
                for (ColumnRef c : ExprUtils.collectColumnRefs(j.condition())) {
                    if (ExprUtils.schemaContains(j.left().schema(), c)) leftReq.add(c);
                    else if (ExprUtils.schemaContains(j.right().schema(), c)) rightReq.add(c);
                    else { leftReq.add(c); rightReq.add(c); }
                }
            }
            LogicalPlan newLeft = pruneRec(j.left(), leftReq);
            LogicalPlan newRight = pruneRec(j.right(), rightReq);
            LogicalPlan lNarrow = maybeProject(newLeft, leftReq);
            LogicalPlan rNarrow = maybeProject(newRight, rightReq);
            return new LogicalJoin(lNarrow, rNarrow, j.type(), j.condition(), j.schema());
        }
        // Scan or Insert: insert a narrowing project if possible
        return maybeProject(node, requiredAbove);
    }

    private LogicalPlan maybeProject(LogicalPlan child, Set<ColumnRef> required) {
        // Build a project keeping only columns from child's schema that appear in required set
        Schema schema = child.schema();
        List<ColumnMeta> keep = mapRequiredToColumns(required, schema);
        if (keep.size() == schema.size()) return child; // nothing to prune
        if (keep.isEmpty()) return child; // avoid empty projections (schema must be non-empty)
        // Build items referencing exact schema column names we keep
        List<ProjectItem> items = new ArrayList<>();
        for (ColumnMeta cm : keep) {
            // Use cm.name() as the column name and also as the output alias
            items.add(new ProjectItem(new ColumnRef(new SourcePos(1,1), null, cm.name()), cm.name()));
        }
        Schema out = new Schema(keep);
        return new LogicalProject(child, items, out);
    }

    private static String normalizeName(ColumnRef c) {
        return c.table() == null ? c.column().toLowerCase(Locale.ROOT)
                : (c.table() + "." + c.column()).toLowerCase(Locale.ROOT);
    }

    private List<ColumnMeta> mapRequiredToColumns(Set<ColumnRef> required, Schema schema) {
        // Collect both fully-qualified and short target names to match
        Set<String> wantsFull = new HashSet<>();
        Set<String> wantsShort = new HashSet<>();
        for (ColumnRef c : required) {
            String full = normalizeName(c);
            wantsFull.add(full);
            int dot = full.indexOf('.');
            wantsShort.add(dot > 0 ? full.substring(dot + 1) : full);
        }
        List<ColumnMeta> keep = new ArrayList<>();
        for (ColumnMeta cm : schema.columns()) {
            String name = cm.name().toLowerCase(Locale.ROOT);
            int dot = name.indexOf('.');
            String shortName = dot > 0 ? name.substring(dot + 1) : name;
            // Accept match if either side matches by full or short name
            if (wantsFull.contains(name) || wantsShort.contains(name) || wantsFull.contains(shortName) || wantsShort.contains(shortName)) {
                keep.add(cm);
            }
        }
        if (keep.isEmpty()) {
            // Fallback: ensure at least one column remains (first column)
            keep = List.of(schema.columns().get(0));
        }
        return keep;
    }
}
