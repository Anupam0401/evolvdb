package io.github.anupam.evolvdb.planner.analyzer;

import io.github.anupam.evolvdb.catalog.CatalogManager;
import io.github.anupam.evolvdb.catalog.TableMeta;
import io.github.anupam.evolvdb.planner.logical.*;
import io.github.anupam.evolvdb.sql.ast.*;
import io.github.anupam.evolvdb.types.ColumnMeta;
import io.github.anupam.evolvdb.types.Schema;
import io.github.anupam.evolvdb.types.Type;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Binds SQL AST to a logical plan using the catalog for name/type resolution. */
public final class Binder {

    public LogicalPlan bind(Statement stmt, CatalogManager catalog) {
        Objects.requireNonNull(stmt, "stmt");
        Objects.requireNonNull(catalog, "catalog");
        if (stmt instanceof Select sel) return bindSelect(sel, catalog);
        if (stmt instanceof Insert ins) return bindInsert(ins, catalog);
        if (stmt instanceof CreateTable || stmt instanceof DropTable) {
            // DDL: planner is not responsible for execution here; return a no-op logical plan later if needed
            throw new UnsupportedOperationException("DDL binding not implemented in planner");
        }
        throw new IllegalArgumentException("Unsupported statement type: " + stmt.getClass().getSimpleName());
    }

    private LogicalPlan bindSelect(Select sel, CatalogManager catalog) {
        // Build binding environment for all FROM tables
        BindingEnv env = new BindingEnv();
        for (TableRef tr : sel.froms()) {
            TableMeta tm = catalog.getTable(tr.tableName())
                    .orElseThrow(() -> err(tr.pos(), "Unknown table: " + tr.tableName()));
            env.add(tm.name(), tr.alias(), tm.schema(), tr.pos());
        }

        // Build left-deep join plan
        List<TableBinding> tabs = env.tables;
        LogicalPlan plan = new LogicalScan(tabs.get(0).name, tabs.get(0).alias, tabs.get(0).schema);
        Set<String> leftQuals = new LinkedHashSet<>();
        leftQuals.add(tabs.get(0).qual());

        // Split WHERE into conjuncts
        List<Expr> predicates = new ArrayList<>();
        if (sel.where() != null) {
            // Validate columns / ambiguity in WHERE
            validateExpr(sel.where(), env);
            predicates.addAll(splitConjuncts(sel.where()));
        }

        for (int i = 1; i < tabs.size(); i++) {
            TableBinding right = tabs.get(i);
            LogicalPlan rightScan = new LogicalScan(right.name, right.alias, right.schema);

            // Extract join predicates referencing left and right only
            List<Expr> joinConds = new ArrayList<>();
            List<Expr> remain = new ArrayList<>();
            for (Expr e : predicates) {
                Set<String> q = referencedQuals(e, env);
                boolean touchesLeft = q.stream().anyMatch(leftQuals::contains);
                boolean touchesRight = q.contains(right.qual());
                boolean onlyLeftRight = q.stream().allMatch(x -> leftQuals.contains(x) || x.equals(right.qual()));
                if (touchesLeft && touchesRight && onlyLeftRight) joinConds.add(e);
                else remain.add(e);
            }
            predicates = remain;
            Expr joinCond = combineConjuncts(joinConds);

            // Build join schema as qualified names to avoid duplicates
            Schema joinSchema = concatQualifiedSchemas(leftQuals, tabs.get(0), plan.schema(), right);
            plan = new LogicalJoin(plan, rightScan, LogicalJoin.JoinType.INNER, joinCond, joinSchema);
            leftQuals.add(right.qual());
        }

        // Remaining filters
        if (!predicates.isEmpty()) {
            plan = new LogicalFilter(plan, combineConjuncts(predicates));
        }

        // Determine if aggregation is present
        boolean hasAgg = hasAggregate(sel.items());
        boolean hasGroup = sel.groupBy() != null && !sel.groupBy().isEmpty();

        if (hasAgg || hasGroup) {
            // Validate group-by expressions and collect their referenced columns
            List<Expr> groups = sel.groupBy();
            for (Expr g : groups) validateExpr(g, env);
            Set<String> groupCols = new HashSet<>();
            for (Expr g : groups) groupCols.addAll(referencedColumns(g, env));

            // Build outputs
            List<ProjectItem> outs = new ArrayList<>();
            List<ColumnMeta> outCols = new ArrayList<>();
            int idx = 0;
            Set<String> usedNames = new HashSet<>();
            for (SelectItem it : sel.items()) {
                if (it.isStar()) throw err(it.pos(), "SELECT * not allowed with GROUP BY");
                Expr expr = it.expr();
                String outName = it.alias();
                if (outName == null) {
                    if (expr instanceof ColumnRef cr) outName = cr.column();
                    else if (expr instanceof FuncCall fc) outName = fc.name().toLowerCase() + (fc.starArg() ? "_*" : "");
                    else outName = "expr" + (++idx);
                }
                outName = uniquify(outName, expr, usedNames, env);
                ColumnMeta cm;
                if (expr instanceof FuncCall fc) {
                    cm = inferAggOutputColumn(fc, outName, env);
                } else {
                    // Non-aggregate expr must be group-invariant: all columns ⊆ groupCols
                    Set<String> used = referencedColumns(expr, env);
                    if (!groupCols.containsAll(used)) throw err(expr.pos(), "Non-aggregated columns must appear in GROUP BY");
                    cm = inferOutputColumn(expr, outName, env);
                }
                outs.add(new ProjectItem(expr, outName));
                outCols.add(cm);
            }
            Schema outSchema = new Schema(outCols);
            return new LogicalAggregate(plan, sel.groupBy(), outs, outSchema);
        }

        // Non-aggregate: projection
        if (sel.items().size() == 1 && sel.items().get(0).isStar()) {
            // SELECT *
            if (tabs.size() == 1) {
                // single-table: keep as Scan/Filter without a Project
                return plan;
            }
            // multi-table: build a Project with qualified names
            List<ProjectItem> items = new ArrayList<>();
            List<ColumnMeta> outCols = new ArrayList<>();
            for (TableBinding tb : tabs) {
                for (ColumnMeta cm : tb.schema.columns()) {
                    Expr e = new ColumnRef(sel.pos(), tb.preferredQualifier(), cm.name());
                    String name = tb.preferredQualifier() + "." + cm.name();
                    items.add(new ProjectItem(e, name));
                    outCols.add(new ColumnMeta(name, cm.type(), cm.length()));
                }
            }
            Schema outSchema = new Schema(outCols);
            return new LogicalProject(plan, items, outSchema);
        }

        List<ProjectItem> items = new ArrayList<>();
        List<ColumnMeta> outCols = new ArrayList<>();
        int idx = 0;
        Set<String> usedNames = new HashSet<>();
        for (SelectItem it : sel.items()) {
            if (it.isStar()) throw err(it.pos(), "SELECT * with other items not supported");
            Expr expr = it.expr();
            // validate expr resolves
            validateExpr(expr, env);
            String outName = it.alias();
            if (outName == null) {
                if (expr instanceof ColumnRef cr) outName = cr.column();
                else outName = "expr" + (++idx);
            }
            outName = uniquify(outName, expr, usedNames, env);
            ColumnMeta cm = inferOutputColumn(expr, outName, env);
            items.add(new ProjectItem(expr, outName));
            outCols.add(cm);
        }
        Schema outSchema = new Schema(outCols);
        return new LogicalProject(plan, items, outSchema);
    }

    private LogicalPlan bindInsert(Insert ins, CatalogManager catalog) {
        TableMeta tm = catalog.getTable(ins.tableName())
                .orElseThrow(() -> err(ins.pos(), "Unknown table: " + ins.tableName()));
        Schema schema = tm.schema();

        List<ColumnMeta> targetCols = new ArrayList<>();
        if (ins.columns().isEmpty()) {
            targetCols.addAll(schema.columns());
        } else {
            for (String name : ins.columns()) {
                ColumnMeta cm = findColumn(schema, name, ins.pos());
                targetCols.add(cm);
            }
        }
        return new LogicalInsert(tm.name(), targetCols, ins.rows(), schema);
    }

    private ColumnMeta inferOutputColumn(Expr expr, String name, BindingEnv env) {
        Type t;
        Integer len = null;
        if (expr instanceof ColumnRef cr) {
            ColumnMeta cm = resolveColumn(cr, env);
            t = cm.type();
            len = cm.length();
        } else if (expr instanceof Literal lit) {
            Object v = lit.value();
            if (v instanceof Integer) t = Type.INT;
            else if (v instanceof Long) t = Type.BIGINT;
            else if (v instanceof Boolean) t = Type.BOOLEAN;
            else if (v instanceof String) t = Type.STRING;
            else throw err(expr.pos(), "Unsupported literal type: " + v.getClass().getSimpleName());
        } else if (expr instanceof BinaryExpr be) {
            Type lt = inferOutputColumn(be.left(), name, env).type();
            Type rt = inferOutputColumn(be.right(), name, env).type();
            if (lt == Type.FLOAT || rt == Type.FLOAT) t = Type.FLOAT;
            else if (lt == Type.BIGINT || rt == Type.BIGINT) t = Type.BIGINT;
            else if (lt == Type.INT && rt == Type.INT) t = Type.INT;
            else throw err(expr.pos(), "Unsupported binary operand types: " + lt + ", " + rt);
        } else if (expr instanceof ComparisonExpr) {
            t = Type.BOOLEAN;
        } else if (expr instanceof LogicalExpr) {
            t = Type.BOOLEAN;
        } else {
            throw err(expr.pos(), "Unsupported expression in projection: " + expr.getClass().getSimpleName());
        }
        return new ColumnMeta(name, t, len);
    }

    private ColumnMeta inferAggOutputColumn(FuncCall fc, String name, BindingEnv env) {
        String fn = fc.name().toUpperCase();
        if (fn.equals("COUNT")) {
            // COUNT(*|expr) -> BIGINT
            return new ColumnMeta(name, Type.BIGINT, null);
        }
        if (fc.args().size() != 1 || fc.starArg()) throw err(fc.pos(), "Aggregate requires one argument");
        ColumnMeta arg = inferOutputColumn(fc.args().get(0), name, env);
        return switch (fn) {
            case "SUM" -> new ColumnMeta(name, (arg.type() == Type.FLOAT) ? Type.FLOAT : Type.BIGINT, null);
            case "AVG" -> new ColumnMeta(name, Type.FLOAT, null);
            case "MIN" -> new ColumnMeta(name, arg.type(), arg.length());
            case "MAX" -> new ColumnMeta(name, arg.type(), arg.length());
            default -> throw err(fc.pos(), "Unknown aggregate function: " + fc.name());
        };
    }

    private ColumnMeta resolveColumn(String name, BindingEnv env, SourcePos pos) {
        ResolvedColumn rc = env.resolveUnqualified(name, pos);
        return rc.col;
    }

    private ColumnMeta resolveColumn(ColumnRef cr, BindingEnv env) {
        if (cr.table() != null) {
            ResolvedColumn rc = env.resolveQualified(cr.table(), cr.column(), cr.pos());
            return rc.col;
        } else {
            return resolveColumn(cr.column(), env, cr.pos());
        }
    }

    private static ColumnMeta findColumn(Schema schema, String name, SourcePos pos) {
        for (ColumnMeta cm : schema.columns())
            if (cm.name().equalsIgnoreCase(name)) return cm;
        throw err(pos, "Unknown column: " + name);
    }

    private void validateExpr(Expr expr, BindingEnv env) {
        if (expr instanceof ColumnRef cr) {
            resolveColumn(cr, env); // throws if not resolvable (unknown or ambiguous)
        } else if (expr instanceof BinaryExpr be) {
            validateExpr(be.left(), env); validateExpr(be.right(), env);
        } else if (expr instanceof LogicalExpr le) {
            validateExpr(le.left(), env); if (le.right() != null) validateExpr(le.right(), env);
        } else if (expr instanceof ComparisonExpr ce) {
            validateExpr(ce.left(), env); validateExpr(ce.right(), env);
        } else if (expr instanceof FuncCall fc) {
            // validate args
            for (Expr a : fc.args()) validateExpr(a, env);
        }
        // literals are fine
    }

    // --- Helpers for multi-table binding ---
    private static final class TableBinding {
        final String name; final String alias; final Schema schema; final SourcePos pos;
        TableBinding(String name, String alias, Schema schema, SourcePos pos) {
            this.name = name; this.alias = alias; this.schema = schema; this.pos = pos;
        }
        String qual() { return (alias != null ? alias : name).toLowerCase(); }
        String preferredQualifier() { return alias != null ? alias : name; }
    }

    private static final class ResolvedColumn { final TableBinding tbl; final ColumnMeta col; ResolvedColumn(TableBinding t, ColumnMeta c) { this.tbl = t; this.col = c; } }

    private static final class BindingEnv {
        final List<TableBinding> tables = new ArrayList<>();
        final Map<String, TableBinding> byQual = new HashMap<>(); // lower-case name/alias

        void add(String name, String alias, Schema schema, SourcePos pos) {
            TableBinding tb = new TableBinding(name, alias, schema, pos);
            String q = tb.qual();
            if (byQual.containsKey(q)) throw new IllegalArgumentException("Duplicate table alias/name: " + (alias != null ? alias : name) + " at " + pos.line() + ":" + pos.column());
            tables.add(tb);
            byQual.put(q, tb);
        }

        ResolvedColumn resolveQualified(String table, String col, SourcePos pos) {
            TableBinding tb = byQual.get(table.toLowerCase());
            if (tb == null) throw new IllegalArgumentException("Unknown table qualifier: " + table + " at " + pos.line() + ":" + pos.column());
            for (ColumnMeta cm : tb.schema.columns()) if (cm.name().equalsIgnoreCase(col)) return new ResolvedColumn(tb, cm);
            throw new IllegalArgumentException("Unknown column: " + col + " at " + pos.line() + ":" + pos.column());
        }

        ResolvedColumn resolveUnqualified(String col, SourcePos pos) {
            ResolvedColumn found = null;
            for (TableBinding tb : tables) {
                for (ColumnMeta cm : tb.schema.columns()) {
                    if (cm.name().equalsIgnoreCase(col)) {
                        if (found != null) throw new IllegalArgumentException("Ambiguous column: " + col + " at " + pos.line() + ":" + pos.column());
                        found = new ResolvedColumn(tb, cm);
                    }
                }
            }
            if (found == null) throw new IllegalArgumentException("Unknown column: " + col + " at " + pos.line() + ":" + pos.column());
            return found;
        }
    }

    private Set<String> referencedQuals(Expr expr, BindingEnv env) {
        Set<String> s = new HashSet<>();
        collectQuals(expr, env, s);
        return s;
    }
    private Set<String> referencedColumns(Expr expr, BindingEnv env) {
        Set<String> s = new HashSet<>();
        collectCols(expr, env, s);
        return s;
    }
    private void collectQuals(Expr e, BindingEnv env, Set<String> out) {
        if (e instanceof ColumnRef cr) {
            if (cr.table() != null) out.add(cr.table().toLowerCase());
            else {
                ResolvedColumn rc = env.resolveUnqualified(cr.column(), e.pos());
                out.add(rc.tbl.qual());
            }
        } else if (e instanceof BinaryExpr be) {
            collectQuals(be.left(), env, out); collectQuals(be.right(), env, out);
        } else if (e instanceof LogicalExpr le) {
            collectQuals(le.left(), env, out); if (le.right() != null) collectQuals(le.right(), env, out);
        } else if (e instanceof ComparisonExpr ce) {
            collectQuals(ce.left(), env, out); collectQuals(ce.right(), env, out);
        } else if (e instanceof FuncCall fc) {
            for (Expr a : fc.args()) collectQuals(a, env, out);
        }
    }
    private void collectCols(Expr e, BindingEnv env, Set<String> out) {
        if (e instanceof ColumnRef cr) {
            if (cr.table() != null) out.add(cr.table().toLowerCase() + "." + cr.column().toLowerCase());
            else {
                ResolvedColumn rc = env.resolveUnqualified(cr.column(), e.pos());
                out.add(rc.tbl.qual() + "." + rc.col.name().toLowerCase());
            }
        } else if (e instanceof BinaryExpr be) {
            collectCols(be.left(), env, out); collectCols(be.right(), env, out);
        } else if (e instanceof LogicalExpr le) {
            collectCols(le.left(), env, out); if (le.right() != null) collectCols(le.right(), env, out);
        } else if (e instanceof ComparisonExpr ce) {
            collectCols(ce.left(), env, out); collectCols(ce.right(), env, out);
        } else if (e instanceof FuncCall fc) {
            for (Expr a : fc.args()) collectCols(a, env, out);
        }
    }

    private List<Expr> splitConjuncts(Expr e) {
        List<Expr> out = new ArrayList<>();
        if (e instanceof LogicalExpr le && le.op() == LogicalExpr.Op.AND) {
            out.addAll(splitConjuncts(le.left()));
            if (le.right() != null) out.addAll(splitConjuncts(le.right()));
        } else {
            out.add(e);
        }
        return out;
    }
    private Expr combineConjuncts(List<Expr> list) {
        if (list.isEmpty()) return null;
        Expr cur = list.get(0);
        for (int i = 1; i < list.size(); i++) {
            cur = new LogicalExpr(cur.pos(), LogicalExpr.Op.AND, cur, list.get(i));
        }
        return cur;
    }

    private boolean hasAggregate(List<SelectItem> items) {
        for (SelectItem it : items) if (!it.isStar() && containsAggregate(it.expr())) return true;
        return false;
    }
    private boolean containsAggregate(Expr e) {
        if (e instanceof FuncCall fc) {
            String fn = fc.name().toUpperCase();
            return fn.equals("COUNT") || fn.equals("SUM") || fn.equals("AVG") || fn.equals("MIN") || fn.equals("MAX");
        } else if (e instanceof BinaryExpr be) {
            return containsAggregate(be.left()) || containsAggregate(be.right());
        } else if (e instanceof LogicalExpr le) {
            return containsAggregate(le.left()) || (le.right() != null && containsAggregate(le.right()));
        } else if (e instanceof ComparisonExpr ce) {
            return containsAggregate(ce.left()) || containsAggregate(ce.right());
        }
        return false;
    }

    private Schema concatQualifiedSchemas(Set<String> leftQuals, TableBinding leftHead, Schema leftSchema, TableBinding right) {
        List<ColumnMeta> cols = new ArrayList<>();
        // We don't have mapping of quals to schema in left plan here; conservatively qualify by known leftQuals order is not preserved; use left schema as-is
        // Assume left schema already unique; just carry over
        cols.addAll(leftSchema.columns());
        for (ColumnMeta cm : right.schema.columns()) {
            String n = right.preferredQualifier() + "." + cm.name();
            cols.add(new ColumnMeta(n, cm.type(), cm.length()));
        }
        return new Schema(cols);
    }

    private static IllegalArgumentException err(SourcePos pos, String msg) {
        return new IllegalArgumentException(msg + " at " + pos.line() + ":" + pos.column());
    }

    private String uniquify(String base, Expr expr, Set<String> used, BindingEnv env) {
        String name = base;
        if (!used.add(name.toLowerCase())) {
            if (expr instanceof ColumnRef cr) {
                ResolvedColumn rc = (cr.table() != null)
                        ? env.resolveQualified(cr.table(), cr.column(), expr.pos())
                        : env.resolveUnqualified(cr.column(), expr.pos());
                name = rc.tbl.preferredQualifier() + "." + rc.col.name();
                int i = 1;
                while (!used.add(name.toLowerCase())) name = name + "_" + (i++);
            } else {
                int i = 1;
                while (!used.add((base + "_" + i).toLowerCase())) i++;
                name = base + "_" + i;
            }
        }
        return name;
    }
}
