package io.github.anupam.evolvdb.sql.validate;

import io.github.anupam.evolvdb.catalog.CatalogManager;
import io.github.anupam.evolvdb.catalog.TableMeta;
import io.github.anupam.evolvdb.sql.ast.AstNode;
import io.github.anupam.evolvdb.sql.ast.BinaryExpr;
import io.github.anupam.evolvdb.sql.ast.ColumnDef;
import io.github.anupam.evolvdb.sql.ast.ColumnRef;
import io.github.anupam.evolvdb.sql.ast.ComparisonExpr;
import io.github.anupam.evolvdb.sql.ast.CreateTable;
import io.github.anupam.evolvdb.sql.ast.DropTable;
import io.github.anupam.evolvdb.sql.ast.Expr;
import io.github.anupam.evolvdb.sql.ast.Insert;
import io.github.anupam.evolvdb.sql.ast.Literal;
import io.github.anupam.evolvdb.sql.ast.LogicalExpr;
import io.github.anupam.evolvdb.sql.ast.Select;
import io.github.anupam.evolvdb.sql.ast.SelectItem;
import io.github.anupam.evolvdb.sql.ast.SourcePos;
import io.github.anupam.evolvdb.types.ColumnMeta;
import io.github.anupam.evolvdb.types.Schema;
import io.github.anupam.evolvdb.types.Type;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Performs basic validation over the AST. Catalog-aware validation will be added in planner.
 */
public final class AstValidator {
    public void validate(AstNode node, CatalogManager catalog) {
        switch (node) {
            case null -> throw new IllegalArgumentException("node");
            case CreateTable ct -> validateCreateTable(ct);
            case DropTable dt -> validateDropTable(dt, catalog);
            case Insert ins -> validateInsert(ins, catalog);
            case Select sel -> validateSelect(sel, catalog);
            default -> {
            }
        }
    }

    private void validateCreateTable(CreateTable ct) {
        // no duplicate column names (case-insensitive)
        java.util.Set<String> seen = new java.util.HashSet<>();
        for (ColumnDef c : ct.columns()) {
            String lc = c.name().toLowerCase(java.util.Locale.ROOT);
            if (!seen.add(lc)) throw err(ct.pos(), "Duplicate column: " + c.name());
            // basic type/length rule similar to ColumnMeta
            if (c.type() == Type.VARCHAR) {
                if (c.length() == null || c.length() <= 0) throw err(ct.pos(), "VARCHAR requires positive length");
            } else if (c.length() != null) {
                throw err(ct.pos(), "Length not allowed for type: " + c.type());
            }
        }
    }

    private void validateDropTable(DropTable dt, CatalogManager catalog) {
        requireTable(catalog, dt.tableName(), dt.pos());
    }

    private void validateInsert(Insert ins, CatalogManager catalog) {
        TableMeta tm = requireTable(catalog, ins.tableName(), ins.pos());
        Schema schema = tm.schema();

        List<String> cols = ins.columns();
        List<ColumnMeta> targetCols = new ArrayList<>();
        if (cols.isEmpty()) {
            targetCols.addAll(schema.columns());
        } else {
            Set<String> seen = new HashSet<>();
            for (String c : cols) {
                String lc = c.toLowerCase(Locale.ROOT);
                if (!seen.add(lc)) throw err(ins.pos(), "Duplicate column in INSERT: " + c);
                ColumnMeta cm = findColumn(schema, c, ins.pos());
                targetCols.add(cm);
            }
        }

        for (List<Expr> row : ins.rows()) {
            if (row.size() != targetCols.size()) {
                throw err(ins.pos(), "INSERT values count " + row.size() + " does not match columns " + targetCols.size());
            }
            for (int i = 0; i < row.size(); i++) {
                Expr e = row.get(i);
                ColumnMeta cm = targetCols.get(i);
                validateLiteralTypeCompat(e, cm);
            }
        }
    }

    private void validateSelect(Select sel, CatalogManager catalog) {
        TableMeta tm = requireTable(catalog, sel.from().tableName(), sel.from().pos());
        Schema schema = tm.schema();
        String alias = sel.from().alias();
        Set<String> names = new java.util.HashSet<>();
        for (ColumnMeta cm : schema.columns()) names.add(cm.name().toLowerCase(Locale.ROOT));

        Consumer<Expr> checker = new Consumer<>() {
            @Override
            public void accept(Expr expr) {
                if (expr instanceof ColumnRef cr) {
                    if (cr.table() != null) {
                        String t = cr.table();
                        boolean matches = t.equalsIgnoreCase(sel.from().tableName()) || (alias != null && t.equalsIgnoreCase(alias));
                        if (!matches) throw err(cr.pos(), "Unknown table qualifier: " + t);
                    }
                    if (!names.contains(cr.column().toLowerCase(Locale.ROOT))) {
                        throw err(cr.pos(), "Unknown column: " + cr.column());
                    }
                } else if (expr instanceof BinaryExpr be) {
                    accept(be.left());
                    accept(be.right());
                } else if (expr instanceof LogicalExpr le) {
                    accept(le.left());
                    if (le.right() != null) accept(le.right());
                } else if (expr instanceof ComparisonExpr ce) {
                    accept(ce.left());
                    accept(ce.right());
                }
            }
        };

        // check select items
        for (SelectItem it : sel.items()) {
            if (!it.isStar()) checker.accept(it.expr());
        }
        if (sel.where() != null) checker.accept(sel.where());
    }

    private TableMeta requireTable(CatalogManager catalog, String name, SourcePos pos) {
        return catalog.getTable(name).orElseThrow(() -> err(pos, "Unknown table: " + name));
    }

    private ColumnMeta findColumn(Schema schema, String name, SourcePos pos) {
        for (ColumnMeta cm : schema.columns())
            if (cm.name().equalsIgnoreCase(name)) return cm;
        throw err(pos, "Unknown column: " + name);
    }

    private void validateLiteralTypeCompat(Expr expr, ColumnMeta cm) {
        if (!(expr instanceof Literal lit)) return; // non-literal: skip static check
        Object v = lit.value();
        Type t = cm.type();
        switch (t) {
            case INT -> {
                if (v instanceof Integer) {
                    // ok
                } else if (v instanceof Long l) {
                    if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE)
                        throw err(lit.pos(), cm.name() + " out of range for INT");
                } else {
                    throw err(lit.pos(), cm.name() + " expects INT literal");
                }
            }
            case BIGINT -> {
                if (!(v instanceof Integer || v instanceof Long))
                    throw err(lit.pos(), cm.name() + " expects BIGINT literal");
            }
            case BOOLEAN -> {
                if (!(v instanceof Boolean)) throw err(lit.pos(), cm.name() + " expects BOOLEAN literal");
            }
            case FLOAT -> {
                if (!(v instanceof Integer || v instanceof Long))
                    throw err(lit.pos(), cm.name() + " expects numeric literal for FLOAT");
            }
            case STRING -> {
                if (!(v instanceof String)) throw err(lit.pos(), cm.name() + " expects STRING literal");
            }
            case VARCHAR -> {
                if (!(v instanceof String s)) throw err(lit.pos(), cm.name() + " expects VARCHAR literal");
                Integer max = cm.length();
                if (max != null && s.length() > max) throw err(lit.pos(), cm.name() + " exceeds VARCHAR(" + max + ")");
            }
            default -> throw new IllegalStateException("Unsupported type: " + t);
        }
    }

    private static IllegalArgumentException err(SourcePos pos, String msg) {
        return new IllegalArgumentException(msg + " at " + pos.line() + ":" + pos.column());
    }
}
