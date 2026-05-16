package io.github.anupam.evolvdb.sql.ast;

import java.util.Map;
import java.util.Objects;

/** UPDATE tableName SET col1 = expr1, col2 = expr2, ... [WHERE expr] */
public final class Update extends Statement {
    private final String tableName;
    private final Map<String, Expr> assignments;
    private final Expr where;

    public Update(SourcePos pos, String tableName, Map<String, Expr> assignments, Expr where) {
        if (tableName == null || tableName.isBlank())
            throw new IllegalArgumentException("tableName");
        Objects.requireNonNull(assignments, "assignments");
        if (assignments.isEmpty())
            throw new IllegalArgumentException("at least one assignment required");
        super(pos);
        this.tableName = tableName;
        this.assignments = Map.copyOf(assignments);
        this.where = where;
    }

    public String tableName() {
        return tableName;
    }

    public Map<String, Expr> assignments() {
        return assignments;
    }

    public Expr where() {
        return where;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitUpdate(this, context);
    }
}
