package io.github.anupam.evolvdb.sql.ast;

import java.util.List;
import java.util.Objects;

/** INSERT INTO tableName [(col1, col2,...)] VALUES (expr, ...) */
public final class Insert extends Statement {
    private final String tableName;
    private final List<String> columns; // may be empty for all columns
    private final List<List<Expr>> rows; // support multi-row VALUES

    public Insert(SourcePos pos, String tableName, List<String> columns, List<List<Expr>> rows) {
        if (tableName == null || tableName.isBlank())
            throw new IllegalArgumentException("tableName");
        Objects.requireNonNull(columns, "columns");
        Objects.requireNonNull(rows, "rows");
        super(pos);
        this.tableName = tableName;
        this.columns = List.copyOf(columns);
        this.rows = List.copyOf(rows);
    }

    public String tableName() {
        return tableName;
    }

    public List<String> columns() {
        return columns;
    }

    public List<List<Expr>> rows() {
        return rows;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitInsert(this, context);
    }
}
