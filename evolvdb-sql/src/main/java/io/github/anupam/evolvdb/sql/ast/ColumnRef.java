package io.github.anupam.evolvdb.sql.ast;

/** Column reference [table.]column */
public final class ColumnRef extends Expr {
    private final String table; // may be null
    private final String column;

    public ColumnRef(SourcePos pos, String table, String column) {
        super(pos);
        if (column == null || column.isBlank()) throw new IllegalArgumentException("column");
        this.table = (table != null && !table.isBlank()) ? table : null;
        this.column = column;
    }

    public String table() { return table; }
    public String column() { return column; }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitColumnRef(this, context);
    }
}
