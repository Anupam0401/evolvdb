package io.github.anupam.evolvdb.sql.ast;

/** DELETE FROM tableName [WHERE expr] */
public final class Delete extends Statement {
    private final String tableName;
    private final Expr where;

    public Delete(SourcePos pos, String tableName, Expr where) {
        super(pos);
        if (tableName == null || tableName.isBlank())
            throw new IllegalArgumentException("tableName");
        this.tableName = tableName;
        this.where = where;
    }

    public String tableName() {
        return tableName;
    }

    public Expr where() {
        return where;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDelete(this, context);
    }
}
