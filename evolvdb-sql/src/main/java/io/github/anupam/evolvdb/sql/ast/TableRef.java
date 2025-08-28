package io.github.anupam.evolvdb.sql.ast;

/** FROM tableName [AS alias] */
public final class TableRef extends AstNode {
    private final String tableName;
    private final String alias; // nullable

    public TableRef(SourcePos pos, String tableName, String alias) {
        super(pos);
        if (tableName == null || tableName.isBlank()) throw new IllegalArgumentException("tableName");
        this.tableName = tableName;
        this.alias = (alias != null && !alias.isBlank()) ? alias : null;
    }

    public String tableName() { return tableName; }
    public String alias() { return alias; }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitTableRef(this, context);
    }
}
