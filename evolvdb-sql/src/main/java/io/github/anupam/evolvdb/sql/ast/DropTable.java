package io.github.anupam.evolvdb.sql.ast;

/** DROP TABLE tableName */
public final class DropTable extends Statement {
    private final String tableName;

    public DropTable(SourcePos pos, String tableName) {
        super(pos);
        if (tableName == null || tableName.isBlank()) throw new IllegalArgumentException("tableName");
        this.tableName = tableName;
    }

    public String tableName() { return tableName; }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropTable(this, context);
    }
}
