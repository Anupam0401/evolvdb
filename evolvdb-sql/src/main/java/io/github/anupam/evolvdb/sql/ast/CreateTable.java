package io.github.anupam.evolvdb.sql.ast;

import java.util.List;
import java.util.Objects;

/** CREATE TABLE tableName (columns...) */
public final class CreateTable extends Statement {
    private final String tableName;
    private final List<ColumnDef> columns;

    public CreateTable(SourcePos pos, String tableName, List<ColumnDef> columns) {
        super(pos);
        if (tableName == null || tableName.isBlank()) throw new IllegalArgumentException("tableName");
        this.tableName = tableName;
        this.columns = List.copyOf(Objects.requireNonNull(columns, "columns"));
    }

    public String tableName() { return tableName; }
    public List<ColumnDef> columns() { return columns; }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateTable(this, context);
    }
}
