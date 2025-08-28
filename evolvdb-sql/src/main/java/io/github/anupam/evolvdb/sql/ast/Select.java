package io.github.anupam.evolvdb.sql.ast;

import java.util.List;
import java.util.Objects;

/** SELECT selectItems FROM tableRef [WHERE expr] */
public final class Select extends Statement {
    private final List<SelectItem> items;
    private final TableRef from;
    private final Expr where; // may be null

    public Select(SourcePos pos, List<SelectItem> items, TableRef from, Expr where) {
        super(pos);
        this.items = List.copyOf(Objects.requireNonNull(items, "items"));
        this.from = Objects.requireNonNull(from, "from");
        this.where = where;
    }

    public List<SelectItem> items() { return items; }
    public TableRef from() { return from; }
    public Expr where() { return where; }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitSelect(this, context);
    }
}
