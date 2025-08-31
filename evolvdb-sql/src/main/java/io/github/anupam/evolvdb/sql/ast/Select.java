package io.github.anupam.evolvdb.sql.ast;

import java.util.List;
import java.util.Objects;

/** SELECT selectItems FROM tableRef [WHERE expr] */
public final class Select extends Statement {
    private final List<SelectItem> items;
    private final List<TableRef> froms;
    private final Expr where; // may be null
    private final List<Expr> groupBy; // may be empty

    public Select(SourcePos pos, List<SelectItem> items, List<TableRef> froms, Expr where, List<Expr> groupBy) {
        super(pos);
        this.items = List.copyOf(Objects.requireNonNull(items, "items"));
        this.froms = List.copyOf(Objects.requireNonNull(froms, "froms"));
        if (this.froms.isEmpty()) throw new IllegalArgumentException("at least one FROM table required");
        this.where = where;
        this.groupBy = groupBy == null ? List.of() : List.copyOf(groupBy);
    }

    public List<SelectItem> items() { return items; }
    public TableRef from() { return froms.get(0); }
    public List<TableRef> froms() { return froms; }
    public Expr where() { return where; }
    public List<Expr> groupBy() { return groupBy; }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitSelect(this, context);
    }
}
