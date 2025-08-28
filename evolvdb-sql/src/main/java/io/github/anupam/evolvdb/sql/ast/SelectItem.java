package io.github.anupam.evolvdb.sql.ast;

/** One item in SELECT list: either '*' or an expression with optional alias. */
public final class SelectItem extends AstNode {
    private final Expr expr;      // null when star=true
    private final boolean star;   // true for SELECT *
    private final String alias;   // nullable

    public static SelectItem star(SourcePos pos) { return new SelectItem(pos, null, true, null); }

    public SelectItem(SourcePos pos, Expr expr, boolean star, String alias) {
        super(pos);
        this.expr = expr;
        this.star = star;
        this.alias = (alias != null && !alias.isBlank()) ? alias : null;
    }

    public boolean isStar() { return star; }
    public Expr expr() { return expr; }
    public String alias() { return alias; }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitSelectItem(this, context);
    }
}
