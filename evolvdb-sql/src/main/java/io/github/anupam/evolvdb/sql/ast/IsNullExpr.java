package io.github.anupam.evolvdb.sql.ast;

/** IS NULL / IS NOT NULL predicate. When {@code negated} is true, represents IS NOT NULL. */
public final class IsNullExpr extends Expr {
    private final Expr operand;
    private final boolean negated;

    public IsNullExpr(SourcePos pos, Expr operand, boolean negated) {
        super(pos);
        this.operand = operand;
        this.negated = negated;
    }

    public Expr operand() {
        return operand;
    }

    public boolean isNegated() {
        return negated;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitIsNullExpr(this, context);
    }
}
