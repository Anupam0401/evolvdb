package io.github.anupam.evolvdb.sql.ast;

/** Comparison operators: =, <>, <, <=, >, >= */
public final class ComparisonExpr extends Expr {
    public enum Op { EQ, NEQ, LT, LTE, GT, GTE }
    private final Op op;
    private final Expr left;
    private final Expr right;

    public ComparisonExpr(SourcePos pos, Op op, Expr left, Expr right) {
        super(pos);
        this.op = op;
        this.left = left;
        this.right = right;
    }

    public Op op() { return op; }
    public Expr left() { return left; }
    public Expr right() { return right; }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitComparisonExpr(this, context);
    }
}
