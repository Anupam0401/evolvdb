package io.github.anupam.evolvdb.sql.ast;

/** Logical expressions: AND, OR, NOT. */
public final class LogicalExpr extends Expr {
    public enum Op { AND, OR, NOT }
    private final Op op;
    private final Expr left;  // for NOT, left holds the single operand; right is null
    private final Expr right; // may be null for NOT

    public LogicalExpr(SourcePos pos, Op op, Expr left, Expr right) {
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
        return visitor.visitLogicalExpr(this, context);
    }
}
