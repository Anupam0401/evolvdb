package io.github.anupam.evolvdb.sql.ast;

/** Arithmetic/string concatenation or generic binary op. */
public final class BinaryExpr extends Expr {
    public enum Op { ADD, SUB, MUL, DIV, CONCAT }
    private final Op op;
    private final Expr left;
    private final Expr right;

    public BinaryExpr(SourcePos pos, Op op, Expr left, Expr right) {
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
        return visitor.visitBinaryExpr(this, context);
    }
}
