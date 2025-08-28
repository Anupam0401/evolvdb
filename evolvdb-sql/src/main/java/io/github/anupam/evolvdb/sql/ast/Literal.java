package io.github.anupam.evolvdb.sql.ast;

/** Literal value (number, string, boolean, null in future). */
public final class Literal extends Expr {
    private final Object value;

    public Literal(SourcePos pos, Object value) {
        super(pos);
        this.value = value; // may be null in future for NULL literal
    }

    public Object value() { return value; }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitLiteral(this, context);
    }
}
