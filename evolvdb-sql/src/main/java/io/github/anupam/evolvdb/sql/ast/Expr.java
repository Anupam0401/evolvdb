package io.github.anupam.evolvdb.sql.ast;

/** Base class for expressions. */
public abstract class Expr extends AstNode {
    protected Expr(SourcePos pos) { super(pos); }
}
