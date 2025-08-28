package io.github.anupam.evolvdb.sql.ast;

import java.util.Objects;

/** Base class for all AST nodes. Immutable. */
public abstract class AstNode {
    private final SourcePos pos;

    protected AstNode(SourcePos pos) {
        this.pos = Objects.requireNonNull(pos, "pos");
    }

    public SourcePos pos() { return pos; }

    public abstract <R, C> R accept(AstVisitor<R, C> visitor, C context);
}
