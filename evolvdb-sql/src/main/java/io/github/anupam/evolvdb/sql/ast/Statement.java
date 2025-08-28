package io.github.anupam.evolvdb.sql.ast;

/** Base class for SQL statements. */
public abstract class Statement extends AstNode {
    protected Statement(SourcePos pos) { super(pos); }
}
