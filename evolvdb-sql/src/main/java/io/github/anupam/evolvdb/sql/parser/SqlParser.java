package io.github.anupam.evolvdb.sql.parser;

import io.github.anupam.evolvdb.sql.ast.AstNode;

/**
 * SQL parser entrypoint. Produces a typed AST from SQL text.
 *
 * Implementation will be a hand-rolled recursive descent parser over a token stream.
 */
public final class SqlParser {

    /** Parses the given SQL string into an AST. */
    public AstNode parse(String sql) {
        if (sql == null) throw new IllegalArgumentException("sql");
        throw new UnsupportedOperationException("SqlParser not implemented yet");
    }
}
