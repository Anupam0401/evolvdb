package io.github.anupam.evolvdb.sql.parser;

import io.github.anupam.evolvdb.sql.ast.SourcePos;

public record Token(TokenType type, String lexeme, SourcePos pos) {
    @Override
    public String toString() {
        return type + (lexeme != null ? ("('" + lexeme + "')") : "") + "@" + pos.line() + ":" + pos.column();
    }
}
