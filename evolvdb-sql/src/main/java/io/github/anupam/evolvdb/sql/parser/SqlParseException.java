package io.github.anupam.evolvdb.sql.parser;

import io.github.anupam.evolvdb.sql.ast.SourcePos;

public class SqlParseException extends RuntimeException {
    private final SourcePos pos;

    public SqlParseException(String message, SourcePos pos) {
        super(message + (pos != null ? (" at " + pos.line() + ":" + pos.column()) : ""));
        this.pos = pos;
    }

    public SourcePos pos() { return pos; }
}
