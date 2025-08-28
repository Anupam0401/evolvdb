package io.github.anupam.evolvdb.sql.ast;

/** Source position for error reporting (1-based line/column). */
public record SourcePos(int line, int column) {
    public SourcePos {
        if (line < 1 || column < 1) throw new IllegalArgumentException("line/column must be >= 1");
    }
}
