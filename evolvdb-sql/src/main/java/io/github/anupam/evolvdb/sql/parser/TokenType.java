package io.github.anupam.evolvdb.sql.parser;

public enum TokenType {
    // Special
    EOF,

    // Identifiers & literals
    IDENT, NUMBER, STRING,

    // Keywords
    CREATE, TABLE, DROP, INSERT, INTO, VALUES, SELECT, FROM, WHERE, AS,
    GROUP, BY,
    AND, OR, NOT,
    TRUE, FALSE,
    INT, BIGINT, BOOLEAN, FLOAT, STRING_T, VARCHAR,

    // Symbols
    LPAREN, RPAREN, COMMA, DOT, SEMI, STAR,

    // Operators
    PLUS, MINUS, SLASH, TIMES,
    EQ, NEQ, LT, LTE, GT, GTE
}
