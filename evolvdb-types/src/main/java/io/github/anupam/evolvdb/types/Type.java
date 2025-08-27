package io.github.anupam.evolvdb.types;

/** Primitive logical types supported initially. */
public enum Type {
    INT,
    BIGINT,
    BOOLEAN,
    VARCHAR,
    FLOAT,
    STRING // variable-length textual type (alias-like, does not require explicit length)
}
