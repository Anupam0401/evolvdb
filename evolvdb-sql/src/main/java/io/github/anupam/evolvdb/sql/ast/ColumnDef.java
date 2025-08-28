package io.github.anupam.evolvdb.sql.ast;

import io.github.anupam.evolvdb.types.Type;

/** Column definition in CREATE TABLE. */
public record ColumnDef(String name, Type type, Integer length) { }
