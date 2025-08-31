package io.github.anupam.evolvdb.planner.analyzer;

import io.github.anupam.evolvdb.types.Schema;

/** Single-table binding context. Future: multiple tables for joins. */
public record BindingContext(String tableName, String alias, Schema schema) { }
