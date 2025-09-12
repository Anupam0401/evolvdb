package io.github.anupam.evolvdb.planner.logical;

import io.github.anupam.evolvdb.sql.ast.Expr;

/** One projection item with a resolved output name. */
public record ProjectItem(Expr expr, String name) { }
