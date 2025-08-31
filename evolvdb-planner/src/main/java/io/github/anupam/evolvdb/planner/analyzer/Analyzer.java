package io.github.anupam.evolvdb.planner.analyzer;

import io.github.anupam.evolvdb.catalog.CatalogManager;
import io.github.anupam.evolvdb.planner.logical.LogicalPlan;
import io.github.anupam.evolvdb.planner.rules.Rule;
import io.github.anupam.evolvdb.planner.rules.RuleEngine;
import io.github.anupam.evolvdb.sql.ast.Statement;

import java.util.List;
import java.util.Objects;

/** Orchestrates binding and rule-based transformations to produce a logical plan. */
public final class Analyzer {
    private final Binder binder = new Binder();

    public LogicalPlan analyze(Statement stmt, CatalogManager catalog, List<Rule> rules) {
        Objects.requireNonNull(stmt, "stmt");
        Objects.requireNonNull(catalog, "catalog");
        List<Rule> ruleList = rules == null ? List.of() : List.copyOf(rules);
        LogicalPlan bound = binder.bind(stmt, catalog);
        if (ruleList.isEmpty()) return bound;
        return new RuleEngine(ruleList).apply(bound);
    }
}
