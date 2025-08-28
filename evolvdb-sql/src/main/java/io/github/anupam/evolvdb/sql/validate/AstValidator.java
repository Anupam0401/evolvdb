package io.github.anupam.evolvdb.sql.validate;

import io.github.anupam.evolvdb.sql.ast.AstNode;
import io.github.anupam.evolvdb.catalog.CatalogManager;

/** Performs basic validation over the AST. Catalog-aware validation will be added in planner. */
public final class AstValidator {
    public void validate(AstNode node, CatalogManager catalog) {
        if (node == null) throw new IllegalArgumentException("node");
        // For M8 scaffolding this is a stub; real validation will come with binder.
        // Keep signature catalog-aware for future-proofing.
    }
}
