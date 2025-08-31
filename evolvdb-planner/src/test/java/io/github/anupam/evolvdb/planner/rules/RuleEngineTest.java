package io.github.anupam.evolvdb.planner.rules;

import io.github.anupam.evolvdb.planner.logical.*;
import io.github.anupam.evolvdb.sql.ast.ColumnRef;
import io.github.anupam.evolvdb.sql.ast.ComparisonExpr;
import io.github.anupam.evolvdb.sql.ast.Expr;
import io.github.anupam.evolvdb.sql.ast.Literal;
import io.github.anupam.evolvdb.sql.ast.SourcePos;
import io.github.anupam.evolvdb.types.ColumnMeta;
import io.github.anupam.evolvdb.types.Schema;
import io.github.anupam.evolvdb.types.Type;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RuleEngineTest {

    private static Schema schema() {
        return new Schema(List.of(
                new ColumnMeta("id", Type.INT, null),
                new ColumnMeta("name", Type.VARCHAR, 10)
        ));
    }

    @Test
    void pushProjectBelowFilter_applies() {
        Schema s = schema();
        LogicalPlan scan = new LogicalScan("users", null, s);
        Expr pred = new ComparisonExpr(new SourcePos(1,1), ComparisonExpr.Op.GTE,
                new ColumnRef(new SourcePos(1,1), null, "id"), new Literal(new SourcePos(1,1), 10));
        LogicalPlan filter = new LogicalFilter(scan, pred);
        List<ProjectItem> items = List.of(
                new ProjectItem(new ColumnRef(new SourcePos(1,1), null, "id"), "id"),
                new ProjectItem(new ColumnRef(new SourcePos(1,1), null, "name"), "name")
        );
        LogicalPlan project = new LogicalProject(filter, items, s);

        RuleEngine engine = new RuleEngine(List.of(new PushProjectBelowFilter()));
        LogicalPlan out = engine.apply(project);

        assertTrue(out instanceof LogicalFilter);
        assertTrue(((LogicalFilter) out).child() instanceof LogicalProject);
    }

    @Test
    void removeRedundantProject_applies() {
        Schema s = schema();
        LogicalPlan scan = new LogicalScan("users", null, s);
        List<ProjectItem> items = List.of(
                new ProjectItem(new ColumnRef(new SourcePos(1,1), null, "id"), "id"),
                new ProjectItem(new ColumnRef(new SourcePos(1,1), null, "name"), "name")
        );
        LogicalPlan project = new LogicalProject(scan, items, s);

        RuleEngine engine = new RuleEngine(List.of(new RemoveRedundantProject()));
        LogicalPlan out = engine.apply(project);

        assertTrue(out instanceof LogicalScan);
    }
}
