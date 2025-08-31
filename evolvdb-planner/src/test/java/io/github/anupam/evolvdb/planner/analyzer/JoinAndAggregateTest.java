package io.github.anupam.evolvdb.planner.analyzer;

import io.github.anupam.evolvdb.catalog.CatalogManager;
import io.github.anupam.evolvdb.config.DbConfig;
import io.github.anupam.evolvdb.core.Database;
import io.github.anupam.evolvdb.planner.logical.*;
import io.github.anupam.evolvdb.planner.rules.PredicateSimplification;
import io.github.anupam.evolvdb.sql.ast.Statement;
import io.github.anupam.evolvdb.sql.parser.SqlParser;
import io.github.anupam.evolvdb.types.ColumnMeta;
import io.github.anupam.evolvdb.types.Schema;
import io.github.anupam.evolvdb.types.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class JoinAndAggregateTest {
    private Path tmpDir;

    private Database db() throws Exception {
        tmpDir = Files.createTempDirectory("evolvdb-planner-");
        DbConfig cfg = DbConfig.builder().pageSize(4096).bufferPoolPages(32).dataDir(tmpDir).build();
        return new Database(cfg);
    }

    @AfterEach
    void cleanup() throws Exception {
        if (tmpDir != null && Files.exists(tmpDir)) {
            try (var walk = Files.walk(tmpDir)) {
                walk.sorted((a,b)->b.getNameCount()-a.getNameCount()).forEach(p -> { try { Files.deleteIfExists(p); } catch (Exception ignored) {} });
            }
        }
    }

    @Test
    void innerJoin_withoutAlias_bindsToJoinAndFilter() throws Exception {
        try (Database db = db()) {
            CatalogManager cat = db.catalog();
            Schema users = new Schema(List.of(
                    new ColumnMeta("id", Type.INT, null),
                    new ColumnMeta("name", Type.VARCHAR, 10)
            ));
            Schema orders = new Schema(List.of(
                    new ColumnMeta("id", Type.INT, null),
                    new ColumnMeta("user_id", Type.INT, null),
                    new ColumnMeta("amount", Type.FLOAT, null)
            ));
            cat.createTable("users", users);
            cat.createTable("orders", orders);

            var parser = new SqlParser();
            Statement stmt = (Statement) parser.parse("SELECT u.name, o.amount FROM users u, orders o WHERE u.id = o.user_id AND o.amount > 10");
            Analyzer analyzer = new Analyzer();
            LogicalPlan plan = analyzer.analyze(stmt, cat, List.of(new PredicateSimplification()));

            assertInstanceOf(LogicalProject.class, plan);
            LogicalPlan child = ((LogicalProject) plan).child();
            assertTrue(child instanceof LogicalFilter);
            LogicalJoin join = (LogicalJoin) ((LogicalFilter) child).child();
            assertNotNull(join.condition());
            assertTrue(join.schema().size() >= 4);
        }
    }

    @Test
    void ambiguousColumn_throws() throws Exception {
        try (Database db = db()) {
            CatalogManager cat = db.catalog();
            Schema users = new Schema(List.of(new ColumnMeta("id", Type.INT, null)));
            Schema orders = new Schema(List.of(new ColumnMeta("id", Type.INT, null)));
            cat.createTable("users", users);
            cat.createTable("orders", orders);
            var parser = new SqlParser();
            Statement stmt = (Statement) parser.parse("SELECT id FROM users, orders");
            Analyzer analyzer = new Analyzer();
            assertThrows(IllegalArgumentException.class, () -> analyzer.analyze(stmt, cat, List.of()));
        }
    }

    @Test
    void groupByAndAgg_bindsToLogicalAggregate() throws Exception {
        try (Database db = db()) {
            CatalogManager cat = db.catalog();
            Schema orders = new Schema(List.of(
                    new ColumnMeta("user_id", Type.INT, null),
                    new ColumnMeta("amount", Type.FLOAT, null)
            ));
            cat.createTable("orders", orders);

            var parser = new SqlParser();
            Statement stmt = (Statement) parser.parse("SELECT user_id, COUNT(*) AS cnt, SUM(amount) AS total FROM orders GROUP BY user_id");
            Analyzer analyzer = new Analyzer();
            LogicalPlan plan = analyzer.analyze(stmt, cat, List.of());
            assertTrue(plan instanceof LogicalAggregate);
            LogicalAggregate agg = (LogicalAggregate) plan;
            assertEquals(3, agg.schema().size());
        }
    }

    @Test
    void nonAggregatedColumnNotInGroupBy_throws() throws Exception {
        try (Database db = db()) {
            CatalogManager cat = db.catalog();
            Schema orders = new Schema(List.of(
                    new ColumnMeta("user_id", Type.INT, null),
                    new ColumnMeta("amount", Type.FLOAT, null)
            ));
            cat.createTable("orders", orders);

            var parser = new SqlParser();
            Statement stmt = (Statement) parser.parse("SELECT user_id, amount FROM orders GROUP BY user_id");
            Analyzer analyzer = new Analyzer();
            assertThrows(IllegalArgumentException.class, () -> analyzer.analyze(stmt, cat, List.of()));
        }
    }
}
