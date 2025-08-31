package io.github.anupam.evolvdb.planner.analyzer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.anupam.evolvdb.catalog.CatalogManager;
import io.github.anupam.evolvdb.config.DbConfig;
import io.github.anupam.evolvdb.core.Database;
import io.github.anupam.evolvdb.planner.logical.LogicalFilter;
import io.github.anupam.evolvdb.planner.logical.LogicalInsert;
import io.github.anupam.evolvdb.planner.logical.LogicalPlan;
import io.github.anupam.evolvdb.planner.logical.LogicalProject;
import io.github.anupam.evolvdb.planner.logical.LogicalScan;
import io.github.anupam.evolvdb.sql.ast.Statement;
import io.github.anupam.evolvdb.sql.parser.SqlParser;
import io.github.anupam.evolvdb.types.ColumnMeta;
import io.github.anupam.evolvdb.types.Schema;
import io.github.anupam.evolvdb.types.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class BinderTest {
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
    void select_withProjectionAndFilter_bindsToProjectFilterScan() throws Exception {
        try (Database db = db()) {
            CatalogManager cat = db.catalog();
            Schema schema = new Schema(List.of(
                    new ColumnMeta("id", Type.INT, null),
                    new ColumnMeta("name", Type.VARCHAR, 10),
                    new ColumnMeta("active", Type.BOOLEAN, null)
            ));
            cat.createTable("users", schema);

            var parser = new SqlParser();
            Statement stmt = (Statement) parser.parse("SELECT id, name FROM users WHERE id >= 10");
            Analyzer analyzer = new Analyzer();
            LogicalPlan plan = analyzer.analyze(stmt, cat, List.of());

            assertTrue(plan instanceof LogicalProject);
            LogicalProject proj = (LogicalProject) plan;
            assertEquals(2, proj.schema().size());
            assertTrue(proj.child() instanceof LogicalFilter);
            LogicalFilter filt = (LogicalFilter) proj.child();
            assertTrue(filt.child() instanceof LogicalScan);
            LogicalScan scan = (LogicalScan) filt.child();
            assertEquals("users", scan.tableName());
        }
    }

    @Test
    void select_star_returnsScanOrFilterWithoutProject() throws Exception {
        try (Database db = db()) {
            CatalogManager cat = db.catalog();
            Schema schema = new Schema(List.of(
                    new ColumnMeta("id", Type.INT, null),
                    new ColumnMeta("name", Type.VARCHAR, 10)
            ));
            cat.createTable("users", schema);

            var parser = new SqlParser();
            Statement stmt = (Statement) parser.parse("SELECT * FROM users");
            Analyzer analyzer = new Analyzer();
            LogicalPlan plan = analyzer.analyze(stmt, cat, List.of());

            assertTrue(plan instanceof LogicalScan);
        }
    }

    @Test
    void select_unknownTable_throws() throws Exception {
        try (Database db = db()) {
            CatalogManager cat = db.catalog();
            var parser = new SqlParser();
            Statement stmt = (Statement) parser.parse("SELECT * FROM nope");
            Analyzer analyzer = new Analyzer();
            assertThrows(IllegalArgumentException.class, () -> analyzer.analyze(stmt, cat, List.of()));
        }
    }

    @Test
    void select_unknownColumn_throws() throws Exception {
        try (Database db = db()) {
            CatalogManager cat = db.catalog();
            Schema schema = new Schema(List.of(
                    new ColumnMeta("id", Type.INT, null),
                    new ColumnMeta("name", Type.VARCHAR, 10)
            ));
            cat.createTable("users", schema);

            var parser = new SqlParser();
            Statement stmt = (Statement) parser.parse("SELECT foo FROM users");
            Analyzer analyzer = new Analyzer();
            assertThrows(IllegalArgumentException.class, () -> analyzer.analyze(stmt, cat, List.of()));
        }
    }

    @Test
    void insert_bindsToLogicalInsert() throws Exception {
        try (Database db = db()) {
            CatalogManager cat = db.catalog();
            Schema schema = new Schema(List.of(
                    new ColumnMeta("id", Type.INT, null),
                    new ColumnMeta("name", Type.VARCHAR, 5)
            ));
            cat.createTable("users", schema);

            var parser = new SqlParser();
            Statement stmt = (Statement) parser.parse("INSERT INTO users VALUES (1, 'Amy')");
            Analyzer analyzer = new Analyzer();
            LogicalPlan plan = analyzer.analyze(stmt, cat, List.of());
            assertTrue(plan instanceof LogicalInsert);
            LogicalInsert li = (LogicalInsert) plan;
            assertEquals("users", li.tableName());
            assertEquals(2, li.targetColumns().size());
        }
    }
}
