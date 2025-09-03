package io.github.anupam.evolvdb.exec;

import io.github.anupam.evolvdb.catalog.CatalogManager;
import io.github.anupam.evolvdb.config.DbConfig;
import io.github.anupam.evolvdb.core.Database;
import io.github.anupam.evolvdb.exec.op.PhysicalOperator;
import io.github.anupam.evolvdb.planner.analyzer.Analyzer;
import io.github.anupam.evolvdb.planner.logical.LogicalPlan;
import io.github.anupam.evolvdb.sql.ast.Statement;
import io.github.anupam.evolvdb.sql.parser.SqlParser;
import io.github.anupam.evolvdb.types.ColumnMeta;
import io.github.anupam.evolvdb.types.Schema;
import io.github.anupam.evolvdb.types.Tuple;
import io.github.anupam.evolvdb.types.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class InsertExecTest {
    private Path tmpDir;

    private Database db() throws Exception {
        tmpDir = Files.createTempDirectory("evolvdb-exec-insert-");
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
    void insert_then_select() throws Exception {
        try (Database db = db()) {
            CatalogManager cat = db.catalog();
            
            // Create table
            Schema users = new Schema(List.of(
                    new ColumnMeta("id", Type.INT, null),
                    new ColumnMeta("name", Type.STRING, null),
                    new ColumnMeta("age", Type.INT, null)
            ));
            cat.createTable("users", users);
            
            // Execute INSERT
            SqlParser parser = new SqlParser();
            Statement insertStmt = (Statement) parser.parse(
                "INSERT INTO users VALUES (1, 'Alice', 25), (2, 'Bob', 30), (3, 'Charlie', 35)"
            );
            
            Analyzer analyzer = new Analyzer();
            LogicalPlan insertPlan = analyzer.analyze(insertStmt, cat, List.of());
            
            ExecContext ctx = new ExecContext(cat);
            PhysicalPlanner pp = new PhysicalPlanner();
            PhysicalOperator insertOp = pp.plan(insertPlan, ctx);
            
            insertOp.open();
            Tuple result = insertOp.next();
            assertNotNull(result);
            assertEquals(3, result.get(0)); // 3 rows inserted
            assertNull(insertOp.next()); // no more results
            insertOp.close();
            
            // Execute SELECT to verify
            Statement selectStmt = (Statement) parser.parse("SELECT * FROM users");
            LogicalPlan selectPlan = analyzer.analyze(selectStmt, cat, List.of());
            PhysicalOperator selectOp = pp.plan(selectPlan, ctx);
            
            selectOp.open();
            List<Tuple> rows = new ArrayList<>();
            for (Tuple t = selectOp.next(); t != null; t = selectOp.next()) {
                rows.add(t);
            }
            selectOp.close();
            
            assertEquals(3, rows.size());
            
            // Verify data
            boolean foundAlice = false, foundBob = false, foundCharlie = false;
            for (Tuple row : rows) {
                int id = (Integer) row.get(0);
                String name = (String) row.get(1);
                int age = (Integer) row.get(2);
                
                if (id == 1) {
                    assertEquals("Alice", name);
                    assertEquals(25, age);
                    foundAlice = true;
                } else if (id == 2) {
                    assertEquals("Bob", name);
                    assertEquals(30, age);
                    foundBob = true;
                } else if (id == 3) {
                    assertEquals("Charlie", name);
                    assertEquals(35, age);
                    foundCharlie = true;
                }
            }
            assertTrue(foundAlice && foundBob && foundCharlie);
        }
    }

    @Test
    void insert_with_expressions() throws Exception {
        try (Database db = db()) {
            CatalogManager cat = db.catalog();
            
            // Create table
            Schema data = new Schema(List.of(
                    new ColumnMeta("x", Type.INT, null),
                    new ColumnMeta("y", Type.INT, null),
                    new ColumnMeta("label", Type.STRING, null)
            ));
            cat.createTable("data", data);
            
            // INSERT with arithmetic expressions
            SqlParser parser = new SqlParser();
            Statement insertStmt = (Statement) parser.parse(
                "INSERT INTO data VALUES (10 + 5, 20 * 2, 'First'), (100 - 50, 8 / 2, 'Second')"
            );
            
            Analyzer analyzer = new Analyzer();
            LogicalPlan insertPlan = analyzer.analyze(insertStmt, cat, List.of());
            
            ExecContext ctx = new ExecContext(cat);
            PhysicalPlanner pp = new PhysicalPlanner();
            PhysicalOperator insertOp = pp.plan(insertPlan, ctx);
            
            insertOp.open();
            Tuple result = insertOp.next();
            assertNotNull(result);
            assertEquals(2, result.get(0)); // 2 rows inserted
            insertOp.close();
            
            // Verify data
            Statement selectStmt = (Statement) parser.parse("SELECT * FROM data");
            LogicalPlan selectPlan = analyzer.analyze(selectStmt, cat, List.of());
            PhysicalOperator selectOp = pp.plan(selectPlan, ctx);
            
            selectOp.open();
            List<Tuple> rows = new ArrayList<>();
            for (Tuple t = selectOp.next(); t != null; t = selectOp.next()) {
                rows.add(t);
            }
            selectOp.close();
            
            assertEquals(2, rows.size());
            
            for (Tuple row : rows) {
                int x = (Integer) row.get(0);
                int y = (Integer) row.get(1);
                String label = (String) row.get(2);
                
                if ("First".equals(label)) {
                    assertEquals(15, x); // 10 + 5
                    assertEquals(40, y); // 20 * 2
                } else if ("Second".equals(label)) {
                    assertEquals(50, x); // 100 - 50
                    assertEquals(4, y);  // 8 / 2
                }
            }
        }
    }

    @Test
    void insert_then_filter() throws Exception {
        try (Database db = db()) {
            CatalogManager cat = db.catalog();
            
            // Create and populate table
            Schema products = new Schema(List.of(
                    new ColumnMeta("id", Type.INT, null),
                    new ColumnMeta("name", Type.STRING, null),
                    new ColumnMeta("price", Type.INT, null)
            ));
            cat.createTable("products", products);
            
            SqlParser parser = new SqlParser();
            Statement insertStmt = (Statement) parser.parse(
                "INSERT INTO products VALUES (1, 'Apple', 5), (2, 'Banana', 3), (3, 'Orange', 4), (4, 'Mango', 8)"
            );
            
            Analyzer analyzer = new Analyzer();
            LogicalPlan insertPlan = analyzer.analyze(insertStmt, cat, List.of());
            
            ExecContext ctx = new ExecContext(cat);
            PhysicalPlanner pp = new PhysicalPlanner();
            PhysicalOperator insertOp = pp.plan(insertPlan, ctx);
            
            insertOp.open();
            insertOp.next();
            insertOp.close();
            
            // Query with filter
            Statement selectStmt = (Statement) parser.parse(
                "SELECT name FROM products WHERE price > 4"
            );
            LogicalPlan selectPlan = analyzer.analyze(selectStmt, cat, List.of());
            PhysicalOperator selectOp = pp.plan(selectPlan, ctx);
            
            selectOp.open();
            List<Tuple> rows = new ArrayList<>();
            for (Tuple t = selectOp.next(); t != null; t = selectOp.next()) {
                rows.add(t);
            }
            selectOp.close();
            
            // Should get Apple (5) and Mango (8)
            assertEquals(2, rows.size());
            List<String> names = new ArrayList<>();
            for (Tuple row : rows) {
                names.add((String) row.get(0));
            }
            assertTrue(names.contains("Apple"));
            assertTrue(names.contains("Mango"));
        }
    }
}
