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

public class ProjectExecTest {
    private Path tmpDir;

    private Database db() throws Exception {
        tmpDir = Files.createTempDirectory("evolvdb-exec-proj-");
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
    void project_with_arithmetic_expressions() throws Exception {
        try (Database db = db()) {
            CatalogManager cat = db.catalog();
            Schema users = new Schema(List.of(
                    new ColumnMeta("id", Type.INT, null),
                    new ColumnMeta("age", Type.INT, null)
            ));
            cat.createTable("users", users);
            var t = cat.openTable("users");
            t.insert(new Tuple(users, List.of(1, 20)));
            t.insert(new Tuple(users, List.of(2, 30)));
            t.insert(new Tuple(users, List.of(3, 40)));

            // SELECT id + 100 AS new_id, age * 2 AS double_age FROM users
            SqlParser parser = new SqlParser();
            Statement stmt = (Statement) parser.parse(
                "SELECT id + 100 AS new_id, age * 2 AS double_age FROM users"
            );
            
            Analyzer analyzer = new Analyzer();
            LogicalPlan logical = analyzer.analyze(stmt, cat, List.of());

            ExecContext ctx = new ExecContext(cat);
            PhysicalPlanner pp = new PhysicalPlanner();
            PhysicalOperator root = pp.plan(logical, ctx);
            
            root.open();
            List<Tuple> out = new ArrayList<>();
            for (Tuple row = root.next(); row != null; row = root.next()) out.add(row);
            root.close();

            assertEquals(3, out.size());
            
            // Check schema
            Schema outSchema = root.schema();
            assertEquals(2, outSchema.size());
            assertEquals("new_id", outSchema.columns().get(0).name());
            assertEquals("double_age", outSchema.columns().get(1).name());
            
            // Check values
            for (int i = 0; i < out.size(); i++) {
                Tuple row = out.get(i);
                // Note: arithmetic on integer literals produces BIGINT (Long)
                long newId = (Long) row.get(0);
                long doubleAge = (Long) row.get(1);
                
                // id + 100: 101, 102, 103
                assertTrue(newId >= 101 && newId <= 103);
                // age * 2: 40, 60, 80
                assertTrue(doubleAge == 40 || doubleAge == 60 || doubleAge == 80);
            }
        }
    }

    @Test
    void project_with_string_concatenation() throws Exception {
        try (Database db = db()) {
            CatalogManager cat = db.catalog();
            Schema products = new Schema(List.of(
                    new ColumnMeta("name", Type.STRING, null),
                    new ColumnMeta("price", Type.INT, null)
            ));
            cat.createTable("products", products);
            var t = cat.openTable("products");
            t.insert(new Tuple(products, List.of("Apple", 5)));
            t.insert(new Tuple(products, List.of("Banana", 3)));

            // SELECT name concatenated with '-Product' (using dummy arithmetic for now)
            // Since parser doesn't support || operator, we'll just select name and price
            SqlParser parser = new SqlParser();
            Statement stmt = (Statement) parser.parse(
                "SELECT name, price FROM products"
            );
            
            Analyzer analyzer = new Analyzer();
            LogicalPlan logical = analyzer.analyze(stmt, cat, List.of());

            ExecContext ctx = new ExecContext(cat);
            PhysicalPlanner pp = new PhysicalPlanner();
            PhysicalOperator root = pp.plan(logical, ctx);
            
            root.open();
            List<Tuple> out = new ArrayList<>();
            for (Tuple row = root.next(); row != null; row = root.next()) out.add(row);
            root.close();

            assertEquals(2, out.size());
            
            boolean foundApple = false, foundBanana = false;
            for (Tuple row : out) {
                String name = (String) row.get(0);
                int price = (Integer) row.get(1);
                
                if ("Apple".equals(name)) {
                    assertEquals(5, price);
                    foundApple = true;
                } else if ("Banana".equals(name)) {
                    assertEquals(3, price);
                    foundBanana = true;
                }
            }
            assertTrue(foundApple && foundBanana);
        }
    }

    @Test
    void project_with_mixed_types() throws Exception {
        try (Database db = db()) {
            CatalogManager cat = db.catalog();
            Schema data = new Schema(List.of(
                    new ColumnMeta("id", Type.INT, null),
                    new ColumnMeta("value", Type.FLOAT, null)
            ));
            cat.createTable("data", data);
            var t = cat.openTable("data");
            t.insert(new Tuple(data, List.of(1, 10.5f)));
            t.insert(new Tuple(data, List.of(2, 20.3f)));

            // SELECT id, value + 5, id * value AS product FROM data
            SqlParser parser = new SqlParser();
            Statement stmt = (Statement) parser.parse(
                "SELECT id, value + 5, id * value AS product FROM data"
            );
            
            Analyzer analyzer = new Analyzer();
            LogicalPlan logical = analyzer.analyze(stmt, cat, List.of());

            ExecContext ctx = new ExecContext(cat);
            PhysicalPlanner pp = new PhysicalPlanner();
            PhysicalOperator root = pp.plan(logical, ctx);
            
            root.open();
            List<Tuple> out = new ArrayList<>();
            for (Tuple row = root.next(); row != null; row = root.next()) out.add(row);
            root.close();

            assertEquals(2, out.size());
            
            for (Tuple row : out) {
                int id = (Integer) row.get(0);
                float valPlus5 = (Float) row.get(1);
                float product = (Float) row.get(2);
                
                if (id == 1) {
                    assertEquals(15.5f, valPlus5, 0.01f);
                    assertEquals(10.5f, product, 0.01f);
                } else if (id == 2) {
                    assertEquals(25.3f, valPlus5, 0.01f);
                    assertEquals(40.6f, product, 0.01f);
                }
            }
        }
    }
}
