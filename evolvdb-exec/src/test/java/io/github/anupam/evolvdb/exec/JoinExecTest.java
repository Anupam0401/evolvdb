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

public class JoinExecTest {
    private Path tmpDir;

    private Database db() throws Exception {
        tmpDir = Files.createTempDirectory("evolvdb-exec-join-");
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
    void multi_table_join_equi_predicate() throws Exception {
        try (Database db = db()) {
            CatalogManager cat = db.catalog();
            
            // Create users table
            Schema users = new Schema(List.of(
                    new ColumnMeta("id", Type.INT, null),
                    new ColumnMeta("name", Type.STRING, null)
            ));
            cat.createTable("users", users);
            var usersTable = cat.openTable("users");
            usersTable.insert(new Tuple(users, List.of(1, "Alice")));
            usersTable.insert(new Tuple(users, List.of(2, "Bob")));
            usersTable.insert(new Tuple(users, List.of(3, "Charlie")));

            // Create orders table
            Schema orders = new Schema(List.of(
                    new ColumnMeta("id", Type.INT, null),
                    new ColumnMeta("user_id", Type.INT, null),
                    new ColumnMeta("amount", Type.INT, null)
            ));
            cat.createTable("orders", orders);
            var ordersTable = cat.openTable("orders");
            ordersTable.insert(new Tuple(orders, List.of(101, 1, 50)));
            ordersTable.insert(new Tuple(orders, List.of(102, 2, 30)));
            ordersTable.insert(new Tuple(orders, List.of(103, 1, 20)));
            ordersTable.insert(new Tuple(orders, List.of(104, 3, 100)));

            // Query: SELECT u.name, o.amount FROM users u, orders o WHERE u.id = o.user_id
            SqlParser parser = new SqlParser();
            Statement stmt = (Statement) parser.parse(
                "SELECT u.name, o.amount FROM users u, orders o WHERE u.id = o.user_id"
            );
            
            Analyzer analyzer = new Analyzer();
            LogicalPlan logical = analyzer.analyze(stmt, cat, List.of());

            ExecContext ctx = new ExecContext(cat);
            PhysicalPlanner pp = new PhysicalPlanner();
            PhysicalOperator root = pp.plan(logical, ctx);
            
            root.open();
            List<Tuple> out = new ArrayList<>();
            for (Tuple t = root.next(); t != null; t = root.next()) out.add(t);
            root.close();

            // Expected: 4 rows (Alice,50), (Bob,30), (Alice,20), (Charlie,100)
            assertEquals(4, out.size());
            
            // Check results
            int aliceCount = 0, bobCount = 0, charlieCount = 0;
            for (Tuple row : out) {
                String name = (String) row.get(0);
                int amount = (Integer) row.get(1);
                
                if ("Alice".equals(name)) {
                    assertTrue(amount == 50 || amount == 20);
                    aliceCount++;
                } else if ("Bob".equals(name)) {
                    assertEquals(30, amount);
                    bobCount++;
                } else if ("Charlie".equals(name)) {
                    assertEquals(100, amount);
                    charlieCount++;
                } else {
                    fail("Unexpected name: " + name);
                }
            }
            assertEquals(2, aliceCount);
            assertEquals(1, bobCount);
            assertEquals(1, charlieCount);
        }
    }

    @Test
    void join_with_mixed_predicates() throws Exception {
        try (Database db = db()) {
            CatalogManager cat = db.catalog();
            
            // Create tables
            Schema users = new Schema(List.of(
                    new ColumnMeta("id", Type.INT, null),
                    new ColumnMeta("age", Type.INT, null)
            ));
            cat.createTable("users", users);
            var usersTable = cat.openTable("users");
            usersTable.insert(new Tuple(users, List.of(1, 25)));
            usersTable.insert(new Tuple(users, List.of(2, 35)));
            usersTable.insert(new Tuple(users, List.of(3, 18)));

            Schema orders = new Schema(List.of(
                    new ColumnMeta("user_id", Type.INT, null),
                    new ColumnMeta("amount", Type.INT, null)
            ));
            cat.createTable("orders", orders);
            var ordersTable = cat.openTable("orders");
            ordersTable.insert(new Tuple(orders, List.of(1, 100)));
            ordersTable.insert(new Tuple(orders, List.of(2, 200)));
            ordersTable.insert(new Tuple(orders, List.of(3, 50)));

            // Query: join with equi-join + additional filter
            // SELECT u.id, o.amount FROM users u, orders o WHERE u.id = o.user_id AND u.age > 20 AND o.amount >= 100
            SqlParser parser = new SqlParser();
            Statement stmt = (Statement) parser.parse(
                "SELECT u.id, o.amount FROM users u, orders o WHERE u.id = o.user_id AND u.age > 20 AND o.amount >= 100"
            );
            
            Analyzer analyzer = new Analyzer();
            LogicalPlan logical = analyzer.analyze(stmt, cat, List.of());

            ExecContext ctx = new ExecContext(cat);
            PhysicalPlanner pp = new PhysicalPlanner();
            PhysicalOperator root = pp.plan(logical, ctx);
            
            root.open();
            List<Tuple> out = new ArrayList<>();
            for (Tuple t = root.next(); t != null; t = root.next()) out.add(t);
            root.close();

            // Expected: (1,100), (2,200) - user 3 is excluded by age > 20
            assertEquals(2, out.size());
            
            for (Tuple row : out) {
                int userId = (Integer) row.get(0);
                int amount = (Integer) row.get(1);
                assertTrue(userId == 1 || userId == 2);
                assertTrue(amount >= 100);
            }
        }
    }
}
