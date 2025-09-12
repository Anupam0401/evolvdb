package io.github.anupam.evolvdb.optimizer;

import io.github.anupam.evolvdb.catalog.CatalogManager;
import io.github.anupam.evolvdb.config.DbConfig;
import io.github.anupam.evolvdb.core.Database;
import io.github.anupam.evolvdb.exec.ExecContext;
import io.github.anupam.evolvdb.exec.PhysicalPlanner;
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

public class OptimizerE2ETest {
    private Path tmpDir;

    private Database db() throws Exception {
        tmpDir = Files.createTempDirectory("evolvdb-opt-");
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
    void optimizer_selects_baseline_join_and_executes() throws Exception {
        try (Database db = db()) {
            CatalogManager cat = db.catalog();
            // users(id, name)
            Schema users = new Schema(List.of(
                    new ColumnMeta("id", Type.INT, null),
                    new ColumnMeta("name", Type.STRING, null)
            ));
            cat.createTable("users", users);
            var ut = cat.openTable("users");
            ut.insert(new Tuple(users, List.of(1, "Alice")));
            ut.insert(new Tuple(users, List.of(2, "Bob")));

            // orders(order_id, user_id, amount)
            Schema orders = new Schema(List.of(
                    new ColumnMeta("order_id", Type.INT, null),
                    new ColumnMeta("user_id", Type.INT, null),
                    new ColumnMeta("amount", Type.INT, null)
            ));
            cat.createTable("orders", orders);
            var ot = cat.openTable("orders");
            ot.insert(new Tuple(orders, List.of(10, 1, 50)));
            ot.insert(new Tuple(orders, List.of(11, 1, 75)));
            ot.insert(new Tuple(orders, List.of(12, 2, 20)));

            // Query with join
            SqlParser parser = new SqlParser();
            Statement stmt = (Statement) parser.parse(
                "SELECT u.name, o.amount FROM users u, orders o WHERE u.id = o.user_id"
            );
            Analyzer analyzer = new Analyzer();
            LogicalPlan logical = analyzer.analyze(stmt, cat, List.of());

            ExecContext ctx = new ExecContext(cat, true); // use optimizer
            PhysicalPlanner pp = new PhysicalPlanner();
            PhysicalOperator root = pp.plan(logical, ctx);

            root.open();
            List<Tuple> out = new ArrayList<>();
            for (Tuple t = root.next(); t != null; t = root.next()) out.add(t);
            root.close();

            assertEquals(3, out.size());
            // Validate names present
            int aliceCnt = 0, bobCnt = 0;
            for (Tuple t : out) {
                String name = (String) t.get(0);
                int amt = (Integer) t.get(1);
                if ("Alice".equals(name)) aliceCnt++;
                if ("Bob".equals(name)) bobCnt++;
                assertTrue(amt == 50 || amt == 75 || amt == 20);
            }
            assertEquals(2, aliceCnt);
            assertEquals(1, bobCnt);
        }
    }
}
