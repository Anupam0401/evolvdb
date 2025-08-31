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

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AggregateExecTest {
    private Path tmpDir;

    private Database db() throws Exception {
        tmpDir = Files.createTempDirectory("evolvdb-exec-agg-");
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
    void group_by_count_executes() throws Exception {
        try (Database db = db()) {
            CatalogManager cat = db.catalog();
            Schema orders = new Schema(List.of(
                    new ColumnMeta("user_id", Type.INT, null),
                    new ColumnMeta("amount", Type.INT, null)
            ));
            cat.createTable("orders", orders);
            var t = cat.openTable("orders");
            t.insert(new Tuple(orders, List.of(1, 10)));
            t.insert(new Tuple(orders, List.of(1, 20)));
            t.insert(new Tuple(orders, List.of(2, 5)));

            SqlParser parser = new SqlParser();
            Statement stmt = (Statement) parser.parse("SELECT user_id, COUNT(*) AS cnt FROM orders GROUP BY user_id");
            Analyzer analyzer = new Analyzer();
            LogicalPlan logical = analyzer.analyze(stmt, cat, List.of());

            ExecContext ctx = new ExecContext(cat);
            PhysicalPlanner pp = new PhysicalPlanner();
            PhysicalOperator root = pp.plan(logical, ctx);
            root.open();
            List<Tuple> out = new ArrayList<>();
            for (Tuple row = root.next(); row != null; row = root.next()) out.add(row);
            root.close();

            // Expect 2 groups: (1,2) and (2,1) in input order of grouping keys
            assertEquals(2, out.size());
            // We don't enforce order of groups; check by scanning
            int seen1 = 0, seen2 = 0;
            for (Tuple row : out) {
                int userId = (Integer) row.get(0);
                long cnt = (Long) row.get(1);
                if (userId == 1) { assertEquals(2L, cnt); seen1++; }
                if (userId == 2) { assertEquals(1L, cnt); seen2++; }
            }
            assertEquals(1, seen1);
            assertEquals(1, seen2);
        }
    }
}
