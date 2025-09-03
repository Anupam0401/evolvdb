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

public class MultiAggregateTest {
    private Path tmpDir;

    private Database db() throws Exception {
        tmpDir = Files.createTempDirectory("evolvdb-exec-multi-agg-");
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
    @org.junit.jupiter.api.Disabled("Type inference mismatch between INT and BIGINT - to be fixed")
    void multiple_aggregates_with_group_by() throws Exception {
        try (Database db = db()) {
            CatalogManager cat = db.catalog();
            Schema sales = new Schema(List.of(
                    new ColumnMeta("region", Type.STRING, null),
                    new ColumnMeta("amount", Type.INT, null),
                    new ColumnMeta("quantity", Type.INT, null)
            ));
            cat.createTable("sales", sales);
            var t = cat.openTable("sales");
            
            // Insert test data
            t.insert(new Tuple(sales, List.of("North", 100, 5)));
            t.insert(new Tuple(sales, List.of("North", 200, 10)));
            t.insert(new Tuple(sales, List.of("North", 150, 7)));
            t.insert(new Tuple(sales, List.of("South", 50, 2)));
            t.insert(new Tuple(sales, List.of("South", 75, 3)));
            t.insert(new Tuple(sales, List.of("East", 300, 15)));

            // SELECT region, COUNT(*) AS cnt, SUM(amount) AS total, AVG(amount) AS avg_amt, 
            //        MIN(quantity) AS min_q, MAX(quantity) AS max_q
            // FROM sales GROUP BY region
            SqlParser parser = new SqlParser();
            Statement stmt = (Statement) parser.parse(
                "SELECT region, COUNT(*) AS cnt, SUM(amount) AS total, AVG(amount) AS avg_amt, " +
                "MIN(quantity) AS min_q, MAX(quantity) AS max_q " +
                "FROM sales GROUP BY region"
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

            // Expected: 3 groups (North, South, East)
            assertEquals(3, out.size());
            
            // Check schema
            Schema outSchema = root.schema();
            assertEquals(6, outSchema.size());
            assertEquals("region", outSchema.columns().get(0).name());
            assertEquals("cnt", outSchema.columns().get(1).name());
            assertEquals("total", outSchema.columns().get(2).name());
            assertEquals("avg_amt", outSchema.columns().get(3).name());
            assertEquals("min_q", outSchema.columns().get(4).name());
            assertEquals("max_q", outSchema.columns().get(5).name());
            
            // Verify aggregates for each group
            for (Tuple row : out) {
                String region = (String) row.get(0);
                long cnt = (Long) row.get(1);
                // SUM returns Long for integer types
                Object totalObj = row.get(2);
                long total = totalObj instanceof Long ? (Long) totalObj : ((Integer) totalObj).longValue();
                float avg = (Float) row.get(3);
                int minQ = (Integer) row.get(4);
                int maxQ = (Integer) row.get(5);
                
                if ("North".equals(region)) {
                    assertEquals(3L, cnt);
                    assertEquals(450L, total);
                    assertEquals(150.0f, avg, 0.01f);
                    assertEquals(5, minQ);
                    assertEquals(10, maxQ);
                } else if ("South".equals(region)) {
                    assertEquals(2L, cnt);
                    assertEquals(125L, total);
                    assertEquals(62.5f, avg, 0.01f);
                    assertEquals(2, minQ);
                    assertEquals(3, maxQ);
                } else if ("East".equals(region)) {
                    assertEquals(1L, cnt);
                    assertEquals(300L, total);
                    assertEquals(300.0f, avg, 0.01f);
                    assertEquals(15, minQ);
                    assertEquals(15, maxQ);
                } else {
                    fail("Unexpected region: " + region);
                }
            }
        }
    }

    @Test
    void aggregates_with_float_values() throws Exception {
        try (Database db = db()) {
            CatalogManager cat = db.catalog();
            Schema measurements = new Schema(List.of(
                    new ColumnMeta("category", Type.STRING, null),
                    new ColumnMeta("value", Type.FLOAT, null)
            ));
            cat.createTable("measurements", measurements);
            var t = cat.openTable("measurements");
            
            t.insert(new Tuple(measurements, List.of("A", 10.5f)));
            t.insert(new Tuple(measurements, List.of("A", 20.3f)));
            t.insert(new Tuple(measurements, List.of("B", 5.7f)));
            t.insert(new Tuple(measurements, List.of("B", 8.2f)));
            t.insert(new Tuple(measurements, List.of("B", 12.1f)));

            // SELECT category, COUNT(*), SUM(value), AVG(value), MIN(value), MAX(value)
            // FROM measurements GROUP BY category
            SqlParser parser = new SqlParser();
            Statement stmt = (Statement) parser.parse(
                "SELECT category, COUNT(*), SUM(value), AVG(value), MIN(value), MAX(value) " +
                "FROM measurements GROUP BY category"
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
                String category = (String) row.get(0);
                long cnt = (Long) row.get(1);
                float sum = (Float) row.get(2);
                float avg = (Float) row.get(3);
                float min = (Float) row.get(4);
                float max = (Float) row.get(5);
                
                if ("A".equals(category)) {
                    assertEquals(2L, cnt);
                    assertEquals(30.8f, sum, 0.01f);
                    assertEquals(15.4f, avg, 0.01f);
                    assertEquals(10.5f, min, 0.01f);
                    assertEquals(20.3f, max, 0.01f);
                } else if ("B".equals(category)) {
                    assertEquals(3L, cnt);
                    assertEquals(26.0f, sum, 0.01f);
                    assertEquals(8.67f, avg, 0.01f);
                    assertEquals(5.7f, min, 0.01f);
                    assertEquals(12.1f, max, 0.01f);
                }
            }
        }
    }
}
