package io.github.anupam.evolvdb.exec;

import io.github.anupam.evolvdb.catalog.CatalogManager;
import io.github.anupam.evolvdb.config.DbConfig;
import io.github.anupam.evolvdb.core.Database;
import io.github.anupam.evolvdb.exec.op.PhysicalOperator;
import io.github.anupam.evolvdb.exec.PhysicalPlanner;
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

public class EndToEndExecTest {
    private Path tmpDir;

    private Database db() throws Exception {
        tmpDir = Files.createTempDirectory("evolvdb-exec-");
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
    void select_filter_executes() throws Exception {
        try (Database db = db()) {
            CatalogManager cat = db.catalog();
            Schema users = new Schema(List.of(
                    new ColumnMeta("id", Type.INT, null),
                    new ColumnMeta("age", Type.INT, null)
            ));
            cat.createTable("users", users);
            var table = cat.openTable("users");
            table.insert(new Tuple(users, List.of(1, 42)));
            table.insert(new Tuple(users, List.of(2, 18)));

            SqlParser parser = new SqlParser();
            Statement stmt = (Statement) parser.parse("SELECT id FROM users WHERE age > 20");
            Analyzer analyzer = new Analyzer();
            LogicalPlan logical = analyzer.analyze(stmt, cat, List.of());

            ExecContext ctx = new ExecContext(cat);
            PhysicalPlanner pp = new PhysicalPlanner();
            PhysicalOperator root = pp.plan(logical, ctx);
            root.open();
            List<Tuple> out = new ArrayList<>();
            for (Tuple t = root.next(); t != null; t = root.next()) out.add(t);
            root.close();

            assertEquals(1, out.size());
            assertEquals(1, out.getFirst().get(0));
        }
    }
}
