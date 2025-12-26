package io.github.anupam.evolvdb.exec;

import java.util.ArrayList;
import java.util.List;

import io.github.anupam.evolvdb.catalog.CatalogManager;
import io.github.anupam.evolvdb.catalog.Table;
import io.github.anupam.evolvdb.config.DbConfig;
import io.github.anupam.evolvdb.exec.op.PhysicalOperator;
import io.github.anupam.evolvdb.planner.analyzer.Binder;
import io.github.anupam.evolvdb.planner.logical.LogicalPlan;
import io.github.anupam.evolvdb.sql.ast.Statement;
import io.github.anupam.evolvdb.sql.parser.SqlParser;
import io.github.anupam.evolvdb.sql.validate.AstValidator;
import io.github.anupam.evolvdb.storage.buffer.DefaultBufferPool;
import io.github.anupam.evolvdb.storage.disk.NioDiskManager;
import io.github.anupam.evolvdb.storage.page.SlottedPageFormat;
import io.github.anupam.evolvdb.types.ColumnMeta;
import io.github.anupam.evolvdb.types.Schema;
import io.github.anupam.evolvdb.types.Tuple;
import io.github.anupam.evolvdb.types.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.*;

class UpdateDeleteExecTest {

    @TempDir java.nio.file.Path tempDir;

    private CatalogManager catalog;
    private AstValidator validator;
    private Binder binder;
    private PhysicalPlanner planner;

    @BeforeEach
    void setUp() throws Exception {
        DbConfig config = DbConfig.builder().pageSize(4096).dataDir(tempDir).build();
        NioDiskManager disk = new NioDiskManager(config);
        DefaultBufferPool buffer = new DefaultBufferPool(config, disk);
        SlottedPageFormat format = new SlottedPageFormat();
        catalog = new CatalogManager(disk, buffer, format);
        validator = new AstValidator();
        binder = new Binder();
        planner = new PhysicalPlanner();

        Schema schema =
                new Schema(
                        List.of(
                                new ColumnMeta("id", Type.INT, null),
                                new ColumnMeta("name", Type.VARCHAR, 50),
                                new ColumnMeta("age", Type.INT, null),
                                new ColumnMeta("active", Type.BOOLEAN, null)));

        catalog.createTable("users", schema);

        Table table = catalog.openTable("users");
        table.insert(new Tuple(schema, List.of(1, "Alice", 25, true)));
        table.insert(new Tuple(schema, List.of(2, "Bob", 17, true)));
        table.insert(new Tuple(schema, List.of(3, "Charlie", 30, false)));
        table.insert(new Tuple(schema, List.of(4, "Diana", 22, true)));
    }

    @Test
    void givenUpdateSingleRow_whenExecute_thenRowUpdated() throws Exception {
        String sql = "UPDATE users SET age = 26 WHERE id = 1";

        Tuple result = executeStatement(sql);

        assertNotNull(result);
        assertEquals(1, result.get(0));

        Table table = catalog.openTable("users");
        List<Tuple> rows = collectRows(table);
        Tuple updatedRow = findRowById(rows, 1);
        assertNotNull(updatedRow);
        assertEquals(26, updatedRow.get(2));
    }

    @Test
    void givenUpdateMultipleColumns_whenExecute_thenAllColumnsUpdated() throws Exception {
        String sql = "UPDATE users SET name = 'Alicia', age = 26, active = false WHERE id = 1";

        Tuple result = executeStatement(sql);

        assertEquals(1, result.get(0));

        Table table = catalog.openTable("users");
        List<Tuple> rows = collectRows(table);
        Tuple updatedRow = findRowById(rows, 1);
        assertEquals("Alicia", updatedRow.get(1));
        assertEquals(26, updatedRow.get(2));
        assertEquals(false, updatedRow.get(3));
    }

    @Test
    void givenUpdateMultipleRows_whenExecute_thenAllMatchingRowsUpdated() throws Exception {
        String sql = "UPDATE users SET active = false WHERE age > 20";

        Tuple result = executeStatement(sql);

        assertEquals(3, result.get(0));

        Table table = catalog.openTable("users");
        List<Tuple> rows = collectRows(table);
        for (Tuple row : rows) {
            int age = (int) row.get(2);
            boolean active = (boolean) row.get(3);
            if (age > 20) {
                assertFalse(active);
            }
        }
    }

    @Test
    void givenUpdateWithExpression_whenExecute_thenExpressionEvaluated() throws Exception {
        String sql = "UPDATE users SET age = age + 1 WHERE id = 1";

        Tuple result = executeStatement(sql);

        assertEquals(1, result.get(0));

        Table table = catalog.openTable("users");
        List<Tuple> rows = collectRows(table);
        Tuple updatedRow = findRowById(rows, 1);
        assertEquals(26, updatedRow.get(2));
    }

    @Test
    void givenUpdateNoMatchingRows_whenExecute_thenZeroUpdated() throws Exception {
        String sql = "UPDATE users SET age = 100 WHERE id = 999";

        Tuple result = executeStatement(sql);

        assertEquals(0, result.get(0));
    }

    @Test
    void givenUpdateAllRows_whenExecute_thenAllRowsUpdated() throws Exception {
        String sql = "UPDATE users SET active = false";

        Tuple result = executeStatement(sql);

        assertEquals(4, result.get(0));

        Table table = catalog.openTable("users");
        List<Tuple> rows = collectRows(table);
        for (Tuple row : rows) {
            assertFalse((boolean) row.get(3));
        }
    }

    @Test
    void givenDeleteSingleRow_whenExecute_thenRowDeleted() throws Exception {
        String sql = "DELETE FROM users WHERE id = 1";

        Tuple result = executeStatement(sql);

        assertEquals(1, result.get(0));

        Table table = catalog.openTable("users");
        List<Tuple> rows = collectRows(table);
        assertEquals(3, rows.size());
        assertNull(findRowById(rows, 1));
    }

    @Test
    void givenDeleteMultipleRows_whenExecute_thenAllMatchingRowsDeleted() throws Exception {
        String sql = "DELETE FROM users WHERE age < 20";

        Tuple result = executeStatement(sql);

        assertEquals(1, result.get(0));

        Table table = catalog.openTable("users");
        List<Tuple> rows = collectRows(table);
        assertEquals(3, rows.size());
        assertNull(findRowById(rows, 2));
    }

    @Test
    void givenDeleteNoMatchingRows_whenExecute_thenZeroDeleted() throws Exception {
        String sql = "DELETE FROM users WHERE id = 999";

        Tuple result = executeStatement(sql);

        assertEquals(0, result.get(0));

        Table table = catalog.openTable("users");
        List<Tuple> rows = collectRows(table);
        assertEquals(4, rows.size());
    }

    @Test
    void givenDeleteAllRows_whenExecute_thenAllRowsDeleted() throws Exception {
        String sql = "DELETE FROM users";

        Tuple result = executeStatement(sql);

        assertEquals(4, result.get(0));

        Table table = catalog.openTable("users");
        List<Tuple> rows = collectRows(table);
        assertEquals(0, rows.size());
    }

    @Test
    void givenDeleteWithComplexWhere_whenExecute_thenCorrectRowsDeleted() throws Exception {
        String sql = "DELETE FROM users WHERE age > 20 AND active = true";

        Tuple result = executeStatement(sql);

        assertEquals(2, result.get(0));

        Table table = catalog.openTable("users");
        List<Tuple> rows = collectRows(table);
        assertEquals(2, rows.size());
    }

    @Test
    void givenUpdateThenSelect_whenExecute_thenChangesVisible() throws Exception {
        executeStatement("UPDATE users SET age = 100 WHERE id = 1");

        String selectSql = "SELECT id, age FROM users WHERE id = 1";
        SqlParser parser = new SqlParser();
        Statement stmt = (Statement) parser.parse(selectSql);
        validator.validate(stmt, catalog);
        LogicalPlan logicalPlan = binder.bind(stmt, catalog);
        PhysicalOperator operator = planner.plan(logicalPlan, new ExecContext(catalog));

        operator.open();
        Tuple row = operator.next();
        operator.close();

        assertNotNull(row);
        assertEquals(1, row.get(0));
        assertEquals(100, row.get(1));
    }

    @Test
    void givenDeleteThenSelect_whenExecute_thenRowNotFound() throws Exception {
        executeStatement("DELETE FROM users WHERE id = 1");

        String selectSql = "SELECT id FROM users WHERE id = 1";
        SqlParser parser = new SqlParser();
        Statement stmt = (Statement) parser.parse(selectSql);
        validator.validate(stmt, catalog);
        LogicalPlan logicalPlan = binder.bind(stmt, catalog);
        PhysicalOperator operator = planner.plan(logicalPlan, new ExecContext(catalog));

        operator.open();
        Tuple row = operator.next();
        operator.close();

        assertNull(row);
    }

    private Tuple executeStatement(String sql) throws Exception {
        SqlParser parser = new SqlParser();
        Statement stmt = (Statement) parser.parse(sql);
        validator.validate(stmt, catalog);
        LogicalPlan logicalPlan = binder.bind(stmt, catalog);
        PhysicalOperator operator = planner.plan(logicalPlan, new ExecContext(catalog));

        operator.open();
        Tuple result = operator.next();
        operator.close();

        return result;
    }

    private List<Tuple> collectRows(Table table) {
        List<Tuple> rows = new ArrayList<>();
        for (Tuple tuple : table.scanTuples()) {
            rows.add(tuple);
        }
        return rows;
    }

    private Tuple findRowById(List<Tuple> rows, int id) {
        for (Tuple row : rows) {
            if ((int) row.get(0) == id) {
                return row;
            }
        }
        return null;
    }
}
