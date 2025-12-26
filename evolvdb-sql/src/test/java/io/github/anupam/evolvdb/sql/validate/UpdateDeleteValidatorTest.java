package io.github.anupam.evolvdb.sql.validate;

import java.util.List;

import io.github.anupam.evolvdb.catalog.CatalogManager;
import io.github.anupam.evolvdb.config.DbConfig;
import io.github.anupam.evolvdb.sql.ast.*;
import io.github.anupam.evolvdb.sql.parser.SqlParser;
import io.github.anupam.evolvdb.storage.buffer.DefaultBufferPool;
import io.github.anupam.evolvdb.storage.disk.NioDiskManager;
import io.github.anupam.evolvdb.storage.page.SlottedPageFormat;
import io.github.anupam.evolvdb.types.ColumnMeta;
import io.github.anupam.evolvdb.types.Schema;
import io.github.anupam.evolvdb.types.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.*;

class UpdateDeleteValidatorTest {

    @TempDir java.nio.file.Path tempDir;

    private CatalogManager catalog;
    private AstValidator validator;

    @BeforeEach
    void setUp() throws Exception {
        DbConfig config = DbConfig.builder().pageSize(4096).dataDir(tempDir).build();
        NioDiskManager disk = new NioDiskManager(config);
        DefaultBufferPool buffer = new DefaultBufferPool(config, disk);
        SlottedPageFormat format = new SlottedPageFormat();
        catalog = new CatalogManager(disk, buffer, format);
        validator = new AstValidator();

        Schema schema =
                new Schema(
                        List.of(
                                new ColumnMeta("id", Type.INT, null),
                                new ColumnMeta("name", Type.VARCHAR, 50),
                                new ColumnMeta("age", Type.INT, null),
                                new ColumnMeta("active", Type.BOOLEAN, null)));

        catalog.createTable("users", schema);
    }

    @Test
    void givenValidUpdate_whenValidate_thenSuccess() {
        String sql = "UPDATE users SET age = 30 WHERE id = 1";
        SqlParser parser = new SqlParser();
        Statement stmt = (Statement) parser.parse(sql);

        assertDoesNotThrow(() -> validator.validate(stmt, catalog));
    }

    @Test
    void givenUpdateUnknownTable_whenValidate_thenThrowsException() {
        String sql = "UPDATE nonexistent SET age = 30";
        SqlParser parser = new SqlParser();
        Statement stmt = (Statement) parser.parse(sql);

        assertThrows(RuntimeException.class, () -> validator.validate(stmt, catalog));
    }

    @Test
    void givenUpdateUnknownColumn_whenValidate_thenThrowsException() {
        String sql = "UPDATE users SET unknown_col = 30";
        SqlParser parser = new SqlParser();
        Statement stmt = (Statement) parser.parse(sql);

        Exception exception =
                assertThrows(RuntimeException.class, () -> validator.validate(stmt, catalog));
        assertTrue(exception.getMessage().contains("Unknown column"));
    }

    @Test
    void givenUpdateInvalidWhereColumn_whenValidate_thenThrowsException() {
        String sql = "UPDATE users SET age = 30 WHERE invalid_col = 1";
        SqlParser parser = new SqlParser();
        Statement stmt = (Statement) parser.parse(sql);

        Exception exception =
                assertThrows(RuntimeException.class, () -> validator.validate(stmt, catalog));
        assertTrue(exception.getMessage().contains("Unknown column"));
    }

    @Test
    void givenUpdateMultipleValidColumns_whenValidate_thenSuccess() {
        String sql = "UPDATE users SET age = 30, name = 'Alice', active = true";
        SqlParser parser = new SqlParser();
        Statement stmt = (Statement) parser.parse(sql);

        assertDoesNotThrow(() -> validator.validate(stmt, catalog));
    }

    @Test
    void givenUpdateWithExpression_whenValidate_thenSuccess() {
        String sql = "UPDATE users SET age = age + 1";
        SqlParser parser = new SqlParser();
        Statement stmt = (Statement) parser.parse(sql);

        assertDoesNotThrow(() -> validator.validate(stmt, catalog));
    }

    @Test
    void givenValidDelete_whenValidate_thenSuccess() {
        String sql = "DELETE FROM users WHERE age < 18";
        SqlParser parser = new SqlParser();
        Statement stmt = (Statement) parser.parse(sql);

        assertDoesNotThrow(() -> validator.validate(stmt, catalog));
    }

    @Test
    void givenDeleteUnknownTable_whenValidate_thenThrowsException() {
        String sql = "DELETE FROM nonexistent WHERE id = 1";
        SqlParser parser = new SqlParser();
        Statement stmt = (Statement) parser.parse(sql);

        assertThrows(RuntimeException.class, () -> validator.validate(stmt, catalog));
    }

    @Test
    void givenDeleteInvalidWhereColumn_whenValidate_thenThrowsException() {
        String sql = "DELETE FROM users WHERE invalid_col = 1";
        SqlParser parser = new SqlParser();
        Statement stmt = (Statement) parser.parse(sql);

        Exception exception =
                assertThrows(RuntimeException.class, () -> validator.validate(stmt, catalog));
        assertTrue(exception.getMessage().contains("Unknown column"));
    }

    @Test
    void givenDeleteWithoutWhere_whenValidate_thenSuccess() {
        String sql = "DELETE FROM users";
        SqlParser parser = new SqlParser();
        Statement stmt = (Statement) parser.parse(sql);

        assertDoesNotThrow(() -> validator.validate(stmt, catalog));
    }

    @Test
    void givenUpdateCaseInsensitiveColumns_whenValidate_thenSuccess() {
        String sql = "UPDATE users SET AGE = 30, NAME = 'Alice'";
        SqlParser parser = new SqlParser();
        Statement stmt = (Statement) parser.parse(sql);

        assertDoesNotThrow(() -> validator.validate(stmt, catalog));
    }

    @Test
    void givenDeleteComplexWhere_whenValidate_thenSuccess() {
        String sql = "DELETE FROM users WHERE age > 18 AND active = false";
        SqlParser parser = new SqlParser();
        Statement stmt = (Statement) parser.parse(sql);

        assertDoesNotThrow(() -> validator.validate(stmt, catalog));
    }
}
