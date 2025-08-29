package io.github.anupam.evolvdb.sql.validate;

import io.github.anupam.evolvdb.catalog.CatalogManager;
import io.github.anupam.evolvdb.config.DbConfig;
import io.github.anupam.evolvdb.core.Database;
import io.github.anupam.evolvdb.sql.ast.AstNode;
import io.github.anupam.evolvdb.sql.parser.SqlParser;
import io.github.anupam.evolvdb.types.ColumnMeta;
import io.github.anupam.evolvdb.types.Schema;
import io.github.anupam.evolvdb.types.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class AstValidatorTest {
    private Path tmpDir;

    private Database db() throws Exception {
        tmpDir = Files.createTempDirectory("evolvdb-sql-");
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
    void givenCreateTableWithDuplicateCols_whenValidate_thenError() throws Exception {
        Database db = db();
        try (db) {
            var parser = new SqlParser();
            var validator = new AstValidator();
            String sql = "CREATE TABLE t (a INT, a INT)";
            AstNode ast = parser.parse(sql);
            assertThrows(IllegalArgumentException.class, () -> validator.validate(ast, db.catalog()));
        }
    }

    @Test
    void givenInsertValuesMatch_whenValidate_thenOk() throws Exception {
        try (Database db = db()) {
            CatalogManager cat = db.catalog();
            Schema schema = new Schema(List.of(
                    new ColumnMeta("id", Type.INT, null),
                    new ColumnMeta("name", Type.VARCHAR, 5)
            ));
            cat.createTable("users", schema);

            var parser = new SqlParser();
            var validator = new AstValidator();
            AstNode ast = parser.parse("INSERT INTO users VALUES (1, 'Alice')");
            assertDoesNotThrow(() -> validator.validate(ast, cat));
        }
    }

    @Test
    void givenInsertWrongArity_whenValidate_thenError() throws Exception {
        try (Database db = db()) {
            CatalogManager cat = db.catalog();
            Schema schema = new Schema(List.of(
                    new ColumnMeta("id", Type.INT, null),
                    new ColumnMeta("name", Type.STRING, null)
            ));
            cat.createTable("users", schema);

            var parser = new SqlParser();
            var validator = new AstValidator();
            AstNode ast = parser.parse("INSERT INTO users VALUES (1)");
            assertThrows(IllegalArgumentException.class, () -> validator.validate(ast, cat));
        }
    }

    @Test
    void givenInsertWrongType_whenValidate_thenError() throws Exception {
        try (Database db = db()) {
            CatalogManager cat = db.catalog();
            Schema schema = new Schema(List.of(
                    new ColumnMeta("id", Type.INT, null),
                    new ColumnMeta("name", Type.STRING, null)
            ));
            cat.createTable("users", schema);

            var parser = new SqlParser();
            var validator = new AstValidator();
            AstNode ast = parser.parse("INSERT INTO users VALUES ('oops', 'Alice')");
            assertThrows(IllegalArgumentException.class, () -> validator.validate(ast, cat));
        }
    }

    @Test
    void givenSelectUnknownColumn_whenValidate_thenError() throws Exception {
        try (Database db = db()) {
            CatalogManager cat = db.catalog();
            Schema schema = new Schema(List.of(
                    new ColumnMeta("id", Type.INT, null),
                    new ColumnMeta("name", Type.STRING, null)
            ));
            cat.createTable("users", schema);

            var parser = new SqlParser();
            var validator = new AstValidator();
            AstNode ast = parser.parse("SELECT foo FROM users");
            assertThrows(IllegalArgumentException.class, () -> validator.validate(ast, cat));
        }
    }

    @Test
    void givenSelectWithAliasWrongQualifier_whenValidate_thenError() throws Exception {
        try (Database db = db()) {
            CatalogManager cat = db.catalog();
            Schema schema = new Schema(List.of(
                    new ColumnMeta("id", Type.INT, null),
                    new ColumnMeta("name", Type.STRING, null)
            ));
            cat.createTable("users", schema);

            var parser = new SqlParser();
            var validator = new AstValidator();
            AstNode ast = parser.parse("SELECT u.id FROM users AS t");
            assertThrows(IllegalArgumentException.class, () -> validator.validate(ast, cat));
        }
    }

    @Test
    void givenDropTableUnknown_whenValidate_thenError() throws Exception {
        try (Database db = db()) {
            CatalogManager cat = db.catalog();
            var parser = new SqlParser();
            var validator = new AstValidator();
            AstNode ast = parser.parse("DROP TABLE nope");
            assertThrows(IllegalArgumentException.class, () -> validator.validate(ast, cat));
        }
    }
}
