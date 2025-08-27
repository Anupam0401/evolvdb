package io.github.anupam.evolvdb.catalog;

import io.github.anupam.evolvdb.config.DbConfig;
import io.github.anupam.evolvdb.core.Database;
import io.github.anupam.evolvdb.types.ColumnMeta;
import io.github.anupam.evolvdb.types.Schema;
import io.github.anupam.evolvdb.types.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CatalogManagerTest {
    private Path tmpDir;

    private DbConfig cfg() throws IOException {
        tmpDir = Files.createTempDirectory("evolvdb-cat-");
        return DbConfig.builder().pageSize(4096).dataDir(tmpDir).bufferPoolPages(16).build();
    }

    @AfterEach
    void cleanup() throws IOException {
        if (tmpDir != null && Files.exists(tmpDir)) {
            try (var paths = Files.walk(tmpDir)) {
                paths.sorted((a,b)->b.getNameCount()-a.getNameCount()).forEach(p -> {
                    try { Files.deleteIfExists(p); } catch (IOException ignored) {}
                });
            }
        }
    }

    private Schema usersSchema() {
        return new Schema(List.of(
                new ColumnMeta("id", Type.INT, null),
                new ColumnMeta("name", Type.STRING, null)
        ));
    }

    @Test
    void givenFreshDb_whenCreateTable_thenRetrievableByNameAndId() throws Exception {
        var config = cfg();
        try (var db = new Database(config)) {
            var cat = db.catalog();
            var id = cat.createTable("users", usersSchema());
            var metaByName = cat.getTable("users").orElseThrow();
            var metaById = cat.getTable(id).orElseThrow();
            assertEquals(metaByName.id(), id);
            assertEquals(metaByName, metaById);
            assertEquals("t_" + id.value(), metaByName.fileId().name());
        }
    }

    @Test
    void givenDuplicateName_whenCreate_thenThrows() throws Exception {
        var config = cfg();
        try (var db = new Database(config)) {
            var cat = db.catalog();
            cat.createTable("users", usersSchema());
            assertThrows(IllegalArgumentException.class, () -> cat.createTable("users", usersSchema()));
        }
    }

    @Test
    void givenDropTable_whenGet_thenAbsent() throws Exception {
        var config = cfg();
        try (var db = new Database(config)) {
            var cat = db.catalog();
            var id = cat.createTable("users", usersSchema());
            cat.dropTable(id);
            assertTrue(cat.getTable("users").isEmpty());
            assertTrue(cat.getTable(id).isEmpty());
        }
    }

    @Test
    void givenRestart_whenReload_thenTablesPersisted() throws Exception {
        var config = cfg();
        TableId tableId;
        {
            try (var db = new Database(config)) {
                var cat = db.catalog();
                tableId = cat.createTable("users", usersSchema());
            }
        }
        {
            try (var db = new Database(config)) {
                var cat = db.catalog();
                var meta = cat.getTable("users").orElseThrow();
                assertEquals(tableId, meta.id());
                assertEquals(2, meta.schema().size());
            }
        }
    }
}
