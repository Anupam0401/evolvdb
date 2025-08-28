package io.github.anupam.evolvdb.catalog;

import io.github.anupam.evolvdb.config.DbConfig;
import io.github.anupam.evolvdb.core.Database;
import io.github.anupam.evolvdb.types.ColumnMeta;
import io.github.anupam.evolvdb.types.Schema;
import io.github.anupam.evolvdb.types.Tuple;
import io.github.anupam.evolvdb.types.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TableTupleIntegrationTest {
    private Path tmpDir;

    private DbConfig cfg() throws IOException {
        tmpDir = Files.createTempDirectory("evolvdb-tuple-");
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

    @Test
    void givenTable_whenInsertAndReadTuples_thenRoundTrip() throws Exception {
        var config = cfg();
        try (var db = new Database(config)) {
            var schema = new Schema(List.of(
                    new ColumnMeta("id", Type.INT, null),
                    new ColumnMeta("name", Type.STRING, null)
            ));
            var cat = db.catalog();
            var id = cat.createTable("people", schema);
            Table table = cat.openTable(id);

            Tuple t1 = new Tuple(table.schema(), List.of(1, "Alice"));
            Tuple t2 = new Tuple(table.schema(), List.of(2, "Bob"));
            var r1 = table.insert(t1);
            var r2 = table.insert(t2);

            // Read back directly
            assertEquals(List.of(1, "Alice"), table.read(r1).values());
            assertEquals(List.of(2, "Bob"), table.read(r2).values());

            // Scan
            var scanned = table.scanTuples();
            int count = 0;
            for (Tuple t : scanned) {
                assertNotNull(t);
                count++;
            }
            assertEquals(2, count);
        }
    }
}
