package io.github.anupam.evolvdb.cli;

import io.github.anupam.evolvdb.config.DbConfig;
import io.github.anupam.evolvdb.core.Database;
import io.github.anupam.evolvdb.catalog.Table;
import io.github.anupam.evolvdb.types.Tuple;
import io.github.anupam.evolvdb.types.ColumnMeta;
import io.github.anupam.evolvdb.types.Schema;
import io.github.anupam.evolvdb.types.Type;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.List;

public final class Main {
    public static void main(String[] args) throws IOException {
        Path dataDir = resolveDataDir();
        DbConfig config = DbConfig.builder()
                .pageSize(4096)
                .dataDir(dataDir)
                .bufferPoolPages(256)
                .build();

        Files.createDirectories(config.dataDir());

        System.out.println("EvolvDB starting with config: " + config);
        System.out.println("Data directory: " + config.dataDir().toAbsolutePath());

        // Minimal demo: create Database, use Catalog to create/open a table, insert tuples and scan
        try (Database db = new Database(config)) {
            // M6: Catalog demo: create table users(id INT, name STRING), insert and scan
            var schema = new Schema(List.of(
                    new ColumnMeta("id", Type.INT, null),
                    new ColumnMeta("name", Type.STRING, null)
            ));
            var cat = db.catalog();
            var meta = cat.getTable("users").orElseGet(() -> {
                try { cat.createTable("users", schema); } catch (IOException e) { throw new RuntimeException(e); }
                return cat.getTable("users").orElseThrow();
            });
            Table users = cat.openTable(meta.id());

            // Insert two tuples
            var t1 = new Tuple(users.schema(), List.of(1, "Alice"));
            var t2 = new Tuple(users.schema(), List.of(2, "Bob"));
            var r1 = users.insert(t1);
            var r2 = users.insert(t2);
            System.out.println("Inserted into users: " + r1 + ", " + r2);

            System.out.println("Scan users:");
            for (var tup : users.scanTuples()) {
                System.out.println("  - " + tup.values());
            }
        }
    }

    /**
     * Resolves data directory in priority order:
     * 1) System property -Devolvdb.dataDir
     * 2) Env var EVOLVDB_DATA_DIR
     * 3) Fallback: ./data under the repository root (detected by walking up to a dir containing settings.gradle.kts or gradlew)
     */
    private static Path resolveDataDir() {
        String prop = System.getProperty("evolvdb.dataDir");
        if (prop != null && !prop.isBlank()) return Path.of(prop);
        String env = System.getenv("EVOLVDB_DATA_DIR");
        if (env != null && !env.isBlank()) return Path.of(env);
        Path repoRoot = findRepoRoot(Path.of("").toAbsolutePath());
        return repoRoot.resolve("data");
    }

    private static Path findRepoRoot(Path start) {
        Path cur = Objects.requireNonNull(start);
        while (cur != null) {
            if (Files.exists(cur.resolve("settings.gradle.kts")) || Files.exists(cur.resolve("gradlew"))) {
                return cur;
            }
            cur = cur.getParent();
        }
        // Fallback to current dir if no markers found
        return start;
    }
}
