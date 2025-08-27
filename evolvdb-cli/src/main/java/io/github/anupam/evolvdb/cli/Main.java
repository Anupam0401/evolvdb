package io.github.anupam.evolvdb.cli;

import io.github.anupam.evolvdb.config.DbConfig;
import io.github.anupam.evolvdb.core.Database;
import io.github.anupam.evolvdb.storage.page.SlottedPageFormat;
import io.github.anupam.evolvdb.storage.record.RecordManager;
import io.github.anupam.evolvdb.storage.record.HeapFile;
import io.github.anupam.evolvdb.storage.page.RecordId;
import io.github.anupam.evolvdb.types.ColumnMeta;
import io.github.anupam.evolvdb.types.Schema;
import io.github.anupam.evolvdb.types.Type;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.List;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

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

        // Minimal demo: create Database, open a HeapFile via RecordManager, insert and read back
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

            var rm = new RecordManager(db.disk(), db.buffer());
            var fmt = new SlottedPageFormat();
            HeapFile users = rm.openHeapFile(meta.fileId().name(), fmt);

            // Insert two rows
            RecordId u1 = users.insert(RowCodec.encode(1, "Alice"));
            RecordId u2 = users.insert(RowCodec.encode(2, "Bob"));
            System.out.println("Inserted into users: " + u1 + ", " + u2);

            System.out.println("Scan users:");
            for (byte[] rec : users.scan()) {
                System.out.println("  - " + RowCodec.toString(rec));
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

    // Simple row codec for demo: [int id][short nameLen][name bytes UTF-8]
    static final class RowCodec {
        static byte[] encode(int id, String name) {
            byte[] nb = name.getBytes(StandardCharsets.UTF_8);
            ByteBuffer buf = ByteBuffer.allocate(4 + 2 + nb.length).order(ByteOrder.LITTLE_ENDIAN);
            buf.putInt(id);
            buf.putShort((short) nb.length);
            buf.put(nb);
            return buf.array();
        }
        static String toString(byte[] bytes) {
            ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
            int id = buf.getInt();
            int nlen = Short.toUnsignedInt(buf.getShort());
            byte[] nb = new byte[nlen];
            buf.get(nb);
            return "(" + id + ", '" + new String(nb, StandardCharsets.UTF_8) + "')";
        }
    }
}
