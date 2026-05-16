package io.github.anupam.evolvdb.cli;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

import io.github.anupam.evolvdb.config.DbConfig;
import io.github.anupam.evolvdb.core.Database;
import io.github.anupam.evolvdb.core.QueryResult;
import io.github.anupam.evolvdb.types.ColumnMeta;
import io.github.anupam.evolvdb.types.Tuple;

public final class Main {

    public static void main(String[] args) throws IOException {
        Path dataDir = resolveDataDir();
        DbConfig config =
                DbConfig.builder().pageSize(4096).dataDir(dataDir).bufferPoolPages(256).build();

        Files.createDirectories(config.dataDir());

        System.out.println("EvolvDB v1.0 — Interactive SQL Shell");
        System.out.println("Data directory: " + config.dataDir().toAbsolutePath());
        System.out.println("Type SQL statements (end with ;), or \\q to quit.\n");

        try (Database db = new Database(config);
                BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
            StringBuilder pending = new StringBuilder();
            while (true) {
                System.out.print(pending.isEmpty() ? "evolvdb> " : "      -> ");
                System.out.flush();
                String line = reader.readLine();
                if (line == null) break;

                String trimmed = line.trim();
                if (trimmed.equals("\\q") || trimmed.equalsIgnoreCase("quit")) break;
                if (trimmed.isEmpty()) continue;

                pending.append(line).append(' ');

                if (!trimmed.endsWith(";")) continue;

                String sql = pending.toString().trim();
                if (sql.endsWith(";")) sql = sql.substring(0, sql.length() - 1).trim();
                pending.setLength(0);

                if (sql.isEmpty()) continue;

                try {
                    QueryResult result = db.execute(sql);
                    printResult(result);
                } catch (Exception e) {
                    System.out.println("ERROR: " + e.getMessage());
                }
            }
        }

        System.out.println("Bye.");
    }

    private static void printResult(QueryResult result) {
        if (result.message() != null) {
            System.out.println(result.message());
            return;
        }
        if (result.schema() != null) {
            var cols = result.schema().columns();
            StringBuilder header = new StringBuilder();
            StringBuilder sep = new StringBuilder();
            for (int i = 0; i < cols.size(); i++) {
                ColumnMeta col = cols.get(i);
                String name = col.name();
                if (i > 0) {
                    header.append(" | ");
                    sep.append("-+-");
                }
                header.append(String.format("%-15s", name));
                sep.append("-".repeat(15));
            }
            System.out.println(header);
            System.out.println(sep);

            for (Tuple row : result.rows()) {
                StringBuilder rowStr = new StringBuilder();
                for (int i = 0; i < cols.size(); i++) {
                    if (i > 0) rowStr.append(" | ");
                    Object val = row.get(i);
                    rowStr.append(String.format("%-15s", val == null ? "NULL" : val));
                }
                System.out.println(rowStr);
            }

            System.out.println("(" + result.rows().size() + " rows)");
        }
    }

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
            if (Files.exists(cur.resolve("settings.gradle.kts"))
                    || Files.exists(cur.resolve("gradlew"))) {
                return cur;
            }
            cur = cur.getParent();
        }
        return start;
    }
}
