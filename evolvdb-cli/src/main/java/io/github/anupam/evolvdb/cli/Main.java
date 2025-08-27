package io.github.anupam.evolvdb.cli;

import io.github.anupam.evolvdb.config.DbConfig;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

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
