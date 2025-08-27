package io.github.anupam.evolvdb.config;

import java.nio.file.Path;
import java.util.Objects;

/**
 * Database configuration. Immutable, built via Builder.
 * Keep this minimal for v0 and evolve as subsystems are added.
 */
public final class DbConfig {
    private final int pageSize;
    private final Path dataDir;
    private final int bufferPoolPages;

    private DbConfig(Builder b) {
        this.pageSize = b.pageSize;
        this.dataDir = b.dataDir;
        this.bufferPoolPages = b.bufferPoolPages;
    }

    public int pageSize() { return pageSize; }
    public Path dataDir() { return dataDir; }
    public int bufferPoolPages() { return bufferPoolPages; }

    @Override
    public String toString() {
        return "DbConfig{" +
                "pageSize=" + pageSize +
                ", dataDir=" + dataDir +
                ", bufferPoolPages=" + bufferPoolPages +
                '}';
    }

    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        private int pageSize = 4096;
        private Path dataDir = Path.of("data");
        private int bufferPoolPages = 256;

        public Builder pageSize(int pageSize) {
            if (pageSize <= 0) throw new IllegalArgumentException("pageSize must be > 0");
            this.pageSize = pageSize; return this;
        }
        public Builder dataDir(Path dataDir) {
            this.dataDir = Objects.requireNonNull(dataDir, "dataDir"); return this;
        }
        public Builder bufferPoolPages(int bufferPoolPages) {
            if (bufferPoolPages <= 0) throw new IllegalArgumentException("bufferPoolPages must be > 0");
            this.bufferPoolPages = bufferPoolPages; return this;
        }
        public DbConfig build() { return new DbConfig(this); }
    }
}
