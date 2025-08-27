package io.github.anupam.evolvdb.cli;

import io.github.anupam.evolvdb.config.DbConfig;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public final class Main {
    public static void main(String[] args) throws IOException {
        DbConfig config = DbConfig.builder()
                .pageSize(4096)
                .dataDir(Path.of("data"))
                .bufferPoolPages(256)
                .build();

        Files.createDirectories(config.dataDir());

        System.out.println("EvolvDB starting with config: " + config);
        System.out.println("Data directory: " + config.dataDir().toAbsolutePath());
    }
}
