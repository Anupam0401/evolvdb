package io.github.anupam.evolvdb.core;

import io.github.anupam.evolvdb.config.DbConfig;
import io.github.anupam.evolvdb.storage.buffer.BufferPool;
import io.github.anupam.evolvdb.storage.buffer.DefaultBufferPool;
import io.github.anupam.evolvdb.storage.disk.DiskManager;
import io.github.anupam.evolvdb.storage.disk.NioDiskManager;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;

/**
 * Database is the facade and composition root for core services.
 * For now only wires DiskManager and BufferPool.
 */
public final class Database implements Closeable {
    private final DbConfig config;
    private final DiskManager diskManager;
    private final BufferPool bufferPool;

    public Database(DbConfig config) throws IOException {
        this.config = Objects.requireNonNull(config, "config");
        this.diskManager = new NioDiskManager(config);
        this.bufferPool = new DefaultBufferPool(config, diskManager);
    }

    public DbConfig config() { return config; }
    public DiskManager disk() { return diskManager; }
    public BufferPool buffer() { return bufferPool; }

    @Override
    public void close() throws IOException {
        bufferPool.close();
        diskManager.close();
    }
}
