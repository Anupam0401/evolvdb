package io.github.anupam.evolvdb.core;

import io.github.anupam.evolvdb.config.DbConfig;
import io.github.anupam.evolvdb.storage.buffer.BufferPool;
import io.github.anupam.evolvdb.storage.buffer.DefaultBufferPool;
import io.github.anupam.evolvdb.storage.disk.DiskManager;
import io.github.anupam.evolvdb.storage.disk.NioDiskManager;
import io.github.anupam.evolvdb.catalog.CatalogManager;
import io.github.anupam.evolvdb.storage.page.SlottedPageFormat;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;

/**
 * Database is the facade and composition root for core services.
 * Wires DiskManager, BufferPool, and CatalogManager.
 */
public final class Database implements Closeable {
    private final DbConfig config;
    private final DiskManager diskManager;
    private final BufferPool bufferPool;
    private final CatalogManager catalogManager;

    public Database(DbConfig config) throws IOException {
        this.config = Objects.requireNonNull(config, "config");
        this.diskManager = new NioDiskManager(config);
        this.bufferPool = new DefaultBufferPool(config, diskManager);
        // Use SlottedPageFormat for system catalog heap file
        this.catalogManager = new CatalogManager(diskManager, bufferPool, new SlottedPageFormat());
    }

    public DbConfig config() { return config; }
    public DiskManager disk() { return diskManager; }
    public BufferPool buffer() { return bufferPool; }
    public CatalogManager catalog() { return catalogManager; }

    @Override
    public void close() throws IOException {
        bufferPool.close();
        diskManager.close();
    }
}
