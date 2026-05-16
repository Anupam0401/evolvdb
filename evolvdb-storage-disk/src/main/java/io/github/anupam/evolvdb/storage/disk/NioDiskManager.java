package io.github.anupam.evolvdb.storage.disk;

import io.github.anupam.evolvdb.common.DbException;
import io.github.anupam.evolvdb.config.DbConfig;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * NIO-based DiskManager. Provides page-level I/O with fixed page size using MemorySegment.
 */
public final class NioDiskManager implements DiskManager {
    private final DbConfig config;
    private final int pageSize;
    private final Map<FileId, FileChannel> openFiles = new ConcurrentHashMap<>();

    public NioDiskManager(DbConfig config) throws IOException {
        this.config = Objects.requireNonNull(config, "config");
        this.pageSize = config.pageSize();
        Files.createDirectories(config.dataDir());
    }

    @Override
    public PageId allocatePage(FileId fileId) throws IOException {
        var ch = openOrCreate(fileId);
        synchronized (ch) {
            long size = ch.size();
            int newPageNo = (int) (size / pageSize);
            long pos = (long) newPageNo * pageSize;
            ByteBuffer zero = ByteBuffer.allocate(pageSize);
            writeFully(ch, pos, zero);
            return new PageId(fileId, newPageNo);
        }
    }

    @Override
    public void readPage(PageId pageId, MemorySegment dst) throws IOException {
        if (dst.byteSize() < pageSize) {
            throw new IllegalArgumentException("dst must have at least " + pageSize + " bytes");
        }
        var ch = openOrCreate(pageId.fileId());
        long pos = (long) pageId.pageNo() * pageSize;
        ByteBuffer buf = dst.asByteBuffer();
        buf.clear().limit(pageSize);
        readFully(ch, pos, buf, pageSize);
    }

    @Override
    public void writePage(PageId pageId, MemorySegment src, long lsn) throws IOException {
        if (src.byteSize() < pageSize) {
            throw new IllegalArgumentException("src must have at least " + pageSize + " bytes");
        }
        var ch = openOrCreate(pageId.fileId());
        long pos = (long) pageId.pageNo() * pageSize;
        ByteBuffer buf = src.asByteBuffer();
        buf.clear().limit(pageSize);
        writeFully(ch, pos, buf);
    }

    @Override
    public void sync() throws IOException {
        IOException first = null;
        for (var ch : openFiles.values()) {
            try {
                ch.force(true);
            } catch (IOException e) {
                if (first == null) first = e;
            }
        }
        if (first != null) throw first;
    }

    @Override
    public int pageCount(FileId fileId) throws IOException {
        var ch = openOrCreate(fileId);
        synchronized (ch) {
            long size = ch.size();
            return (int) (size / pageSize);
        }
    }

    @Override
    public void close() throws IOException {
        IOException first = null;
        for (var entry : openFiles.entrySet()) {
            try {
                entry.getValue().close();
            } catch (IOException e) {
                if (first == null) first = e;
            }
        }
        openFiles.clear();
        if (first != null) throw first;
    }

    private FileChannel openOrCreate(FileId fileId) throws IOException {
        return openFiles.computeIfAbsent(
            fileId,
            id -> {
                try {
                    Path p = resolvePath(id);
                    return FileChannel.open(
                        p,
                        EnumSet.of(
                            StandardOpenOption.CREATE,
                            StandardOpenOption.READ,
                            StandardOpenOption.WRITE));
                } catch (IOException e) {
                    throw new DbException("Failed to open file: " + id.name(), e);
                }
            });
    }

    private Path resolvePath(FileId fileId) {
        String fileName =
            fileId.name().endsWith(".evolv") ? fileId.name() : fileId.name() + ".evolv";
        return config.dataDir().resolve(fileName);
    }

    private static void writeFully(FileChannel ch, long pos, ByteBuffer src) throws IOException {
        int toWrite = src.remaining();
        int written = 0;
        while (written < toWrite) {
            int n = ch.write(src, pos + written);
            if (n < 0) throw new IOException("Unexpected EOF while writing");
            written += n;
        }
    }

    private static void readFully(FileChannel ch, long pos, ByteBuffer dst, int len)
        throws IOException {
        int read = 0;
        while (read < len) {
            int n = ch.read(dst, pos + read);
            if (n < 0) throw new IOException("Unexpected EOF while reading");
            read += n;
        }
    }
}
