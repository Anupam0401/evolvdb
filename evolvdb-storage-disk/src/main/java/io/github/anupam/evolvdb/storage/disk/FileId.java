package io.github.anupam.evolvdb.storage.disk;

/** Identifies a physical table file by a logical name (resolved by DiskManager). */
public record FileId(String name) {
    public FileId {
        if (name == null || name.isBlank()) throw new IllegalArgumentException("name must be non-empty");
    }
}
