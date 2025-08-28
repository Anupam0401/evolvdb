package io.github.anupam.evolvdb.catalog;

import io.github.anupam.evolvdb.storage.record.HeapFile;
import io.github.anupam.evolvdb.storage.page.RecordId;
import io.github.anupam.evolvdb.types.RowCodec;
import io.github.anupam.evolvdb.types.Schema;
import io.github.anupam.evolvdb.types.Tuple;

import java.io.IOException;
import java.util.Iterator;

/**
 * High-level table handle providing tuple-oriented operations backed by a HeapFile.
 */
public final class Table {
    private final TableMeta meta;
    private final HeapFile heapFile;

    Table(TableMeta meta, HeapFile heapFile) {
        this.meta = meta;
        this.heapFile = heapFile;
    }

    public TableMeta meta() { return meta; }
    public Schema schema() { return meta.schema(); }

    public RecordId insert(Tuple tuple) throws IOException {
        byte[] bytes = RowCodec.encode(meta.schema(), tuple);
        return heapFile.insert(bytes);
    }

    public Tuple read(RecordId rid) throws IOException {
        byte[] bytes = heapFile.read(rid);
        return RowCodec.decode(meta.schema(), bytes);
    }

    public RecordId update(RecordId rid, Tuple tuple) throws IOException {
        byte[] bytes = RowCodec.encode(meta.schema(), tuple);
        return heapFile.update(rid, bytes);
    }

    public Iterable<Tuple> scanTuples() {
        return () -> new Iterator<>() {
            final Iterator<byte[]> it = heapFile.scan().iterator();
            @Override public boolean hasNext() { return it.hasNext(); }
            @Override public Tuple next() { return RowCodec.decode(meta.schema(), it.next()); }
        };
    }
}
