package io.github.anupam.evolvdb.catalog;

import java.io.IOException;
import java.util.Iterator;

import io.github.anupam.evolvdb.storage.page.RecordId;
import io.github.anupam.evolvdb.storage.record.HeapFile;
import io.github.anupam.evolvdb.types.RowCodec;
import io.github.anupam.evolvdb.types.Schema;
import io.github.anupam.evolvdb.types.Tuple;

/** High-level table handle providing tuple-oriented operations backed by a HeapFile. */
public final class Table {
    private final TableMeta meta;
    private final HeapFile heapFile;

    Table(TableMeta meta, HeapFile heapFile) {
        this.meta = meta;
        this.heapFile = heapFile;
    }

    public TableMeta meta() {
        return meta;
    }

    public Schema schema() {
        return meta.schema();
    }

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
        return () ->
                new Iterator<>() {
                    final Iterator<byte[]> it = heapFile.scan().iterator();

                    @Override
                    public boolean hasNext() {
                        return it.hasNext();
                    }

                    @Override
                    public Tuple next() {
                        return RowCodec.decode(meta.schema(), it.next());
                    }
                };
    }

    public Iterable<TupleWithRecordId> scanTuplesWithRecordIds() {
        return () ->
                new Iterator<>() {
                    final Iterator<RecordId> it = heapFile.iterator();

                    @Override
                    public boolean hasNext() {
                        return it.hasNext();
                    }

                    @Override
                    public TupleWithRecordId next() {
                        RecordId rid = it.next();
                        try {
                            byte[] bytes = heapFile.read(rid);
                            Tuple tuple = RowCodec.decode(meta.schema(), bytes);
                            return new TupleWithRecordId(tuple, rid);
                        } catch (Exception e) {
                            throw new IllegalStateException("Failed to read tuple", e);
                        }
                    }
                };
    }

    public static final class TupleWithRecordId {
        public final Tuple tuple;
        public final RecordId recordId;

        public TupleWithRecordId(Tuple tuple, RecordId recordId) {
            this.tuple = tuple;
            this.recordId = recordId;
        }
    }

    public void delete(RecordId rid) throws IOException {
        heapFile.delete(rid);
    }
}
