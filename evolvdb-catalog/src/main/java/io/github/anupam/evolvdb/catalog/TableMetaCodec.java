package io.github.anupam.evolvdb.catalog;

import io.github.anupam.evolvdb.storage.disk.FileId;
import io.github.anupam.evolvdb.types.ColumnMeta;
import io.github.anupam.evolvdb.types.Schema;
import io.github.anupam.evolvdb.types.Type;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** Binary codec for catalog records. Versioned for future migrations. */
final class TableMetaCodec {
    private static final short VERSION = 1;
    enum Kind { UPSERT((byte)1), DROP((byte)2); final byte b; Kind(byte b){this.b=b;} }

    static byte[] encodeUpsert(TableMeta meta) {
        byte[] name = meta.name().getBytes(StandardCharsets.UTF_8);
        byte[] file = meta.fileId().name().getBytes(StandardCharsets.UTF_8);
        int cols = meta.schema().size();
        int size = 2 /*ver*/ + 1 /*kind*/ + 8 /*id*/ + 2 + name.length + 2 /*col count*/;
        for (ColumnMeta c : meta.schema().columns()) {
            byte[] cn = c.name().getBytes(StandardCharsets.UTF_8);
            size += 2 + cn.length; // name
            size += 1;             // type id
            size += 4;             // varchar length (or -1)
        }
        size += 2 + file.length; // fileId
        ByteBuffer buf = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
        buf.putShort(VERSION);
        buf.put(Kind.UPSERT.b);
        buf.putLong(meta.id().value());
        buf.putShort((short) name.length).put(name);
        buf.putShort((short) cols);
        for (ColumnMeta c : meta.schema().columns()) {
            byte[] cn = c.name().getBytes(StandardCharsets.UTF_8);
            buf.putShort((short) cn.length).put(cn);
            buf.put((byte) c.type().ordinal());
            int vlen = (c.type() == Type.VARCHAR) ? c.length() : -1;
            buf.putInt(vlen);
        }
        buf.putShort((short) file.length).put(file);
        return buf.array();
    }

    static byte[] encodeDrop(TableId id) {
        int size = 2 + 1 + 8;
        ByteBuffer buf = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
        buf.putShort(VERSION);
        buf.put(Kind.DROP.b);
        buf.putLong(id.value());
        return buf.array();
    }

    static Decoded decode(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        short ver = buf.getShort();
        if (ver != VERSION) throw new IllegalArgumentException("Unsupported catalog record version: " + ver);
        byte kind = buf.get();
        if (kind == Kind.DROP.b) {
            long id = buf.getLong();
            return new Decoded(new TableId(id), null, true);
        } else if (kind == Kind.UPSERT.b) {
            long id = buf.getLong();
            int nlen = Short.toUnsignedInt(buf.getShort());
            byte[] nb = new byte[nlen]; buf.get(nb);
            String name = new String(nb, StandardCharsets.UTF_8);
            int colCount = Short.toUnsignedInt(buf.getShort());
            List<ColumnMeta> cols = new ArrayList<>(colCount);
            for (int i = 0; i < colCount; i++) {
                int cnl = Short.toUnsignedInt(buf.getShort());
                byte[] cnb = new byte[cnl]; buf.get(cnb);
                String cn = new String(cnb, StandardCharsets.UTF_8);
                int typeOrdinal = Byte.toUnsignedInt(buf.get());
                Type t = Type.values()[typeOrdinal];
                int vlen = buf.getInt();
                Integer len = (t == Type.VARCHAR) ? vlen : null;
                cols.add(new ColumnMeta(cn, t, len));
            }
            int fil = Short.toUnsignedInt(buf.getShort());
            byte[] fnb = new byte[fil]; buf.get(fnb);
            String file = new String(fnb, StandardCharsets.UTF_8);
            TableMeta meta = new TableMeta(new TableId(id), name, new Schema(cols), new FileId(file));
            return new Decoded(meta.id(), meta, false);
        } else {
            throw new IllegalArgumentException("Unknown catalog record kind: " + kind);
        }
    }

    static final class Decoded {
        final TableId id; final TableMeta meta; final boolean drop;
        Decoded(TableId id, TableMeta meta, boolean drop){this.id=id;this.meta=meta;this.drop=drop;}
    }
}
