package io.github.anupam.evolvdb.types;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * RowCodec encodes/decodes a Tuple bound to a Schema into a compact binary form.
 *
 * <p>Format v1 (current): [version=0x01][null-bitmap][non-null values]
 *
 * <p>The null bitmap uses ceil(N/8) bytes where N is the number of columns. Bit i = 1 means column
 * i is NULL and its bytes are omitted from the payload.
 *
 * <p>Format v0 (legacy, read-only): [values back-to-back, no bitmap] — records written before M13.
 *
 * <p>Value encoding (little-endian): INT: 4 bytes, BIGINT: 8 bytes, BOOLEAN: 1 byte, FLOAT: 4 bytes
 * (IEEE-754), STRING/VARCHAR: [u16 byteLen][UTF-8 bytes].
 */
public final class RowCodec {

    private static final byte FORMAT_V1 = 0x01;

    private RowCodec() {}

    public static byte[] encode(Schema schema, Tuple tuple) {
        Objects.requireNonNull(schema, "schema");
        Objects.requireNonNull(tuple, "tuple");
        if (tuple.schema() != schema) {
            throw new IllegalArgumentException("Tuple is not bound to provided Schema instance");
        }
        List<Object> values = tuple.values();
        int bitmapLen = nullBitmapLength(schema.size());
        byte[] bitmap = new byte[bitmapLen];
        int payloadSize = 0;
        for (int i = 0; i < schema.size(); i++) {
            Object v = values.get(i);
            if (v == null) {
                setBit(bitmap, i);
            } else {
                payloadSize += valueSize(schema.columns().get(i), v);
            }
        }
        int total = 1 + bitmapLen + payloadSize;
        ByteBuffer buf = ByteBuffer.allocate(total).order(ByteOrder.LITTLE_ENDIAN);
        buf.put(FORMAT_V1);
        buf.put(bitmap);
        for (int i = 0; i < schema.size(); i++) {
            Object v = values.get(i);
            if (v != null) {
                writeOne(schema.columns().get(i), v, buf);
            }
        }
        return buf.array();
    }

    /**
     * All rows written by the current encode() start with FORMAT_V1. Legacy rows (written before
     * M13) have no version prefix and are decoded as v0. We distinguish them by checking whether
     * the first byte equals FORMAT_V1 <em>and</em> the total length is consistent with the v1
     * header overhead (version byte + bitmap). A v0 row whose first data byte happens to be 0x01
     * will almost certainly fail the length consistency check, making false positives
     * near-impossible for schemas with more than one column.
     */
    public static Tuple decode(Schema schema, byte[] bytes) {
        Objects.requireNonNull(schema, "schema");
        Objects.requireNonNull(bytes, "bytes");
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        int v1HeaderSize = 1 + nullBitmapLength(schema.size());
        if (bytes.length >= v1HeaderSize && bytes[0] == FORMAT_V1) {
            return decodeV1(schema, buf);
        }
        return decodeV0(schema, buf);
    }

    public static String toDebugString(Tuple t) {
        StringBuilder sb = new StringBuilder();
        sb.append('(');
        for (int i = 0; i < t.schema().size(); i++) {
            if (i > 0) sb.append(", ");
            Object v = t.get(i);
            if (v == null) sb.append("NULL");
            else if (v instanceof String s) sb.append('\'').append(s).append('\'');
            else sb.append(v);
        }
        sb.append(')');
        return sb.toString();
    }

    // ── v1 decode (version byte + null bitmap + non-null values) ──

    private static Tuple decodeV1(Schema schema, ByteBuffer buf) {
        buf.get(); // consume version byte
        int bitmapLen = nullBitmapLength(schema.size());
        byte[] bitmap = new byte[bitmapLen];
        buf.get(bitmap);
        Object[] vals = new Object[schema.size()];
        for (int i = 0; i < schema.size(); i++) {
            if (isBitSet(bitmap, i)) {
                vals[i] = null;
            } else {
                vals[i] = readOne(schema.columns().get(i), buf);
            }
        }
        return new Tuple(schema, Arrays.asList(vals));
    }

    // ── v0 decode (legacy: no version byte, all values back-to-back) ──

    private static Tuple decodeV0(Schema schema, ByteBuffer buf) {
        Object[] vals = new Object[schema.size()];
        for (int i = 0; i < schema.size(); i++) {
            vals[i] = readOne(schema.columns().get(i), buf);
        }
        return new Tuple(schema, Arrays.asList(vals));
    }

    // ── single-value read/write ──

    private static void writeOne(ColumnMeta col, Object v, ByteBuffer buf) {
        switch (col.type()) {
            case INT -> buf.putInt((Integer) v);
            case BIGINT -> buf.putLong((Long) v);
            case BOOLEAN -> buf.put((byte) ((Boolean) v ? 1 : 0));
            case FLOAT -> buf.putFloat((Float) v);
            case STRING, VARCHAR -> {
                byte[] nb = ((String) v).getBytes(StandardCharsets.UTF_8);
                buf.putShort((short) nb.length);
                buf.put(nb);
            }
            default -> throw new IllegalStateException("Unsupported type: " + col.type());
        }
    }

    private static Object readOne(ColumnMeta col, ByteBuffer buf) {
        return switch (col.type()) {
            case INT -> buf.getInt();
            case BIGINT -> buf.getLong();
            case BOOLEAN -> buf.get() != 0;
            case FLOAT -> buf.getFloat();
            case STRING, VARCHAR -> {
                int len = Short.toUnsignedInt(buf.getShort());
                byte[] nb = new byte[len];
                buf.get(nb);
                yield new String(nb, StandardCharsets.UTF_8);
            }
            default -> throw new IllegalStateException("Unsupported type: " + col.type());
        };
    }

    private static int valueSize(ColumnMeta col, Object v) {
        return switch (col.type()) {
            case INT -> 4;
            case BIGINT -> 8;
            case BOOLEAN -> 1;
            case FLOAT -> 4;
            case STRING, VARCHAR -> {
                byte[] nb = ((String) v).getBytes(StandardCharsets.UTF_8);
                if (nb.length > 0xFFFF)
                    throw new IllegalArgumentException("string too large to encode");
                yield 2 + nb.length;
            }
            default -> throw new IllegalStateException("Unsupported type: " + col.type());
        };
    }

    // ── null bitmap helpers ──

    private static int nullBitmapLength(int columnCount) {
        return (columnCount + 7) / 8;
    }

    private static void setBit(byte[] bitmap, int index) {
        bitmap[index / 8] |= (byte) (1 << (index % 8));
    }

    private static boolean isBitSet(byte[] bitmap, int index) {
        return (bitmap[index / 8] & (1 << (index % 8))) != 0;
    }
}
