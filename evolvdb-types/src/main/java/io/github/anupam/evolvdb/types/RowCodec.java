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
 * Encoding (little-endian):
 *  - INT:    4 bytes
 *  - BIGINT: 8 bytes
 *  - BOOLEAN: 1 byte (0=false,1=true)
 *  - FLOAT:  4 bytes (IEEE-754)
 *  - STRING/VARCHAR: [u16 byteLen][UTF-8 bytes]
 */
public final class RowCodec {
    private RowCodec() {}

    public static byte[] encode(Schema schema, Tuple tuple) {
        Objects.requireNonNull(schema, "schema");
        Objects.requireNonNull(tuple, "tuple");
        if (tuple.schema() != schema) {
            // require same instance to avoid accidental mismatch; caller can pass exact schema used to build tuple
            throw new IllegalArgumentException("Tuple is not bound to provided Schema instance");
        }
        int size = computeSize(schema, tuple.values());
        ByteBuffer buf = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
        write(schema, tuple.values(), buf);
        return buf.array();
    }

    public static Tuple decode(Schema schema, byte[] bytes) {
        Objects.requireNonNull(schema, "schema");
        Objects.requireNonNull(bytes, "bytes");
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        Object[] vals = new Object[schema.size()];
        for (int i = 0; i < schema.size(); i++) {
            ColumnMeta col = schema.columns().get(i);
            vals[i] = readOne(col, buf);
        }
        return new Tuple(schema, Arrays.asList(vals));
    }

    public static String toDebugString(Tuple t) {
        StringBuilder sb = new StringBuilder();
        sb.append('(');
        for (int i = 0; i < t.schema().size(); i++) {
            if (i > 0) sb.append(", ");
            Object v = t.get(i);
            if (v instanceof String s) sb.append('\'').append(s).append('\'');
            else sb.append(v);
        }
        sb.append(')');
        return sb.toString();
    }

    private static int computeSize(Schema schema, List<Object> values) {
        int total = 0;
        for (int i = 0; i < schema.size(); i++) {
            ColumnMeta col = schema.columns().get(i);
            Object v = values.get(i);
            switch (col.type()) {
                case INT -> total += 4;
                case BIGINT -> total += 8;
                case BOOLEAN -> total += 1;
                case FLOAT -> total += 4;
                case STRING, VARCHAR -> {
                    String s = (String) v;
                    int bytesLen = s.getBytes(StandardCharsets.UTF_8).length;
                    if (bytesLen > 0xFFFF) throw new IllegalArgumentException("string too large to encode");
                    total += 2 + bytesLen;
                }
                default -> throw new IllegalStateException("Unsupported type: " + col.type());
            }
        }
        return total;
    }

    private static void write(Schema schema, List<Object> values, ByteBuffer buf) {
        for (int i = 0; i < schema.size(); i++) {
            ColumnMeta col = schema.columns().get(i);
            Object v = values.get(i);
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
}
