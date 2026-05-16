package io.github.anupam.evolvdb.types;

import java.util.List;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RowCodecTest {

    @Test
    void givenTuple_whenEncodeDecode_thenRoundTrip() {
        Schema schema =
                new Schema(
                        List.of(
                                new ColumnMeta("id", Type.INT, null),
                                new ColumnMeta("name", Type.STRING, null)));
        Tuple t = new Tuple(schema, List.of(1, "Alice"));
        byte[] bytes = RowCodec.encode(schema, t);
        Tuple out = RowCodec.decode(schema, bytes);
        assertEquals(t.values(), out.values());
    }

    @Test
    void givenDifferentSchemaInstance_whenEncode_thenThrows() {
        Schema s1 = new Schema(List.of(new ColumnMeta("id", Type.INT, null)));
        Schema s2 = new Schema(List.of(new ColumnMeta("id", Type.INT, null)));
        Tuple t = new Tuple(s1, List.of(42));
        assertThrows(IllegalArgumentException.class, () -> RowCodec.encode(s2, t));
    }

    @Test
    void givenVarcharTooLong_whenConstructTuple_thenThrows() {
        Schema schema = new Schema(List.of(new ColumnMeta("name", Type.VARCHAR, 3)));
        assertThrows(IllegalArgumentException.class, () -> new Tuple(schema, List.of("long")));
    }

    @Test
    void givenNullValue_whenEncodeDecode_thenRoundTrips() {
        Schema schema =
                new Schema(
                        List.of(
                                new ColumnMeta("id", Type.INT, null),
                                new ColumnMeta("name", Type.STRING, null),
                                new ColumnMeta("age", Type.INT, null)));
        List<Object> values = new java.util.ArrayList<>();
        values.add(1);
        values.add(null);
        values.add(30);
        Tuple t = new Tuple(schema, values);
        byte[] bytes = RowCodec.encode(schema, t);
        Tuple out = RowCodec.decode(schema, bytes);
        assertEquals(1, out.get(0));
        assertNull(out.get(1));
        assertEquals(30, out.get(2));
    }

    @Test
    void givenAllNulls_whenEncodeDecode_thenAllNullRoundTrips() {
        Schema schema =
                new Schema(
                        List.of(
                                new ColumnMeta("a", Type.INT, null),
                                new ColumnMeta("b", Type.STRING, null)));
        List<Object> values = new java.util.ArrayList<>();
        values.add(null);
        values.add(null);
        Tuple t = new Tuple(schema, values);
        byte[] bytes = RowCodec.encode(schema, t);
        Tuple out = RowCodec.decode(schema, bytes);
        assertNull(out.get(0));
        assertNull(out.get(1));
    }

    @Test
    void givenNotNullColumn_whenInsertNull_thenThrows() {
        Schema schema = new Schema(List.of(new ColumnMeta("id", Type.INT, null, false)));
        List<Object> values = new java.util.ArrayList<>();
        values.add(null);
        assertThrows(IllegalArgumentException.class, () -> new Tuple(schema, values));
    }

    @Test
    void givenNoNulls_whenV1EncodeDecode_thenStillRoundTrips() {
        Schema schema =
                new Schema(
                        List.of(
                                new ColumnMeta("id", Type.INT, null),
                                new ColumnMeta("name", Type.VARCHAR, 10),
                                new ColumnMeta("flag", Type.BOOLEAN, null)));
        Tuple t = new Tuple(schema, List.of(42, "test", true));
        byte[] bytes = RowCodec.encode(schema, t);
        Tuple out = RowCodec.decode(schema, bytes);
        assertEquals(t.values(), out.values());
    }
}
