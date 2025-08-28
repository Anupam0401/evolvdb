package io.github.anupam.evolvdb.types;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RowCodecTest {

    @Test
    void givenTuple_whenEncodeDecode_thenRoundTrip() {
        Schema schema = new Schema(List.of(
                new ColumnMeta("id", Type.INT, null),
                new ColumnMeta("name", Type.STRING, null)
        ));
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
}
