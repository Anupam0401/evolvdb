package io.github.anupam.evolvdb.exec;

import java.util.List;
import java.util.stream.Stream;

import io.github.anupam.evolvdb.types.ColumnMeta;
import io.github.anupam.evolvdb.types.Schema;
import io.github.anupam.evolvdb.types.Tuple;
import io.github.anupam.evolvdb.types.Type;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class QueryGatherersTest {

    private static final Schema SCHEMA =
            new Schema(
                    List.of(
                            new ColumnMeta("id", Type.INT, null),
                            new ColumnMeta("name", Type.STRING, null)));

    private static Tuple row(int id, String name) {
        return new Tuple(SCHEMA, List.of(id, name));
    }

    @Test
    void givenRows_whenLimitOffset_thenSkipsAndLimits() {
        List<Tuple> input =
                List.of(row(1, "a"), row(2, "b"), row(3, "c"), row(4, "d"), row(5, "e"));

        List<Tuple> result = input.stream().gather(QueryGatherers.limitOffset(2, 1)).toList();

        assertEquals(2, result.size());
        assertEquals(2, result.get(0).get(0));
        assertEquals(3, result.get(1).get(0));
    }

    @Test
    void givenRows_whenLimitOffsetExceedsSize_thenReturnsRemaining() {
        List<Tuple> input = List.of(row(1, "a"), row(2, "b"), row(3, "c"));

        List<Tuple> result = input.stream().gather(QueryGatherers.limitOffset(10, 2)).toList();

        assertEquals(1, result.size());
        assertEquals(3, result.get(0).get(0));
    }

    @Test
    void givenDuplicates_whenDistinctBy_thenDeduplicates() {
        List<Tuple> input = List.of(row(1, "alice"), row(2, "alice"), row(3, "bob"), row(4, "bob"));

        List<Tuple> result =
                input.stream().gather(QueryGatherers.distinctBy(t -> t.get(1))).toList();

        assertEquals(2, result.size());
        assertEquals("alice", result.get(0).get(1));
        assertEquals("bob", result.get(1).get(1));
    }

    @Test
    void givenRows_whenRunningCount_thenEmitsSequentialNumbers() {
        List<Tuple> input = List.of(row(10, "x"), row(20, "y"), row(30, "z"));

        List<Long> result = input.stream().gather(QueryGatherers.runningCount()).toList();

        assertEquals(List.of(1L, 2L, 3L), result);
    }

    @Test
    void givenEmptyStream_whenLimitOffset_thenEmpty() {
        List<Tuple> result =
                Stream.<Tuple>empty().gather(QueryGatherers.limitOffset(5, 0)).toList();
        assertTrue(result.isEmpty());
    }
}
