package io.github.anupam.evolvdb.exec;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Gatherer;

import io.github.anupam.evolvdb.types.Tuple;

/**
 * Custom Stream Gatherers (JEP 485) for database query result processing. These provide reusable
 * intermediate stream operations that go beyond map/filter/reduce.
 */
public final class QueryGatherers {

    private QueryGatherers() {}

    /**
     * LIMIT/OFFSET gatherer: skips the first {@code offset} elements, then emits at most {@code
     * limit} elements. Equivalent to SQL {@code LIMIT limit OFFSET offset}.
     */
    public static Gatherer<Tuple, ?, Tuple> limitOffset(long limit, long offset) {
        record State(long skipped, long emitted) {}
        return Gatherer.ofSequential(
                () -> new long[] {0, 0},
                (state, element, downstream) -> {
                    if (state[0] < offset) {
                        state[0]++;
                        return true;
                    }
                    if (state[1] >= limit) {
                        return false;
                    }
                    state[1]++;
                    return downstream.push(element);
                });
    }

    /**
     * Distinct-by gatherer: emits only elements whose key (extracted by keyFn) has not been seen
     * before. Useful for deduplication on a projection.
     */
    public static <K> Gatherer<Tuple, ?, Tuple> distinctBy(Function<Tuple, K> keyFn) {
        return Gatherer.ofSequential(
                HashSet<K>::new,
                (Set<K> seen, Tuple element, Gatherer.Downstream<? super Tuple> downstream) -> {
                    K key = keyFn.apply(element);
                    if (seen.add(key)) {
                        return downstream.push(element);
                    }
                    return true;
                });
    }

    /**
     * Running count gatherer: emits an incrementing count for each element processed. Useful for
     * ROW_NUMBER() style operations where you need to number rows as they stream through.
     */
    public static Gatherer<Tuple, ?, Long> runningCount() {
        return Gatherer.ofSequential(
                () -> new long[] {0},
                (long[] state, Tuple _, Gatherer.Downstream<? super Long> downstream) -> {
                    state[0]++;
                    return downstream.push(state[0]);
                });
    }
}
