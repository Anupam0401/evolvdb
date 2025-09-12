package io.github.anupam.evolvdb.exec.op;

import io.github.anupam.evolvdb.types.Schema;
import io.github.anupam.evolvdb.types.Tuple;
import io.github.anupam.evolvdb.optimizer.Cost;

/** Volcano-style operator. */
public interface PhysicalOperator {
    void open() throws Exception;
    Tuple next() throws Exception; // returns null when exhausted
    void close() throws Exception;
    Schema schema();

    /** Optional: estimated cost for this operator subtree (M11). */
    default Cost estimatedCost() { return Cost.of(-1, 0, 0); }
    default double estimatedRowCount() { return estimatedCost().rowCount(); }
}
