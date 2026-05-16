package io.github.anupam.evolvdb.exec.op;

import io.github.anupam.evolvdb.optimizer.Cost;
import io.github.anupam.evolvdb.storage.page.RecordId;
import io.github.anupam.evolvdb.types.Schema;
import io.github.anupam.evolvdb.types.Tuple;

/** Volcano-style operator. */
public interface PhysicalOperator {
    void open() throws Exception;

    Tuple next() throws Exception; // returns null when exhausted

    void close() throws Exception;

    Schema schema();

    /**
     * Returns the RecordId of the tuple most recently returned by {@link #next()}. Only meaningful
     * for operators that produce tuples directly from a heap file (e.g. SeqScanWithRidExec) or that
     * delegate transparently (e.g. FilterExec). Returns null by default.
     */
    default RecordId lastRecordId() {
        return null;
    }

    /** Optional: estimated cost for this operator subtree (M11). */
    default Cost estimatedCost() {
        return Cost.of(-1, 0, 0);
    }

    default double estimatedRowCount() {
        return estimatedCost().rowCount();
    }
}
