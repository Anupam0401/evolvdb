package io.github.anupam.evolvdb.exec.op;

import io.github.anupam.evolvdb.types.Schema;
import io.github.anupam.evolvdb.types.Tuple;

/** Volcano-style operator. */
public interface PhysicalOperator {
    void open() throws Exception;
    Tuple next() throws Exception; // returns null when exhausted
    void close() throws Exception;
    Schema schema();
}
