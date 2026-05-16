package io.github.anupam.evolvdb.core;

import java.util.List;

import io.github.anupam.evolvdb.types.Schema;
import io.github.anupam.evolvdb.types.Tuple;

/**
 * Result of executing a SQL statement. For DML/DDL, rows may be empty with a status message. For
 * SELECT, rows contains the result tuples and schema describes the output columns.
 */
public final class QueryResult {
    private final Schema schema;
    private final List<Tuple> rows;
    private final String message;

    private QueryResult(Schema schema, List<Tuple> rows, String message) {
        this.schema = schema;
        this.rows = rows;
        this.message = message;
    }

    public static QueryResult ofRows(Schema schema, List<Tuple> rows) {
        return new QueryResult(schema, List.copyOf(rows), null);
    }

    public static QueryResult ofMessage(String message) {
        return new QueryResult(null, List.of(), message);
    }

    public Schema schema() {
        return schema;
    }

    public List<Tuple> rows() {
        return rows;
    }

    public String message() {
        return message;
    }

    public boolean hasRows() {
        return schema != null && !rows.isEmpty();
    }
}
