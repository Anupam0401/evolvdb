package io.github.anupam.evolvdb.common;

/**
 * Immutable transaction context propagated via ScopedValue through the execution stack. This is the
 * foundation for M17 (Transactions). Currently carries a transaction ID; will be extended with
 * isolation level, read/write sets, and commit/abort state.
 */
public record TransactionContext(long transactionId, IsolationLevel isolationLevel) {

    public enum IsolationLevel {
        READ_COMMITTED,
        REPEATABLE_READ,
        SERIALIZABLE
    }

    public static TransactionContext implicit() {
        return new TransactionContext(0, IsolationLevel.READ_COMMITTED);
    }
}
