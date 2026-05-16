package io.github.anupam.evolvdb.common;

/**
 * Holds the ScopedValue for the active transaction context. Operators and storage layers access the
 * current transaction via {@code TxnScope.CURRENT.get()} without explicit parameter threading.
 *
 * <p>Usage at the top level (e.g., Database.execute):
 *
 * <pre>{@code
 * ScopedValue.where(TxnScope.CURRENT, txnCtx).run(() -> {
 *     // All code within this scope can read TxnScope.CURRENT.get()
 *     operator.open();
 *     ...
 * });
 * }</pre>
 */
public final class TxnScope {
    public static final ScopedValue<TransactionContext> CURRENT = ScopedValue.newInstance();

    private TxnScope() {}
}
