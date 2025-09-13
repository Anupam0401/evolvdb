package io.github.anupam.evolvdb.optimizer.stats;

public final class ColumnStats {
    private final long distinctCount;
    private final double nullFraction; // 0.0 to 1.0

    public ColumnStats(long distinctCount, double nullFraction) {
        this.distinctCount = distinctCount;
        this.nullFraction = nullFraction;
    }

    public long distinctCount() { return distinctCount; }
    public double nullFraction() { return nullFraction; }
}
