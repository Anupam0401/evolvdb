package io.github.anupam.evolvdb.optimizer;

/** Simple cost vector for physical planning. */
public final class Cost implements Comparable<Cost> {
    public static final Cost INFINITE = new Cost(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY);

    private final double rowCount; // estimated output rows
    private final double cpu;      // abstract CPU units
    private final double io;       // abstract IO units

    public Cost(double rowCount, double cpu, double io) {
        this.rowCount = rowCount;
        this.cpu = cpu;
        this.io = io;
    }

    public static Cost of(double rowCount, double cpu, double io) {
        return new Cost(rowCount, cpu, io);
    }

    public double rowCount() { return rowCount; }
    public double cpu() { return cpu; }
    public double io() { return io; }

    public Cost plus(Cost other) {
        return new Cost(this.rowCount + other.rowCount, this.cpu + other.cpu, this.io + other.io);
    }

    public Cost scale(double factor) {
        return new Cost(this.rowCount * factor, this.cpu * factor, this.io * factor);
    }

    public double total() {
        // simple weighted sum; tweakable later
        return rowCount + cpu + io;
    }

    @Override
    public int compareTo(Cost o) {
        return Double.compare(this.total(), o.total());
    }

    @Override
    public String toString() {
        return "Cost{rows=" + rowCount + ", cpu=" + cpu + ", io=" + io + '}';
    }
}
