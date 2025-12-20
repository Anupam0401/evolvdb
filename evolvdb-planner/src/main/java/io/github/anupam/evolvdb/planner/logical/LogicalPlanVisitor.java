package io.github.anupam.evolvdb.planner.logical;

public interface LogicalPlanVisitor<R, C> {
    R visitScan(LogicalScan scan, C ctx);
    R visitProject(LogicalProject project, C ctx);
    R visitFilter(LogicalFilter filter, C ctx);
    R visitJoin(LogicalJoin join, C ctx);
    R visitAggregate(LogicalAggregate agg, C ctx);
    R visitInsert(LogicalInsert insert, C ctx);
    R visitUpdate(LogicalUpdate update, C ctx);
    R visitDelete(LogicalDelete delete, C ctx);
}
