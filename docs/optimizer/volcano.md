# Volcano Optimizer (M11)

This document outlines the minimal Volcano-style optimizer added in M11, with a simple cost model and pluggable transformation rules that produce physical alternatives.

## Goals (M11 scope)
- Provide a working optimizer skeleton that can explore multiple physical alternatives for a logical plan and select a lowest-cost plan.
- Keep the framework extensible for future operators (HashJoin, SortMergeJoin, HashAggregate) and improved statistics.
- Integrate cleanly into the pipeline with a simple on/off flag.

## Architecture Overview

- Memo-lite traversal (no persistent memo yet):
  - The optimizer recursively optimizes children bottom-up and then applies rules on the current logical node to produce physical plan alternatives.
  - Among alternatives, it picks the one with the lowest cost according to the cost model.

- Components:
  - Cost vector: `rowCount`, `cpu`, `io` in `optimizer/Cost.java`.
  - CostModel: `optimizer/CostModel.java`, with naive default implementation in `optimizer/DefaultCostModel.java`.
  - Physical plans: `exec/plan/*.java`, each can:
    - return an estimated `Cost` via `estimate(CostModel)`
    - build a `PhysicalOperator` via `create(ExecContext)`
  - Rules: `optimizer/Rules.java` generate physical alternatives for each logical node.
  - Optimizer: `optimizer/VolcanoOptimizer.java` orchestrates bottom-up optimization and alternative selection.

## Pipeline Integration

SQL → Parser → Analyzer/Binder → Logical Plan → Optimizer (optional) → Best Physical Plan → Operator Tree (Volcano) → Execution

- The optimizer is enabled by `ExecContext`:
```java
ExecContext ctx = new ExecContext(catalogManager, /* useOptimizer = */ true);
```
- When disabled, the engine uses the direct logical→physical lowering in `exec/PhysicalPlanner.java`.

## Rules (Search Space Expansion)

Implemented rules in `optimizer/Rules.java`:
- `ScanRule`: `LogicalScan` → `SeqScanPlan`.
- `FilterRule`: `LogicalFilter` → `FilterPlan`.
- `ProjectRule`: `LogicalProject` → `ProjectPlan`.
- `AggregateRule`: `LogicalAggregate` → `AggregatePlan`.
- `JoinRule`: `LogicalJoin` → alternatives:
  - `NestedLoopJoinPlan` (baseline, implemented via `NestedLoopJoinExec`)
  - `HashJoinPlan` (placeholder, not executable yet)
  - `SortMergeJoinPlan` (placeholder, not executable yet)
- `InsertRule`: `LogicalInsert` → `InsertPlan`.

The optimizer picks the alternative with the lowest estimated cost. For now, the default cost model makes NestedLoopJoin cheaper than the (placeholder) hash or sort-merge variants, so it is chosen.

## Cost Model

`DefaultCostModel` provides naive estimates:
- `SeqScan`: rows = defaultRows (1000 by default); cpu ~ rows; io ~ rows/100.
- `Filter`: rows = child.rows × 0.1; small per-row cpu overhead.
- `Project`: rows unchanged; small per-row cpu overhead.
- `NestedLoopJoin`: rows = left.rows × right.rows × 0.25; cpu ~ left.rows × right.rows.
- `HashJoin`, `SortMergeJoin`: scaled up from NLJ for now (placeholders).
- `Aggregate`: rows ~ child.rows × 0.1; cpu ~ child.rows.
- `Insert`: rows, cpu, io scale linearly with number of inserted rows.

Tune parameters by subclassing `CostModel` or adjusting `DefaultCostModel` constructor.

## Example Walkthrough

Query:
```sql
SELECT u.name, COUNT(*)
FROM users u, orders o
WHERE u.id = o.user_id
GROUP BY u.name
```

1) Analyzer builds a `LogicalJoin` between `users` and `orders` (with `u.id = o.user_id`), followed by `LogicalAggregate` over `u.name` and `COUNT(*)`.
2) The optimizer optimizes bottom-up:
   - For each `LogicalScan`, a `SeqScanPlan` is produced.
   - For `LogicalJoin`, multiple alternatives are produced; cost model selects `NestedLoopJoinPlan`.
   - For `LogicalAggregate`, `AggregatePlan` is produced.
3) Final physical plan is turned into executable operators via `create(ctx)` and run with the Volcano model.

## Testing

- Unit tests:
  - `CostModelTest`: sanity checks for cost ordering between operator variants.
- E2E test:
  - `OptimizerE2ETest`: creates small `users` and `orders` tables, runs a join query with the optimizer enabled, and validates output.

## Future Work (Deferred)

- Statistics and Cardinality Estimation: real table/column stats, histograms, distinct counts.
- Advanced Rules: join reordering, predicate pushdown across joins, projection pruning.
- Additional Operators: HashJoinExec, SortMergeJoinExec, HashAggregate (with spill if needed).
- Memo structure with group CSE and better exploration control.
- Cost-based plan selection with real stats and calibrated costs.
