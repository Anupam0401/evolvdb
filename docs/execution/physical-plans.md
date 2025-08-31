# Physical Planner & Execution (Milestone 10)

Status: In Progress — Volcano operators and planner scaffolding added.

## Volcano Model (Iterator)
- Each operator implements `open()`, `next()`, `close()`.
- Tuples flow top-down via repeated `next()` calls.

## Core Interfaces
- `evolvdb-exec` module
- `PhysicalOperator`: `open/next/close`, `schema()`
- `PhysicalPlan`: provides `schema()`, `children()`, and builds operators via `create(context)` (for future).
- `ExecContext`: carries `CatalogManager` (and later, txn/session state).

## Implemented Operators
- `SeqScanExec`: scans a table via `CatalogManager.openTable().scanTuples()`.
- `FilterExec`: evaluates boolean predicate with `ExprEvaluator`.
- `ProjectExec`: computes expressions to produce a new tuple per output schema.
- `NestedLoopJoinExec`: inner join; buffers right side; predicate supports equi/non-equi.
- `AggregateExec`: naive group-by; in-memory hash of group keys; supports COUNT/SUM/AVG/MIN/MAX.

## Expression Evaluation
- `ExprEvaluator`: evaluates literals, column refs (qualified/unqualified), arithmetic, comparisons, logical ops; aggregates only in `AggregateExec`.

## Logical → Physical
- `PhysicalPlanner`: 1:1 lowering
  - `LogicalScan` → `SeqScanExec`
  - `LogicalFilter` → `FilterExec`
  - `LogicalProject` → `ProjectExec`
  - `LogicalJoin` → `NestedLoopJoinExec`
  - `LogicalAggregate` → `AggregateExec`

## Example
```sql
CREATE TABLE users (id INT, age INT);
INSERT INTO users VALUES (1, 42), (2, 18);
SELECT id FROM users WHERE age > 20;
```
Produces a Volcano pipeline `Project → Filter → SeqScan` that returns a single row `(1)`.

## Next Steps
- Enrich evaluator (NULLs, type coercion, functions); add `HAVING` when parser supports it.
- Improve join algorithms (hash join, sort-merge) and aggregation strategies.
- Instrumentation & metrics; spill-to-disk for large groups/joins.
