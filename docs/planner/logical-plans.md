# Logical Planner & Analyzer (Milestone 9)

Status: Completed — Binder/Analyzer, logical nodes, joins, aggregates, and minimal rules are implemented with tests.

## Purpose
- Translate parsed SQL AST into a catalog-aware logical plan.
- Resolve names and types using `CatalogManager`.
- Provide a rule-based framework to transform logical plans (e.g., pushdowns).

## Architecture
- Package `io.github.anupam.evolvdb.planner.analyzer`: `Binder`, `Analyzer`.
- Package `io.github.anupam.evolvdb.planner.logical`: immutable logical nodes and visitor.
- Package `io.github.anupam.evolvdb.planner.rules`: rule interface and engine.

## Supported mapping (AST → LogicalPlan)
- SELECT ... FROM T [WHERE ...]
  - `LogicalScan(T)`
  - Optional `LogicalFilter(predicate)`
  - Optional `LogicalProject(items)` unless SELECT *.
- SELECT ... FROM T1[, T2, ...] [WHERE ...]
  - Left-deep `LogicalJoin` chain is built from comma-separated FROM tables.
  - Join conditions are extracted from WHERE conjuncts that reference columns from both sides; remaining predicates stay in a `LogicalFilter` above the join.
  - For multi-table `SELECT *`, a `LogicalProject` is produced with qualified output names (e.g., `t.col`).
- SELECT ... FROM T [WHERE ...] GROUP BY expr[, ...]
  - `LogicalAggregate(groupKeys, aggregates)` over the input plan.
  - Aggregate functions supported: COUNT(*|expr), SUM(expr), AVG(expr), MIN(expr), MAX(expr).
  - Non-aggregated SELECT items must be functionally dependent on GROUP BY (columns must appear in GROUP BY).
- INSERT INTO T VALUES (...)
  - `LogicalInsert(T, targetColumns, rows)`

## Examples

```sql
SELECT id, name FROM users WHERE id >= 10;
```

```mermaid
flowchart LR
  Project --> Filter
  Filter --> Scan
  Project[LogicalProject(id,name)]
  Filter[LogicalFilter(id>=10)]
  Scan[LogicalScan(users)]
```

After rule (push project below filter):

```mermaid
flowchart LR
  Filter --> Project
  Project --> Scan
  Filter[LogicalFilter(id>=10)]
  Project[LogicalProject(id,name)]
  Scan[LogicalScan(users)]
```

### Multi-table join example

```sql
SELECT u.name, o.amount
FROM users u, orders o
WHERE u.id = o.user_id AND o.amount > 10;
```

```mermaid
flowchart LR
  Proj --> F
  F --> J
  J --> Su
  J --> So
  Proj[LogicalProject(u.name, o.amount)]
  F[LogicalFilter(o.amount > 10)]
  J[LogicalJoin(INNER, cond: u.id = o.user_id)]
  Su[LogicalScan(users AS u)]
  So[LogicalScan(orders AS o)]
```

### Group by + aggregates example

```sql
SELECT user_id, COUNT(*) AS cnt, SUM(amount) AS total
FROM orders
GROUP BY user_id;
```

```mermaid
flowchart LR
  Agg --> S
  Agg[LogicalAggregate(keys: user_id, aggs: COUNT(*), SUM(amount))]
  S[LogicalScan(orders)]
```

## Binder responsibilities
- Resolve table names to `TableMeta`/`Schema`.
- Validate column references and qualifiers against schema/alias.
- Derive output schema for projections using simple type inference:
  - ColumnRef → referenced column type.
  - Literals → mapped to INT/BIGINT/BOOLEAN/STRING.
  - Arithmetic → INT/BIGINT/FLOAT promotion.
  - Comparisons/Logical → BOOLEAN.
- DDL binding is not handled in planner (exec layer will handle DDL separately).

## Rule Framework
- `Rule`: `matches(plan)` and `apply(plan)`.
- `RuleEngine`: top-down recursion, rebuilds children, applies rules to fixed-point per node.
- Example rules included:
  - `PushProjectBelowFilter` (safe when filter only uses projected columns).
  - `RemoveRedundantProject` (identity projection over child schema).
  - `PredicateSimplification` (e.g., NOT(NOT x) -> x).

## Limitations & Next Steps
- Joins are formed from comma-separated FROMs; explicit ANSI JOIN syntax is not parsed yet.
- Aggregation supports COUNT/SUM/AVG/MIN/MAX; advanced expressions and HAVING not yet supported.
- Optimizer (M11): constant folding, projection pruning, predicate reordering, simple join heuristics.
- Prepare for physical planning (Volcano iterator model in M10).
