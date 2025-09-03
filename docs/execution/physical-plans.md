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
- `InsertExec`: inserts values into a table.

## Expression Evaluation

`ExprEvaluator` evaluates SQL AST expressions at runtime:
- Literals, column references
- Binary arithmetic (ADD, SUB, MUL, DIV, CONCAT)
- Comparisons (EQ, NEQ, LT, LTE, GT, GTE)
- Logical operators (AND, OR, NOT)
- Aggregate functions handled in `AggregateExec`

## Execution Examples

### Simple Query with Filter
```sql
SELECT id, name FROM users WHERE age > 25
```

Physical operator tree:
```
ProjectExec(id, name)
  └── FilterExec(age > 25)
        └── SeqScanExec(users)
```

### Join Query
```sql
SELECT u.name, o.amount 
FROM users u, orders o 
WHERE u.id = o.user_id
```

Physical operator tree:
```
ProjectExec(u.name, o.amount)
  └── NestedLoopJoinExec(u.id = o.user_id)
        ├── SeqScanExec(users u)
        └── SeqScanExec(orders o)
```

### Aggregation Query
```sql
SELECT region, COUNT(*), SUM(amount) 
FROM sales 
GROUP BY region
```

Physical operator tree:
```
AggregateExec(groupBy=[region], aggs=[COUNT(*), SUM(amount)])
  └── SeqScanExec(sales)
```

### INSERT Statement
```sql
INSERT INTO users VALUES (1, 'Alice', 25), (2, 'Bob', 30)
```

Physical operator:
```
InsertExec(table=users, values=[[1,'Alice',25], [2,'Bob',30]])
```

## Logical to Physical Lowering

`PhysicalPlanner` converts logical plans to physical operators:
- `LogicalScan` → `SeqScanExec`
- `LogicalFilter` → `FilterExec`
- `LogicalProject` → `ProjectExec`
- `LogicalJoin` → `NestedLoopJoinExec`
- `LogicalAggregate` → `AggregateExec`
- `LogicalInsert` → `InsertExec`

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
