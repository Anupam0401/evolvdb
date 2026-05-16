# Phase 5: Logical Planning (M9)

**Goal**: Understand how validated AST becomes a logical query plan.

**Prerequisites**: Phase 0-4 (Foundations + Storage + Types + SQL)

---

## Overview: From AST to Logical Plan

```
Validated AST
    ↓
Binder (name resolution)
    ↓
Analyzer (type inference)
    ↓
Logical Plan (relational algebra)
    ↓
Rule Engine (transformations)
    ↓
Optimized Logical Plan
```

**Key question**: Why not execute the AST directly?

**Answer**: AST is syntax-focused. Logical plans are **semantically rich** and easier to optimize.

---

## What Is a Logical Plan?

A **logical plan** describes **what** to compute, not **how** to compute it.

**Relational algebra** operators:
- **Scan**: Read all rows from a table
- **Filter**: Keep rows matching a predicate
- **Project**: Select specific columns
- **Join**: Combine rows from two tables
- **Aggregate**: Group rows and compute aggregates (COUNT, SUM, etc.)

**Example**:
```sql
SELECT name FROM users WHERE age > 25
```

**Logical plan**:
```
Project(name)
  ↓
Filter(age > 25)
  ↓
Scan(users)
```

**Interpretation**: "Scan users, keep rows where age > 25, project name column"

---

## Logical Plan Nodes

**Module**: `evolvdb-planner`  
**Package**: `io.github.anupam.evolvdb.planner.logical`

### Base Interface

```java
interface LogicalPlan {
    Schema schema();              // Output schema
    List<LogicalPlan> children(); // Child nodes
    <R> R accept(LogicalPlanVisitor<R> visitor);
}
```

**Key insight**: Every plan node knows its **output schema**.

### LogicalScan

**Purpose**: Read all rows from a table.

```java
class LogicalScan implements LogicalPlan {
    String tableName;
    String alias;  // Optional (e.g., "users u")
    Schema schema; // From catalog
}
```

**Example**:
```sql
SELECT * FROM users
```

**Plan**: `LogicalScan(tableName="users", alias=null)`

**Output schema**: All columns of `users` table

### LogicalFilter

**Purpose**: Keep rows matching a predicate.

```java
class LogicalFilter implements LogicalPlan {
    LogicalPlan child;
    Expr predicate;
}
```

**Example**:
```sql
SELECT * FROM users WHERE age > 25
```

**Plan**:
```
LogicalFilter(predicate: age > 25)
  ↓
LogicalScan(users)
```

**Output schema**: Same as child (filter doesn't change columns)

### LogicalProject

**Purpose**: Select specific columns or compute expressions.

```java
class LogicalProject implements LogicalPlan {
    LogicalPlan child;
    List<ProjectItem> items;  // (expr, alias)
}
```

**Example**:
```sql
SELECT name, age + 1 AS next_age FROM users
```

**Plan**:
```
LogicalProject([name, age+1 AS next_age])
  ↓
LogicalScan(users)
```

**Output schema**: (name: VARCHAR, next_age: INT)

### LogicalJoin

**Purpose**: Combine rows from two tables.

```java
class LogicalJoin implements LogicalPlan {
    LogicalPlan left;
    LogicalPlan right;
    JoinType type;      // INNER, LEFT, RIGHT, FULL (only INNER in M8-M11)
    Expr condition;     // Join predicate (e.g., t1.id = t2.fk)
}
```

**Example**:
```sql
SELECT u.name, o.amount 
FROM users u, orders o 
WHERE u.id = o.user_id
```

**Plan**:
```
LogicalJoin(INNER, u.id = o.user_id)
  ├─ LogicalScan(users AS u)
  └─ LogicalScan(orders AS o)
```

**Output schema**: Combination of left and right schemas (qualified names)

### LogicalAggregate

**Purpose**: Group rows and compute aggregates.

```java
class LogicalAggregate implements LogicalPlan {
    LogicalPlan child;
    List<Expr> groupBy;           // Grouping keys
    List<AggregateCall> aggregates; // COUNT(*), SUM(x), etc.
}
```

**Example**:
```sql
SELECT dept, COUNT(*), SUM(salary) 
FROM employees 
GROUP BY dept
```

**Plan**:
```
LogicalAggregate(
  groupBy: [dept],
  aggregates: [COUNT(*), SUM(salary)]
)
  ↓
LogicalScan(employees)
```

**Output schema**: (dept: type_of_dept, COUNT(*): BIGINT, SUM(salary): type_of_salary)

### LogicalInsert

**Purpose**: Insert rows into a table.

```java
class LogicalInsert implements LogicalPlan {
    String tableName;
    List<String> targetColumns;  // null = all columns
    List<List<Expr>> rows;       // Literal values
}
```

**Example**:
```sql
INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25)
```

**Plan**:
```
LogicalInsert(
  table: users,
  columns: null,
  rows: [[1, 'Alice', 30], [2, 'Bob', 25]]
)
```

**Output schema**: Empty (INSERT doesn't return rows in EvolvDB)

---

## The Binder: Name Resolution

**Module**: `evolvdb-planner`  
**Class**: `io.github.anupam.evolvdb.planner.analyzer.Binder`

### Purpose

Resolve names against the **catalog**:
- Table names → TableMeta
- Column names → ColumnMeta
- Aliases → Table references

**Input**: Validated AST + CatalogManager  
**Output**: Logical plan with resolved references

### Name Resolution Rules

#### Unqualified column reference

```sql
SELECT name FROM users
```

**Resolution**:
1. Check if "name" exists in "users" table schema
2. If yes: Resolve to users.name
3. If no: Error "Column 'name' not found"

#### Qualified column reference

```sql
SELECT u.name FROM users u
```

**Resolution**:
1. Find table with alias "u" (or name "u" if no alias)
2. Check if "name" exists in that table's schema
3. Resolve to users.name

#### Ambiguous column reference

```sql
SELECT id FROM users, orders
```

**Resolution**:
1. Column "id" exists in both tables
2. Error: "Ambiguous column 'id', could be users.id or orders.id"
3. Fix: Use qualified name "users.id" or "orders.id"

### Schema Propagation

Each plan node must compute its **output schema**:

**LogicalScan**:
```java
Schema schema = catalog.getTable(tableName).schema();
if (alias != null) {
    // Qualify all columns with alias
    schema = schema.qualify(alias);
}
```

**LogicalFilter**:
```java
Schema schema = child.schema();  // Same as child
```

**LogicalProject**:
```java
List<ColumnMeta> columns = new ArrayList<>();
for (ProjectItem item : items) {
    Type type = inferType(item.expr);
    String name = item.alias != null ? item.alias : defaultName(item.expr);
    columns.add(new ColumnMeta(name, type, null));
}
return new Schema(columns);
```

**LogicalJoin**:
```java
List<ColumnMeta> leftCols = left.schema().columns();
List<ColumnMeta> rightCols = right.schema().columns();
return new Schema(leftCols + rightCols);
```

---

## The Analyzer: Type Inference

**Module**: `evolvdb-planner`  
**Class**: `io.github.anupam.evolvdb.planner.analyzer.Analyzer`

### Purpose

Infer types for all expressions in the logical plan.

### Type Inference Rules

#### Literals

```java
42                → Type.INT
1000000000000L    → Type.BIGINT
'Alice'           → Type.STRING
TRUE, FALSE       → Type.BOOLEAN
3.14f             → Type.FLOAT
```

#### Column references

```sql
SELECT age FROM users
```

**Type**: Lookup `users.age` in schema → Type.INT

#### Arithmetic operators

```java
INT + INT       → INT
INT + BIGINT    → BIGINT (promote INT to BIGINT)
BIGINT + FLOAT  → FLOAT (promote to widest type)
STRING + STRING → STRING (concatenation)
```

**Type promotion hierarchy**:
```
INT → BIGINT → FLOAT
```

#### Comparison operators

```java
age > 25        → BOOLEAN
name = 'Alice'  → BOOLEAN
```

**All comparisons return BOOLEAN**

#### Logical operators

```java
AND, OR, NOT    → BOOLEAN
```

**Require BOOLEAN operands**

#### Aggregates

```java
COUNT(*)        → BIGINT
COUNT(expr)     → BIGINT
SUM(INT)        → BIGINT
SUM(BIGINT)     → BIGINT
SUM(FLOAT)      → FLOAT
AVG(INT)        → FLOAT (division)
AVG(BIGINT)     → FLOAT
MIN(T)          → T (same as input)
MAX(T)          → T
```

### Type Checking

**Comparison type rules**:
- Both operands must have compatible types
- INT and BIGINT are compatible (promote INT)
- Numeric and STRING are incompatible

**Example valid**:
```sql
age > 25           ✅ (INT > INT)
age > 25.5         ✅ (INT > FLOAT, promote to FLOAT)
```

**Example invalid**:
```sql
age > 'Alice'      ❌ (INT > STRING, incompatible)
```

---

## Handling Multi-Table Queries

### Comma-Separated FROM

EvolvDB M8-M11 uses **comma-separated FROM** for joins:

```sql
SELECT u.name, o.amount 
FROM users u, orders o 
WHERE u.id = o.user_id AND o.amount > 100
```

### Join Condition Extraction

**Algorithm**:
1. Build Cartesian product (cross join) of all tables
2. Extract join conditions from WHERE clause
3. Remaining predicates stay as filters

**Join condition** = predicate referencing columns from **both sides**

**Example**:
```sql
WHERE u.id = o.user_id AND o.amount > 100
```

**Analysis**:
- `u.id = o.user_id`: References both `u` and `o` → **Join condition**
- `o.amount > 100`: References only `o` → **Filter**

**Logical plan**:
```
LogicalFilter(o.amount > 100)
  ↓
LogicalJoin(INNER, u.id = o.user_id)
  ├─ LogicalScan(users AS u)
  └─ LogicalScan(orders AS o)
```

### Multi-Way Joins

```sql
FROM t1, t2, t3 WHERE t1.id = t2.fk1 AND t2.id = t3.fk2
```

**Builds left-deep join tree**:
```
LogicalJoin(t2.id = t3.fk2)
  ├─ LogicalJoin(t1.id = t2.fk1)
  │   ├─ LogicalScan(t1)
  │   └─ LogicalScan(t2)
  └─ LogicalScan(t3)
```

**Later optimization (M11)** can reorder joins for better performance.

---

## Rule Engine: Logical Transformations

**Module**: `evolvdb-planner`  
**Package**: `io.github.anupam.evolvdb.planner.rules`

### Purpose

Apply **equivalence-preserving transformations** to improve the plan.

**Equivalence-preserving** = produces same results, but possibly more efficient

### Rule Interface

```java
interface Rule {
    boolean matches(LogicalPlan plan);
    LogicalPlan apply(LogicalPlan plan);
}
```

**Contract**:
- `matches`: Returns true if rule can be applied
- `apply`: Returns transformed plan (must be equivalent)

### Rule Engine

```java
class RuleEngine {
    List<Rule> rules;
    
    LogicalPlan optimize(LogicalPlan plan) {
        boolean changed = true;
        while (changed) {
            changed = false;
            for (Rule rule : rules) {
                if (rule.matches(plan)) {
                    plan = rule.apply(plan);
                    changed = true;
                    break;  // Restart from first rule
                }
            }
        }
        return plan;
    }
}
```

**Fixed-point iteration**: Apply rules until no more changes.

### Built-in Rules

#### PredicateSimplification

**Simplify logical expressions**:

```
NOT(NOT(x))          → x
TRUE AND x           → x
FALSE OR x           → x
x AND x              → x
```

**Example**:
```sql
WHERE NOT(NOT(age > 25))
```

**Transforms to**:
```sql
WHERE age > 25
```

#### PushProjectBelowFilter

**When safe, push projection below filter**:

**Before**:
```
Project(name)
  ↓
Filter(age > 25)
  ↓
Scan(users)
```

**After** (if filter only uses 'name' and 'age'):
```
Filter(age > 25)
  ↓
Project(name, age)
  ↓
Scan(users)
```

**Benefit**: Filter operates on fewer columns.

**Safety condition**: Filter must not reference columns removed by projection.

#### RemoveRedundantProject

**Remove identity projections**:

**Before**:
```
Project(id, name, age)    ← All columns in same order
  ↓
Scan(users)               ← Schema: (id, name, age)
```

**After**:
```
Scan(users)
```

**Benefit**: One less operator to execute.

### Why Rules at Logical Level?

**Logical level** = algorithm-independent optimizations
- Predicate pushdown works regardless of join algorithm
- Projection pruning benefits all operators

**Physical level** = algorithm-specific optimizations (Phase 7)

---

## Complete Example: Multi-Table Query

**SQL**:
```sql
SELECT u.name, o.amount 
FROM users u, orders o 
WHERE u.id = o.user_id AND o.amount > 100
```

### Step 1: Parse to AST

```
Select {
  items: [u.name, o.amount],
  tables: [users AS u, orders AS o],
  where: (u.id = o.user_id) AND (o.amount > 100)
}
```

### Step 2: Bind Names

**Resolve tables**:
- `users u` → TableMeta for "users", alias "u"
- `orders o` → TableMeta for "orders", alias "o"

**Resolve columns**:
- `u.name` → users.name (VARCHAR)
- `o.amount` → orders.amount (INT)
- `u.id` → users.id (INT)
- `o.user_id` → orders.user_id (INT)

### Step 3: Build Initial Logical Plan

```
LogicalProject([u.name, o.amount])
  ↓
LogicalFilter((u.id = o.user_id) AND (o.amount > 100))
  ↓
LogicalJoin(CROSS)  ← Cartesian product
  ├─ LogicalScan(users AS u)
  └─ LogicalScan(orders AS o)
```

### Step 4: Extract Join Conditions

**Analyze WHERE clause**:
- `u.id = o.user_id`: References both sides → Move to join
- `o.amount > 100`: References only right side → Stay as filter

**Transformed plan**:
```
LogicalProject([u.name, o.amount])
  ↓
LogicalFilter(o.amount > 100)
  ↓
LogicalJoin(INNER, u.id = o.user_id)
  ├─ LogicalScan(users AS u)
  └─ LogicalScan(orders AS o)
```

### Step 5: Infer Types

**Project schema**:
- u.name: VARCHAR
- o.amount: INT

**Filter condition**:
- o.amount (INT) > 100 (INT) → BOOLEAN ✅

**Join condition**:
- u.id (INT) = o.user_id (INT) → BOOLEAN ✅

### Step 6: Apply Rules

**RemoveRedundantProject**: Not applicable (projection is not identity)

**PushProjectBelowFilter**: Not safe (projection removes columns needed by filter)

**PredicateSimplification**: No simplifications needed

**Final logical plan**:
```
LogicalProject([u.name, o.amount])
  ↓
LogicalFilter(o.amount > 100)
  ↓
LogicalJoin(INNER, u.id = o.user_id)
  ├─ LogicalScan(users AS u)
  └─ LogicalScan(orders AS o)
```

**This plan is ready for physical planning (Phase 6).**

---

## Key Insights

### Why Logical Plans?

1. **Algorithm-independent**: Describes "what", not "how"
2. **Composable**: Operators can be rearranged
3. **Analyzable**: Easy to reason about equivalence
4. **Optimizable**: Rules can transform without changing semantics

### Schema Propagation

Every node knows its output schema:
- Scan: From catalog
- Filter: Same as child
- Project: Computed from expressions
- Join: Concatenation of left and right

**Why important?**
- Type checking
- Name resolution
- Optimization (e.g., column pruning)

### Binding vs. Planning

**Binder**: Name resolution (catalog lookups)
**Analyzer**: Type inference (expression typing)
**Planner**: Logical plan construction (relational algebra)

**Order matters**: Must bind before type inference, must type before planning.

---

## Self-Test Questions

1. **What's the difference between Filter and Project?**
   - Filter: Keep/reject rows
   - Project: Select/compute columns

2. **Why does every LogicalPlan node have a schema() method?**
   - Schema propagation for type checking and optimization

3. **What's the difference between logical and physical plans?**
   - Logical: "What" to compute (algorithm-independent)
   - Physical: "How" to compute (specific algorithms)

4. **How does the Binder resolve "SELECT name FROM users, orders"?**
   - Error: Ambiguous (name could be in users or orders)

5. **What does the Rule Engine do?**
   - Applies equivalence-preserving transformations to improve plans

6. **Why extract join conditions from WHERE clause?**
   - More efficient than filtering after Cartesian product

---

## Next Steps

You now understand logical planning. In **Phase 6**, we'll explore physical execution:
- Volcano iterator model
- Physical operators (how they actually execute)
- Expression evaluation at runtime
- Data flow through operator trees

**Continue to**: [`phase-6-physical-execution.md`](./phase-6-physical-execution.md)
