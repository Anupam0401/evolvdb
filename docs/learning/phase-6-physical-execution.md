# Phase 6: Physical Execution (M10)

**Goal**: Understand the Volcano iterator model and how queries actually execute.

**Prerequisites**: Phase 0-5 (Foundations through Logical Planning)

---

## Overview: From Logical to Physical

```
Logical Plan (WHAT to compute)
    ↓
Physical Planner
    ↓
Physical Plan (HOW to compute)
    ↓
Operator Execution (Volcano Model)
    ↓
Results
```

**Key difference**:
- **Logical**: "Join these two tables"
- **Physical**: "Use nested loop join with left side as outer loop"

---

## The Volcano Model

**Also called**: Iterator model, Pipeline model

**Invented**: 1990s by Goetz Graefe

**Key idea**: Every operator is an **iterator** with three methods:

```java
interface PhysicalOperator {
    void open();              // Initialize
    Tuple next();            // Get next tuple (null = done)
    void close();            // Cleanup
    Schema schema();         // Output schema
}
```

### Execution Flow

**Pull-based** execution:

```
Client calls root.next()
    ↓
Root calls child.next()
    ↓
Child calls grandchild.next()
    ↓
...reaches leaf (table scan)
    ↓
Leaf returns tuple
    ↓
Bubbles up through operators
    ↓
Each operator processes/transforms
    ↓
Returns to client
```

**Analogy**: Assembly line pulling parts from upstream.

### Example: Simple Query

```sql
SELECT name FROM users WHERE age > 25
```

**Physical plan**:
```
ProjectExec(name)
  ↓
FilterExec(age > 25)
  ↓
SeqScanExec(users)
```

**Execution**:
```java
// Client code
ProjectExec root = ...;
root.open();
while (true) {
    Tuple tuple = root.next();
    if (tuple == null) break;
    System.out.println(tuple);
}
root.close();
```

**Internal flow for first next() call**:
```
1. ProjectExec.next() called
2.   Calls FilterExec.next()
3.     Calls SeqScanExec.next()
4.       Reads first row from table: (1, "Alice", 30)
5.       Returns Tuple(1, "Alice", 30)
6.     FilterExec evaluates: age (30) > 25? → TRUE
7.     Returns Tuple(1, "Alice", 30)
8.   ProjectExec extracts name column
9.   Returns Tuple("Alice")
```

**For second next() call**:
```
1. ProjectExec.next() called
2.   Calls FilterExec.next()
3.     Calls SeqScanExec.next()
4.       Reads second row: (2, "Bob", 20)
5.       Returns Tuple(2, "Bob", 20)
6.     FilterExec evaluates: age (20) > 25? → FALSE
7.     Calls SeqScanExec.next() again (loop internally)
8.       Reads third row: (3, "Charlie", 35)
9.       Returns Tuple(3, "Charlie", 35)
10.    FilterExec evaluates: age (35) > 25? → TRUE
11.    Returns Tuple(3, "Charlie", 35)
12.  ProjectExec extracts name column
13.  Returns Tuple("Charlie")
```

---

## Physical Operators

**Module**: `evolvdb-exec`  
**Package**: `io.github.anupam.evolvdb.exec.op`

### SeqScanExec - Sequential Scan

**Purpose**: Read all tuples from a table.

```java
class SeqScanExec implements PhysicalOperator {
    Table table;
    Iterator<Tuple> iterator;
    
    void open() {
        iterator = table.scanTuples().iterator();
    }
    
    Tuple next() {
        return iterator.hasNext() ? iterator.next() : null;
    }
    
    void close() {
        // No cleanup needed
    }
}
```

**Performance**: O(n) where n = number of rows

**Usage**: Base case for most queries (no indexes in M1-M11)

### FilterExec - Predicate Filter

**Purpose**: Keep only tuples matching a predicate.

```java
class FilterExec implements PhysicalOperator {
    PhysicalOperator child;
    Expr predicate;
    ExprEvaluator evaluator;
    
    void open() {
        child.open();
    }
    
    Tuple next() {
        while (true) {
            Tuple tuple = child.next();
            if (tuple == null) return null;
            
            Object result = evaluator.eval(predicate, tuple);
            if (Boolean.TRUE.equals(result)) {
                return tuple;
            }
            // Loop until match or exhausted
        }
    }
    
    void close() {
        child.close();
    }
}
```

**Key insight**: Loops internally until finding a matching tuple.

**Performance**: O(n) scan, but fewer rows returned (selectivity-dependent)

### ProjectExec - Projection

**Purpose**: Compute expressions and build new tuple.

```java
class ProjectExec implements PhysicalOperator {
    PhysicalOperator child;
    List<Expr> projections;
    Schema outputSchema;
    ExprEvaluator evaluator;
    
    void open() {
        child.open();
    }
    
    Tuple next() {
        Tuple input = child.next();
        if (input == null) return null;
        
        List<Object> values = new ArrayList<>();
        for (Expr expr : projections) {
            values.add(evaluator.eval(expr, input));
        }
        return new Tuple(outputSchema, values);
    }
    
    void close() {
        child.close();
    }
}
```

**Example**:
- Input: `(id=1, name="Alice", age=30)`
- Projection: `[name, age+1]`
- Output: `("Alice", 31)`

**Performance**: O(n) with small per-row overhead

### NestedLoopJoinExec - Nested Loop Join

**Purpose**: Join two tables using nested loops.

```java
class NestedLoopJoinExec implements PhysicalOperator {
    PhysicalOperator left;
    PhysicalOperator right;
    Expr condition;
    ExprEvaluator evaluator;
    
    Tuple currentLeft;
    List<Tuple> rightTuples;  // Buffered
    int rightIndex;
    
    void open() {
        left.open();
        right.open();
        // Buffer entire right side
        rightTuples = new ArrayList<>();
        while (true) {
            Tuple t = right.next();
            if (t == null) break;
            rightTuples.add(t);
        }
        right.close();
        rightIndex = 0;
    }
    
    Tuple next() {
        while (true) {
            if (currentLeft == null) {
                currentLeft = left.next();
                if (currentLeft == null) return null;
                rightIndex = 0;
            }
            
            if (rightIndex >= rightTuples.size()) {
                currentLeft = null;
                continue;
            }
            
            Tuple rightTuple = rightTuples.get(rightIndex++);
            Tuple combined = combine(currentLeft, rightTuple);
            
            if (condition == null) {
                return combined;  // Cross join
            }
            
            Object result = evaluator.eval(condition, combined);
            if (Boolean.TRUE.equals(result)) {
                return combined;
            }
        }
    }
    
    void close() {
        left.close();
    }
}
```

**Algorithm**:
```
For each tuple in left:
    For each tuple in right:
        If condition matches:
            Emit combined tuple
```

**Performance**: O(n × m) where n = left rows, m = right rows

**Memory**: O(m) to buffer right side

**When good**: Small right side, no equi-join condition

**When bad**: Large tables (quadratic blowup)

### HashJoinExec - Hash Join

**Purpose**: Efficient equi-join using hash table.

```java
class HashJoinExec implements PhysicalOperator {
    PhysicalOperator left;
    PhysicalOperator right;
    Expr leftKey;   // e.g., u.id
    Expr rightKey;  // e.g., o.user_id
    
    Map<Object, List<Tuple>> hashTable;
    Iterator<Tuple> leftIterator;
    Tuple currentLeft;
    List<Tuple> matches;
    int matchIndex;
    
    void open() {
        left.open();
        right.open();
        
        // Build phase: hash right side
        hashTable = new HashMap<>();
        while (true) {
            Tuple t = right.next();
            if (t == null) break;
            Object key = evaluator.eval(rightKey, t);
            hashTable.computeIfAbsent(key, k -> new ArrayList<>()).add(t);
        }
        right.close();
    }
    
    Tuple next() {
        while (true) {
            // Emit remaining matches for current left tuple
            if (matches != null && matchIndex < matches.size()) {
                Tuple right = matches.get(matchIndex++);
                return combine(currentLeft, right);
            }
            
            // Get next left tuple and probe hash table
            currentLeft = left.next();
            if (currentLeft == null) return null;
            
            Object key = evaluator.eval(leftKey, currentLeft);
            matches = hashTable.get(key);
            matchIndex = 0;
            
            if (matches == null || matches.isEmpty()) {
                matches = null;  // No matches, continue to next left tuple
            }
        }
    }
    
    void close() {
        left.close();
    }
}
```

**Algorithm**:
```
Build phase:
    For each tuple in right:
        key = extract join key
        hashTable[key].add(tuple)

Probe phase:
    For each tuple in left:
        key = extract join key
        For each match in hashTable[key]:
            Emit combined tuple
```

**Performance**: O(n + m) average case

**Memory**: O(m) for hash table

**Requirement**: Equi-join condition (e.g., `t1.id = t2.fk`)

**When good**: Large tables with equi-join

**When bad**: Non-equi-join, skewed data (many duplicates for one key)

### SortMergeJoinExec - Sort-Merge Join

**Purpose**: Join sorted inputs by merging.

```java
class SortMergeJoinExec implements PhysicalOperator {
    PhysicalOperator left;
    PhysicalOperator right;
    Expr leftKey;
    Expr rightKey;
    
    List<Tuple> leftSorted;
    List<Tuple> rightSorted;
    int leftIndex;
    int rightIndex;
    
    void open() {
        left.open();
        right.open();
        
        // Sort both sides
        leftSorted = sortByKey(left, leftKey);
        rightSorted = sortByKey(right, rightKey);
        
        left.close();
        right.close();
        
        leftIndex = 0;
        rightIndex = 0;
    }
    
    Tuple next() {
        // Merge algorithm (simplified)
        while (leftIndex < leftSorted.size() && rightIndex < rightSorted.size()) {
            Tuple l = leftSorted.get(leftIndex);
            Tuple r = rightSorted.get(rightIndex);
            
            Object leftK = evaluator.eval(leftKey, l);
            Object rightK = evaluator.eval(rightKey, r);
            
            int cmp = compare(leftK, rightK);
            if (cmp < 0) {
                leftIndex++;
            } else if (cmp > 0) {
                rightIndex++;
            } else {
                // Match found
                // Handle duplicates...
                return combine(l, r);
            }
        }
        return null;
    }
    
    void close() {
        // Already closed
    }
}
```

**Algorithm**:
```
Sort left by join key
Sort right by join key
Merge:
    While both have rows:
        If left.key < right.key: advance left
        If left.key > right.key: advance right
        If left.key = right.key: emit match, advance
```

**Performance**: O(n log n + m log m) for sorting + O(n + m) for merge

**Memory**: O(n + m) to store sorted inputs

**When good**: Inputs already sorted, or need sorted output

**When bad**: Small tables (hash join faster)

### AggregateExec - Aggregation

**Purpose**: Group rows and compute aggregates.

```java
class AggregateExec implements PhysicalOperator {
    PhysicalOperator child;
    List<Expr> groupByExprs;
    List<AggregateCall> aggregates;  // COUNT(*), SUM(x), etc.
    
    Map<GroupKey, AggregateState> groups;
    Iterator<Map.Entry<GroupKey, AggregateState>> iterator;
    
    void open() {
        child.open();
        groups = new HashMap<>();
        
        // Consume entire input
        while (true) {
            Tuple tuple = child.next();
            if (tuple == null) break;
            
            // Compute group key
            List<Object> keyValues = new ArrayList<>();
            for (Expr expr : groupByExprs) {
                keyValues.add(evaluator.eval(expr, tuple));
            }
            GroupKey key = new GroupKey(keyValues);
            
            // Update aggregates for this group
            AggregateState state = groups.computeIfAbsent(key, k -> new AggregateState());
            for (AggregateCall agg : aggregates) {
                state.update(agg, tuple);
            }
        }
        
        child.close();
        iterator = groups.entrySet().iterator();
    }
    
    Tuple next() {
        if (!iterator.hasNext()) return null;
        
        Map.Entry<GroupKey, AggregateState> entry = iterator.next();
        List<Object> values = new ArrayList<>();
        values.addAll(entry.getKey().values);  // Group keys
        values.addAll(entry.getValue().finalize());  // Aggregate results
        
        return new Tuple(outputSchema, values);
    }
    
    void close() {
        // Already closed child
    }
}
```

**Aggregates supported**:

**COUNT(*)**: Count all rows
```java
int count = 0;
for each tuple: count++;
return count;
```

**COUNT(expr)**: Count non-null values (nulls not supported yet)
```java
int count = 0;
for each tuple:
    if (eval(expr, tuple) != null) count++;
return count;
```

**SUM(expr)**: Sum numeric values
```java
long sum = 0;  // or double for FLOAT
for each tuple:
    sum += (Number) eval(expr, tuple);
return sum;
```

**AVG(expr)**: Average
```java
long sum = 0;
int count = 0;
for each tuple:
    sum += (Number) eval(expr, tuple);
    count++;
return (double) sum / count;
```

**MIN(expr), MAX(expr)**: Minimum/maximum
```java
Comparable min = null;
for each tuple:
    Object val = eval(expr, tuple);
    if (min == null || val.compareTo(min) < 0)
        min = val;
return min;
```

**Performance**: O(n) for hash aggregation

**Memory**: O(distinct groups) - can be large for high cardinality

### InsertExec - Insert Rows

**Purpose**: Insert tuples into a table.

```java
class InsertExec implements PhysicalOperator {
    String tableName;
    List<List<Expr>> rows;  // Literal values
    CatalogManager catalog;
    
    int currentRow;
    boolean done;
    
    void open() {
        Table table = catalog.openTable(tableName);
        Schema schema = table.schema();
        
        // Insert all rows
        for (List<Expr> row : rows) {
            List<Object> values = new ArrayList<>();
            for (Expr expr : row) {
                values.add(evaluator.eval(expr, null));  // Literals, no input tuple
            }
            Tuple tuple = new Tuple(schema, values);
            table.insert(tuple);
        }
        
        done = true;
    }
    
    Tuple next() {
        // INSERT doesn't return rows in EvolvDB
        return null;
    }
    
    void close() {
        // Nothing to close
    }
}
```

**Performance**: O(n × page_scan_cost) where n = rows inserted

**Future optimization**: Batch inserts to same page

---

## Expression Evaluation

**Module**: `evolvdb-exec`  
**Class**: `io.github.anupam.evolvdb.exec.expr.ExprEvaluator`

### Purpose

Evaluate AST expressions at runtime with a given tuple as context.

### Evaluation Rules

#### Literals
```java
Literal(42) → 42
Literal("Alice") → "Alice"
Literal(true) → true
```

#### Column References
```java
ColumnRef("age") + Tuple(id=1, age=30) → 30
ColumnRef("u.name") + Tuple(u.name="Alice", ...) → "Alice"
```

#### Binary Operators
```java
BinaryExpr(a + b):
    left = eval(a, tuple)
    right = eval(b, tuple)
    return left + right

BinaryExpr(age + 1) + Tuple(age=30):
    left = eval(age, tuple) → 30
    right = eval(1, tuple) → 1
    return 30 + 1 → 31
```

**Type coercion**:
```java
INT + INT → INT
INT + BIGINT → BIGINT (promote INT)
INT + FLOAT → FLOAT (promote INT)
```

#### Comparison Operators
```java
ComparisonExpr(age > 25):
    left = eval(age, tuple) → 30
    right = eval(25, tuple) → 25
    return 30 > 25 → true
```

#### Logical Operators
```java
LogicalExpr(a AND b):
    left = eval(a, tuple)
    if (!left) return false;  // Short-circuit
    return eval(b, tuple);

LogicalExpr(a OR b):
    left = eval(a, tuple)
    if (left) return true;  // Short-circuit
    return eval(b, tuple);

NotExpr(expr):
    return !eval(expr, tuple);
```

### Short-Circuit Evaluation

**AND**:
```sql
WHERE expensive_check(x) AND cheap_check(y)
```

If `expensive_check` returns false, `cheap_check` is **never evaluated**.

**OR**:
```sql
WHERE cheap_check(y) OR expensive_check(x)
```

If `cheap_check` returns true, `expensive_check` is **never evaluated**.

**Optimization tip**: Put cheap predicates first!

---

## Data Flow Example

**Query**:
```sql
SELECT u.name, COUNT(*) 
FROM users u, orders o 
WHERE u.id = o.user_id 
GROUP BY u.name
```

**Physical plan**:
```
AggregateExec(groupBy=[u.name], aggs=[COUNT(*)])
  ↓
HashJoinExec(u.id = o.user_id)
  ├─ SeqScanExec(users AS u)
  └─ SeqScanExec(orders AS o)
```

**Execution trace**:

1. **AggregateExec.open()**:
   - Calls `HashJoinExec.open()`
   - Which calls `SeqScanExec(users).open()` and `SeqScanExec(orders).open()`
   - Builds hash table on orders

2. **AggregateExec consumes all tuples**:
   ```
   Loop: tuple = HashJoinExec.next()
       Extract group key: tuple.get("u.name")
       Increment count for that group
   ```

3. **HashJoinExec.next()** (called repeatedly):
   - Probes hash table with each user
   - Emits matching (user, order) pairs

4. **After consuming all input, AggregateExec.next()** emits results:
   - ("Alice", 5)  ← Alice has 5 orders
   - ("Bob", 3)    ← Bob has 3 orders

---

## Volcano Model Benefits

### Simplicity
Each operator has a **uniform interface**. Easy to understand and implement.

### Composability
Operators can be **arbitrarily nested**. Any operator can be a child of any other.

### Pipelining
Tuples flow through operators **without materialization** (except for blocking operators).

**Pipelined operators**: Filter, Project
**Blocking operators**: Aggregate, Sort, Hash Join (build phase)

### Low Memory
Only **one tuple in flight** at a time (for pipelined operators).

---

## Volcano Model Drawbacks

### Function Call Overhead
**Problem**: `next()` called once per tuple per operator.

**Example**: 1 million rows × 3 operators = 3 million function calls

**Mitigation**: Vectorized execution (not in EvolvDB M1-M11)

### Poor CPU Cache Locality
**Problem**: Switching between operators = cache misses

**Mitigation**: Compiled execution (not in EvolvDB M1-M11)

### No Parallelism
**Problem**: Single-threaded, one tuple at a time

**Mitigation**: Parallel operators (planned M23)

**Despite drawbacks**: Volcano model is still the **standard** for most databases due to simplicity and flexibility.

---

## Key Takeaways

### Volcano Iterator Model

- **Pull-based**: Parent pulls from child
- **Uniform interface**: open/next/close
- **Composable**: Arbitrary nesting
- **Tuple-at-a-time**: One tuple flows through pipeline

### Physical Operators

- **SeqScan**: Read table rows (O(n))
- **Filter**: Match predicate (O(n))
- **Project**: Compute expressions (O(n))
- **NestedLoopJoin**: Nested loops (O(n×m))
- **HashJoin**: Build hash + probe (O(n+m))
- **SortMergeJoin**: Sort + merge (O(n log n))
- **Aggregate**: Hash grouping (O(n))
- **Insert**: Write to table

### Expression Evaluation

- **Runtime evaluation**: AST expressions with tuple context
- **Type coercion**: Automatic for arithmetic
- **Short-circuit**: AND/OR optimize evaluation

---

## Self-Test Questions

1. **What does next() return when operator is exhausted?**
   - `null`

2. **Why buffer the right side in NestedLoopJoin?**
   - Need to iterate right side multiple times (once per left tuple)

3. **What's the difference between HashJoin and NestedLoopJoin?**
   - HashJoin: O(n+m), requires equi-join
   - NestedLoopJoin: O(n×m), works for any condition

4. **Is AggregateExec a blocking operator?**
   - Yes, must consume all input before emitting first output

5. **Why is Filter faster than NestedLoopJoin for same data?**
   - Filter: O(n) single pass
   - NestedLoopJoin: O(n×m) quadratic

6. **What happens if you forget to call close()?**
   - Resource leak (pages stay pinned in BufferPool)

---

## Next Steps

You now understand physical execution. In **Phase 7**, we'll explore query optimization:
- Why optimization matters
- Volcano optimizer architecture
- Cost model
- Join reordering
- Predicate pushdown

**Continue to**: [`phase-7-query-optimizer.md`](./phase-7-query-optimizer.md)
