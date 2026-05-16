# Phase 7: Query Optimizer (M11)

**Goal**: Understand how EvolvDB chooses the best execution plan.

**Prerequisites**: Phase 0-6 (Foundations through Physical Execution)

---

## Why Optimization Matters

### The Problem

Same query can execute in **vastly different ways**:

**Query**:
```sql
SELECT * FROM users u, orders o WHERE u.id = o.user_id AND o.amount > 100
```

**Plan A** (Bad):
```
Filter(o.amount > 100)
  ↓
NestedLoopJoin(u.id = o.user_id)     ← O(n×m)
  ├─ SeqScan(users)    [1M rows]
  └─ SeqScan(orders)   [10M rows]
```
**Cost**: 1M × 10M = **10 trillion comparisons** 😱

**Plan B** (Better):
```
HashJoin(u.id = o.user_id)           ← O(n+m)
  ├─ SeqScan(users)    [1M rows]
  └─ Filter(o.amount > 100)
       ↓
       SeqScan(orders)  [10M rows → 100K after filter]
```
**Cost**: 1M + 100K = **1.1 million operations** ✅

**Speedup**: ~10,000x faster! 🚀

**Optimizer's job**: Choose Plan B automatically.

---

## Query Optimization Levels

### Logical Optimization (Phase 5)

**Algorithm-independent** transformations:
- Predicate pushdown
- Projection pruning
- Join reordering

**Example**: Push filter below join
```
Before: Join → Filter(amount > 100)
After:  Join → Filter below join scan
```

### Physical Optimization (This Phase)

**Algorithm selection**:
- Which join algorithm? (NestedLoop vs Hash vs SortMerge)
- Which scan method? (SeqScan vs IndexScan - future M16)
- Which aggregate strategy? (Hash vs Sort)

### Cost-Based Optimization

**Use statistics** to estimate costs:
- Row counts
- Data distribution
- Index selectivity

**EvolvDB M11**: Simple cost model (no real statistics yet)

---

## Volcano Optimizer Architecture

**Module**: `evolvdb-exec`  
**Package**: `io.github.anupam.evolvdb.exec.optimizer`

### Components

```
Logical Plan
    ↓
Logical Rewriter (pushdowns, pruning)
    ↓
Volcano Optimizer
  ├─ Memo (equivalence classes)
  ├─ Cost Model (estimates)
  └─ Physical Rules (alternatives)
    ↓
Best Physical Plan
```

### Why "Volcano"?

Named after the **Volcano/Cascades** optimizer framework (1990s).

**Key ideas**:
1. **Memo structure**: Group equivalent expressions
2. **Top-down search**: Explore alternatives recursively
3. **Cost-based pruning**: Only explore promising alternatives
4. **Physical properties**: Request sorted output, etc.

**EvolvDB M11**: Simplified Volcano (bottom-up, no properties)

---

## The Memo Structure

### Purpose

**Avoid redundant work** by grouping equivalent expressions.

### Group

A **group** represents all equivalent ways to compute the same result.

**Example**:
```
Group G1: "Join users and orders"
  ├─ NestedLoopJoin(u, o)
  ├─ HashJoin(u, o)
  └─ SortMergeJoin(u, o)
```

All three produce the **same rows**, just with different algorithms.

### Equivalence

**What makes plans equivalent?**

1. **Same result set** (rows and columns)
2. **Possibly different order** (unless ORDER BY specified)

**Example equivalences**:
- Join commutativity: `A JOIN B` ≡ `B JOIN A`
- Associativity: `(A JOIN B) JOIN C` ≡ `A JOIN (B JOIN C)`
- Filter pushdown: `Filter(Join(A,B), pred)` ≡ `Join(Filter(A,pred), B)` (if safe)

### EvolvDB Memo (Simplified)

**EvolvDB M11** doesn't have a persistent memo, but uses **memo-like thinking**:

**For joins**:
- Automatically consider all join algorithms
- Automatically consider commutativity (`A JOIN B` vs `B JOIN A`)

**For other operators**:
- One physical implementation per logical operator

---

## Cost Model

**Module**: `evolvdb-exec`  
**Class**: `io.github.anupam.evolvdb.exec.optimizer.CostModel`

### Cost Vector

```java
class Cost {
    double rowCount;    // Estimated output rows
    double cpu;         // CPU cost (abstract units)
    double io;          // I/O cost (page reads)
    
    double total() {
        return cpu + io * IO_COST_FACTOR;
    }
}
```

**Total cost**: Weighted sum of CPU and I/O.

**Why separate?**
- I/O is typically **100-1000x** slower than CPU
- Can tune weight based on hardware (SSD vs HDD)

### Default Cost Model

**Class**: `DefaultCostModel`

**Assumptions** (no statistics in M11):
- Every table has **1000 rows** by default
- Filter selectivity: **0.1** (keeps 10% of rows)
- Join selectivity: **0.25** (25% of Cartesian product)

### Cost Estimation Rules

#### SeqScan

```java
Cost estimateSeqScan(TableMeta table) {
    double rows = DEFAULT_ROWS;  // 1000
    double cpu = rows * CPU_PER_TUPLE;
    double io = rows / TUPLES_PER_PAGE;  // Pages read
    return new Cost(rows, cpu, io);
}
```

**Interpretation**: Scan all rows, read all pages.

#### Filter

```java
Cost estimateFilter(Cost childCost, Expr predicate) {
    double selectivity = 0.1;  // Hardcoded for now
    double rows = childCost.rowCount * selectivity;
    double cpu = childCost.cpu + (childCost.rowCount * CPU_PER_PREDICATE);
    double io = childCost.io;  // Same I/O as child
    return new Cost(rows, cpu, io);
}
```

**Interpretation**: Keep 10% of rows, small CPU overhead per row.

#### Project

```java
Cost estimateProject(Cost childCost, List<Expr> projections) {
    double rows = childCost.rowCount;  // Same row count
    double cpu = childCost.cpu + (rows * projections.size() * CPU_PER_EXPR);
    double io = childCost.io;
    return new Cost(rows, cpu, io);
}
```

**Interpretation**: Same rows, small CPU per expression per row.

#### NestedLoopJoin

```java
Cost estimateNestedLoopJoin(Cost leftCost, Cost rightCost) {
    double selectivity = 0.25;
    double rows = leftCost.rowCount * rightCost.rowCount * selectivity;
    double cpu = leftCost.cpu + rightCost.cpu 
                 + (leftCost.rowCount * rightCost.rowCount * CPU_PER_COMPARE);
    double io = leftCost.io + (leftCost.rowCount * rightCost.io);
    return new Cost(rows, cpu, io);
}
```

**Key insight**: Right side read **once per left tuple**.

**I/O cost**: `left.io + (left.rows × right.io)`

**Example**:
- Left: 1000 rows, 10 pages
- Right: 100 rows, 1 page
- Total I/O: 10 + (1000 × 1) = **1010 page reads**

#### HashJoin

```java
Cost estimateHashJoin(Cost leftCost, Cost rightCost) {
    double selectivity = 0.25;
    double rows = leftCost.rowCount * rightCost.rowCount * selectivity;
    double cpu = leftCost.cpu + rightCost.cpu
                 + (leftCost.rowCount + rightCost.rowCount) * CPU_PER_HASH;
    double io = leftCost.io + rightCost.io;  // Read each side once
    return new Cost(rows, cpu, io);
}
```

**Key insight**: Read each side **once**.

**I/O cost**: `left.io + right.io`

**Same example**:
- Total I/O: 10 + 1 = **11 page reads**

**Speedup**: 1010 / 11 = ~92x faster on I/O!

#### SortMergeJoin

```java
Cost estimateSortMergeJoin(Cost leftCost, Cost rightCost) {
    double selectivity = 0.25;
    double rows = leftCost.rowCount * rightCost.rowCount * selectivity;
    
    // Sort cost: n log n
    double leftSortCost = leftCost.rowCount * Math.log(leftCost.rowCount);
    double rightSortCost = rightCost.rowCount * Math.log(rightCost.rowCount);
    
    double cpu = leftCost.cpu + rightCost.cpu + leftSortCost + rightSortCost
                 + (leftCost.rowCount + rightCost.rowCount) * CPU_PER_COMPARE;
    double io = leftCost.io + rightCost.io;
    return new Cost(rows, cpu, io);
}
```

**Key insight**: Sort overhead, but I/O same as hash join.

**When better than HashJoin**: If output needs to be sorted anyway.

#### Aggregate

```java
Cost estimateAggregate(Cost childCost, List<Expr> groupBy) {
    double distinctGroups = childCost.rowCount * 0.1;  // Assume 10% distinct
    double cpu = childCost.cpu + (childCost.rowCount * CPU_PER_HASH);
    double io = childCost.io;
    return new Cost(distinctGroups, cpu, io);
}
```

**Interpretation**: Hash-based grouping, output fewer rows.

---

## Logical Rewriter (Pre-Optimization)

**Module**: `evolvdb-exec`  
**Class**: `io.github.anupam.evolvdb.exec.optimizer.LogicalRewriter`

### Purpose

Apply **logical transformations** before physical optimization.

### Transformations

#### Predicate Pushdown

**Push filters down** through joins:

**Before**:
```
Filter(o.amount > 100)
  ↓
Join(u.id = o.user_id)
  ├─ Scan(users)
  └─ Scan(orders)
```

**After**:
```
Join(u.id = o.user_id)
  ├─ Scan(users)
  └─ Filter(o.amount > 100)
       ↓
       Scan(orders)
```

**Benefit**: Filter 10M orders down to 100K **before** join.

**Safety condition**: Predicate must only reference one side of join.

#### Projection Pruning

**Remove unused columns** early:

**Before**:
```
Project(name)
  ↓
Join(...)
  ├─ Scan(users)  [id, name, age, email, phone, ...]
  └─ Scan(orders)
```

**After**:
```
Project(name)
  ↓
Join(...)
  ├─ Project(id, name)  ← Only columns needed
       ↓
       Scan(users)
  └─ Scan(orders)
```

**Benefit**: Less data to move through operators.

#### Join Reordering

**Reorder joins** for better selectivity:

**Example**:
```sql
FROM users u, orders o, products p
WHERE u.id = o.user_id AND o.product_id = p.id AND u.country = 'US'
```

**Bad order**:
```
Join(o.product_id = p.id)
  ├─ Join(u.id = o.user_id)
  │   ├─ Scan(users)    [1M rows]
  │   └─ Scan(orders)   [10M rows]
  └─ Scan(products)
```
First join: 1M × 10M = huge intermediate result

**Good order**:
```
Join(o.product_id = p.id)
  ├─ Join(u.id = o.user_id)
  │   ├─ Filter(u.country = 'US')
  │   │    ↓
  │   │  Scan(users)    [1M → 100K after filter]
  │   └─ Scan(orders)   [10M rows]
  └─ Scan(products)
```
First join: 100K × 10M = much smaller

**Heuristic**: Join with most selective table first.

---

## Physical Rule-Based Optimization

**Module**: `evolvdb-exec`  
**Package**: `io.github.anupam.evolvdb.exec.optimizer`

### Rules

**Each logical operator → physical alternatives**

#### ScanRule

```
LogicalScan → SeqScanPlan
```

**Future (M16)**: Also generate `IndexScanPlan` if index exists

#### FilterRule

```
LogicalFilter → FilterPlan
```

**Only one physical implementation** for now.

#### ProjectRule

```
LogicalProject → ProjectPlan
```

**Only one physical implementation** for now.

#### JoinRule

```
LogicalJoin → [NestedLoopJoinPlan, HashJoinPlan, SortMergeJoinPlan]
```

**Generate all three**, let cost model choose.

**Additional**: Try both join orders (commutativity)
- `A JOIN B`
- `B JOIN A` (swap left and right)

#### AggregateRule

```
LogicalAggregate → AggregatePlan
```

**Future**: Hash vs Sort-based aggregate.

#### InsertRule

```
LogicalInsert → InsertPlan
```

**Straightforward**: Just execute insert.

---

## Optimization Algorithm

**Class**: `VolcanoOptimizer`

### Bottom-Up Optimization

```java
PhysicalPlan optimize(LogicalPlan logicalPlan) {
    // Base case: leaf node
    if (logicalPlan is LogicalScan) {
        return new SeqScanPlan(logicalPlan);
    }
    
    // Recursive case: optimize children first
    List<PhysicalPlan> optimizedChildren = new ArrayList<>();
    for (LogicalPlan child : logicalPlan.children()) {
        optimizedChildren.add(optimize(child));
    }
    
    // Generate physical alternatives for this node
    List<PhysicalPlan> alternatives = generateAlternatives(logicalPlan, optimizedChildren);
    
    // Pick lowest cost
    PhysicalPlan best = null;
    Cost bestCost = Cost.INFINITY;
    for (PhysicalPlan alt : alternatives) {
        Cost cost = costModel.estimate(alt);
        if (cost.total() < bestCost.total()) {
            best = alt;
            bestCost = cost;
        }
    }
    
    return best;
}
```

**Key insight**: Optimize children first, then choose best algorithm for parent.

### Join Algorithm Selection

**Example scenario**:
- Left: 1000 rows, 10 pages
- Right: 100 rows, 1 page
- Equi-join condition

**Cost estimates**:

**NestedLoopJoin**:
- I/O: 10 + (1000 × 1) = 1010 pages
- CPU: 1000 × 100 = 100,000 comparisons
- **Total**: High

**HashJoin**:
- I/O: 10 + 1 = 11 pages
- CPU: 1000 + 100 = 1,100 hashes + probes
- **Total**: Low ✅

**SortMergeJoin**:
- I/O: 10 + 1 = 11 pages
- CPU: 1000×log(1000) + 100×log(100) + 1100 = ~11,000
- **Total**: Medium

**Winner**: HashJoin (lowest total cost)

### Join Order Selection

**Example**:
```sql
FROM a, b, c WHERE a.id = b.fk1 AND b.id = c.fk2
```

**Possible orders**:
1. `((a JOIN b) JOIN c)`
2. `((b JOIN a) JOIN c)`
3. `((a JOIN c) JOIN b)`
4. `((b JOIN c) JOIN a)`
5. `((c JOIN a) JOIN b)`
6. `((c JOIN b) JOIN a)`

**With 3 tables**: 12 possible orders (considering commutativity)
**With N tables**: O(N!) explosion 😱

**EvolvDB M11 approach**: Greedy left-deep trees
- Join order from logical plan
- Consider commutativity per join
- Not optimal, but fast

**Future (M19)**: Dynamic programming for optimal join order

---

## Complete Optimization Example

**Query**:
```sql
SELECT u.name, COUNT(*) 
FROM users u, orders o 
WHERE u.id = o.user_id AND o.amount > 100 
GROUP BY u.name
```

### Step 1: Initial Logical Plan

```
LogicalAggregate(groupBy=[u.name], aggs=[COUNT(*)])
  ↓
LogicalProject([u.name, o.amount])
  ↓
LogicalFilter(u.id = o.user_id AND o.amount > 100)
  ↓
LogicalJoin(CROSS)
  ├─ LogicalScan(users AS u)
  └─ LogicalScan(orders AS o)
```

### Step 2: Logical Rewriter

**Extract join condition**:
```
LogicalAggregate(groupBy=[u.name], aggs=[COUNT(*)])
  ↓
LogicalFilter(o.amount > 100)
  ↓
LogicalJoin(INNER, u.id = o.user_id)
  ├─ LogicalScan(users AS u)
  └─ LogicalScan(orders AS o)
```

**Push filter below join**:
```
LogicalAggregate(groupBy=[u.name], aggs=[COUNT(*)])
  ↓
LogicalJoin(INNER, u.id = o.user_id)
  ├─ LogicalScan(users AS u)
  └─ LogicalFilter(o.amount > 100)
       ↓
       LogicalScan(orders AS o)
```

**Prune projection** (aggregate needs all columns anyway):
```
LogicalAggregate(groupBy=[u.name], aggs=[COUNT(*)])
  ↓
LogicalJoin(INNER, u.id = o.user_id)
  ├─ LogicalScan(users AS u)
  └─ LogicalFilter(o.amount > 100)
       ↓
       LogicalScan(orders AS o)
```

### Step 3: Physical Optimization (Bottom-Up)

**Optimize Scan(users)**:
- Only option: `SeqScanPlan(users)`
- Cost: 1000 rows, 10 pages

**Optimize Filter(orders)**:
- Child: `SeqScanPlan(orders)` - 10,000 rows, 100 pages
- `FilterPlan(amount > 100)` - 1,000 rows, 100 pages (same I/O)

**Optimize Join**:
- Left: SeqScanPlan(users) - 1000 rows
- Right: FilterPlan(orders) - 1000 rows

**Alternatives**:
1. NestedLoopJoin(left=users, right=orders_filtered)
2. NestedLoopJoin(left=orders_filtered, right=users)
3. HashJoin(left=users, right=orders_filtered)
4. HashJoin(left=orders_filtered, right=users)
5. SortMergeJoin(left=users, right=orders_filtered)
6. SortMergeJoin(left=orders_filtered, right=users)

**Cost estimates** (simplified):

1. NLJ(users, orders): I/O = 10 + 1000×100 = 100,010 ❌
2. NLJ(orders, users): I/O = 100 + 1000×10 = 10,100 ❌
3. HashJoin(users, orders): I/O = 10 + 100 = 110 ✅
4. HashJoin(orders, users): I/O = 100 + 10 = 110 ✅
5. SMJ(users, orders): I/O = 110, CPU higher ❌
6. SMJ(orders, users): I/O = 110, CPU higher ❌

**Winner**: HashJoin (either order, tie-breaker picks first)

**Optimize Aggregate**:
- Only option: `AggregatePlan(groupBy=[u.name])`
- Cost: Proportional to input rows

### Step 4: Final Physical Plan

```
AggregatePlan(groupBy=[u.name], aggs=[COUNT(*)])
  ↓
HashJoinPlan(u.id = o.user_id)
  ├─ SeqScanPlan(users AS u)
  └─ FilterPlan(o.amount > 100)
       ↓
       SeqScanPlan(orders AS o)
```

**Execution cost**: ~110 page reads vs 100,000+ with bad plan.

---

## When Optimizer Helps

### Scenario 1: Large Joins

**Problem**: Nested loop join is O(n×m)
**Solution**: HashJoin is O(n+m)
**Speedup**: 100-1000x

### Scenario 2: Filter Placement

**Problem**: Filter after join processes huge intermediate result
**Solution**: Push filter before join
**Speedup**: 10-100x

### Scenario 3: Join Order

**Problem**: Join large tables first
**Solution**: Join with most selective predicates first
**Speedup**: 10-100x

### Cumulative Effect

**Good optimization**: All three improvements
**Total speedup**: 1000-10,000x is realistic! 🚀

---

## Limitations of M11 Optimizer

### No Statistics

**Problem**: Assumes 1000 rows for every table
**Reality**: Tables can have 10 or 10 million rows

**Impact**: Can choose wrong plan for actual data distribution

**Future (M19)**: Collect real statistics (row counts, histograms)

### Fixed Selectivity

**Problem**: Assumes filter keeps 10%, join keeps 25%
**Reality**: Varies wildly by predicate

**Example**:
- `WHERE country = 'US'` → 33% selectivity
- `WHERE user_id = 12345` → 0.0001% selectivity

**Future (M19)**: Cardinality estimation with statistics

### Greedy Join Order

**Problem**: Considers limited reorderings
**Reality**: Optimal order may be very different

**Future (M19)**: Dynamic programming for optimal join order

### No Index Awareness

**Problem**: Always uses SeqScan
**Reality**: Index can speed up by 1000x

**Future (M16)**: IndexScan operator and index selection rules

---

## Key Takeaways

### Why Optimize?

Same query, different plans can differ by **10,000x** in performance.

### Cost-Based Optimization

1. **Generate alternatives** (all valid plans)
2. **Estimate costs** (statistics + cost model)
3. **Pick minimum cost**

### Volcano Optimizer

- **Memo structure**: Group equivalent plans
- **Bottom-up**: Optimize children first
- **Physical rules**: Generate alternatives
- **Cost model**: Select best

### Join Algorithm Selection

- **NestedLoopJoin**: O(n×m), always works
- **HashJoin**: O(n+m), equi-join only, usually fastest
- **SortMergeJoin**: O(n log n), good for sorted output

### Logical Transformations

- **Predicate pushdown**: Filter early
- **Projection pruning**: Remove unused columns
- **Join reordering**: Selective joins first

---

## Self-Test Questions

1. **Why is HashJoin faster than NestedLoopJoin?**
   - I/O: O(n+m) vs O(n×m), reads each side once vs many times

2. **When would you prefer SortMergeJoin over HashJoin?**
   - When output needs to be sorted anyway

3. **What does "predicate pushdown" mean?**
   - Move filters down the tree to reduce intermediate results

4. **Why does join order matter?**
   - Joining selective tables first produces smaller intermediates

5. **What's the main limitation of M11 optimizer?**
   - No real statistics, uses default estimates

6. **How does bottom-up optimization work?**
   - Optimize children first, then choose best algorithm for parent

---

## Next Steps

You now understand query optimization. In **Phase 8**, we'll do a complete end-to-end walkthrough:
- Trace a real query from SQL to results
- See every component in action
- Understand how all pieces fit together

**Continue to**: [`phase-8-query-walkthrough.md`](./phase-8-query-walkthrough.md)
