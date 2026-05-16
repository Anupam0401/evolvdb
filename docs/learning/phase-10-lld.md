# Phase 10: Low-Level Design (LLD)

**Goal**: Understand key classes, interfaces, design patterns, and implementation details.

**Prerequisites**: Phase 0-9 (Complete system understanding)

---

## Core Interfaces

### Physical Storage Layer

#### DiskManager Interface

```java
package io.github.anupam.evolvdb.storage.disk;

interface DiskManager {
    PageId allocatePage(FileId fileId);
    void readPage(PageId pageId, ByteBuffer dst);
    void writePage(PageId pageId, ByteBuffer src, long lsn);
    int pageCount(FileId fileId);
    void sync();
    void close();
}
```

**Key implementation**: `NioDiskManager`
- Uses `java.nio.FileChannel` for I/O
- File naming: `{dataDir}/{fileName}.evolv`
- Synchronized per FileChannel for thread safety

#### PageFormat Interface

```java
package io.github.anupam.evolvdb.storage.page;

interface PageFormat {
    void init(Page page);
    int freeSpace(Page page);
    RecordId insert(Page page, byte[] record);
    byte[] read(Page page, RecordId rid);
    void delete(Page page, RecordId rid);
    boolean update(Page page, RecordId rid, byte[] newRecord);
    int slotCount(Page page);
    boolean isLive(Page page, short slot);
}
```

**Key implementation**: `SlottedPageFormat`
- Header: pageType, lsn, slotCount, freeStartOffset
- Slot directory grows backward from page end
- Payload grows forward from header
- Compaction when fragmented

#### EvictionPolicy Interface

```java
package io.github.anupam.evolvdb.storage.buffer;

interface EvictionPolicy {
    void onInsert(PageId pageId);
    void onAccess(PageId pageId);
    void onRemove(PageId pageId);
    PageId evictCandidate(Predicate<PageId> canEvict);
}
```

**Key implementation**: `LruEvictionPolicy`
- Uses `LinkedHashMap` with access-order
- O(1) insert, access, remove
- O(n) eviction (iterate until `canEvict` returns true)

---

## Type System & Data Representation

### Type System Classes

```java
package io.github.anupam.evolvdb.types;

enum Type {
    INT(4, true),
    BIGINT(8, true),
    BOOLEAN(1, true),
    FLOAT(4, true),
    VARCHAR(-1, false),  // Variable-width
    STRING(-1, false);   // Variable-width
    
    private final int fixedWidth;
    private final boolean fixed;
}

record ColumnMeta(String name, Type type, Integer length) {
    // Validation in constructor
}

class Schema {
    private final List<ColumnMeta> columns;
    private final Map<String, Integer> nameToIndex;  // Case-insensitive
    
    // Methods: getColumn, indexOf, columnCount
}
```

### Tuple & Encoding

```java
class Tuple {
    private final Schema schema;
    private final List<Object> values;  // Immutable
    
    Tuple(Schema schema, List<Object> values) {
        // Validate size and types
    }
    
    Object get(int index);
    Object get(String columnName);
}

class RowCodec {
    static byte[] encode(Schema schema, Tuple tuple) {
        ByteBuffer buf = ByteBuffer.allocate(estimateSize(schema, tuple));
        buf.order(ByteOrder.LITTLE_ENDIAN);
        
        // Encode fixed-width fields first
        for (ColumnMeta col : schema.fixedWidthColumns()) {
            encodeValue(buf, col.type(), tuple.get(col.name()));
        }
        
        // Encode variable-width fields
        for (ColumnMeta col : schema.varWidthColumns()) {
            String str = (String) tuple.get(col.name());
            byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
            buf.putShort((short) bytes.length);
            buf.put(bytes);
        }
        
        return Arrays.copyOf(buf.array(), buf.position());
    }
    
    static Tuple decode(Schema schema, byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        buf.order(ByteOrder.LITTLE_ENDIAN);
        
        List<Object> values = new ArrayList<>();
        
        // Decode in schema order
        for (ColumnMeta col : schema.columns()) {
            values.add(decodeValue(buf, col.type()));
        }
        
        return new Tuple(schema, values);
    }
}
```

---

## SQL & Planning Layer

### AST Visitor Pattern

```java
package io.github.anupam.evolvdb.sql.ast;

interface StatementVisitor<R> {
    R visit(CreateTable stmt);
    R visit(DropTable stmt);
    R visit(Insert stmt);
    R visit(Select stmt);
}

interface ExprVisitor<R> {
    R visit(BinaryExpr expr);
    R visit(ComparisonExpr expr);
    R visit(LogicalExpr expr);
    R visit(NotExpr expr);
    R visit(ColumnRef expr);
    R visit(Literal expr);
}

// Each AST node implements accept method
class Select implements Statement {
    <R> R accept(StatementVisitor<R> visitor) {
        return visitor.visit(this);
    }
}
```

**Usage example**: Column collector
```java
class ColumnCollector implements ExprVisitor<Set<String>> {
    public Set<String> visit(ColumnRef ref) {
        return Set.of(ref.name());
    }
    
    public Set<String> visit(BinaryExpr expr) {
        Set<String> result = new HashSet<>();
        result.addAll(expr.left().accept(this));
        result.addAll(expr.right().accept(this));
        return result;
    }
    // ... other visits
}
```

### Logical Plan Tree

```java
package io.github.anupam.evolvdb.planner.logical;

interface LogicalPlan {
    Schema schema();
    List<LogicalPlan> children();
    <R> R accept(LogicalPlanVisitor<R> visitor);
}

class LogicalScan implements LogicalPlan {
    private final String tableName;
    private final String alias;
    private final Schema schema;
    
    // Immutable, constructed with all fields
}

class LogicalFilter implements LogicalPlan {
    private final LogicalPlan child;
    private final Expr predicate;
    private final Schema schema;  // Same as child
}

class LogicalJoin implements LogicalPlan {
    private final LogicalPlan left;
    private final LogicalPlan right;
    private final JoinType type;
    private final Expr condition;
    private final Schema schema;  // Concatenation of left and right
}
```

---

## Execution Layer

### PhysicalOperator Template

```java
package io.github.anupam.evolvdb.exec.op;

abstract class PhysicalOperator {
    protected final Schema schema;
    
    // Template method pattern
    abstract void open();
    abstract Tuple next();
    abstract void close();
    
    Schema schema() {
        return schema;
    }
}

class SeqScanExec extends PhysicalOperator {
    private final Table table;
    private Iterator<Tuple> iterator;
    
    void open() {
        iterator = table.scanTuples().iterator();
    }
    
    Tuple next() {
        return iterator.hasNext() ? iterator.next() : null;
    }
    
    void close() {
        // No resources to release
    }
}
```

### Expression Evaluation Strategy

```java
package io.github.anupam.evolvdb.exec.expr;

class ExprEvaluator {
    Object eval(Expr expr, Tuple tuple) {
        return expr.accept(new ExprEvalVisitor(tuple));
    }
    
    private class ExprEvalVisitor implements ExprVisitor<Object> {
        private final Tuple context;
        
        public Object visit(Literal lit) {
            return lit.value();
        }
        
        public Object visit(ColumnRef ref) {
            if (ref.qualifier() != null) {
                return context.get(ref.qualifier() + "." + ref.name());
            }
            return context.get(ref.name());
        }
        
        public Object visit(BinaryExpr expr) {
            Object left = expr.left().accept(this);
            Object right = expr.right().accept(this);
            return applyBinaryOp(expr.op(), left, right);
        }
        
        public Object visit(ComparisonExpr expr) {
            Object left = expr.left().accept(this);
            Object right = expr.right().accept(this);
            return applyComparison(expr.op(), left, right);
        }
        
        public Object visit(LogicalExpr expr) {
            if (expr.op() == LogicalOp.AND) {
                Object left = expr.left().accept(this);
                if (!Boolean.TRUE.equals(left)) return false;
                return expr.right().accept(this);
            } else { // OR
                Object left = expr.left().accept(this);
                if (Boolean.TRUE.equals(left)) return true;
                return expr.right().accept(this);
            }
        }
    }
}
```

---

## Optimizer Architecture

### Cost Model Interface

```java
package io.github.anupam.evolvdb.exec.optimizer;

interface CostModel {
    Cost estimate(PhysicalPlan plan);
}

class Cost {
    final double rowCount;
    final double cpu;
    final double io;
    
    double total() {
        return cpu + io * IO_COST_WEIGHT;
    }
    
    Cost add(Cost other) {
        return new Cost(
            rowCount + other.rowCount,
            cpu + other.cpu,
            io + other.io
        );
    }
}

class DefaultCostModel implements CostModel {
    private static final double DEFAULT_ROWS = 1000;
    private static final double FILTER_SELECTIVITY = 0.1;
    private static final double JOIN_SELECTIVITY = 0.25;
    
    Cost estimate(PhysicalPlan plan) {
        return plan.accept(new CostVisitor());
    }
    
    private class CostVisitor implements PhysicalPlanVisitor<Cost> {
        public Cost visit(SeqScanPlan plan) {
            return new Cost(
                DEFAULT_ROWS,
                DEFAULT_ROWS * CPU_PER_TUPLE,
                DEFAULT_ROWS / TUPLES_PER_PAGE
            );
        }
        
        public Cost visit(HashJoinPlan plan) {
            Cost leftCost = plan.left().accept(this);
            Cost rightCost = plan.right().accept(this);
            
            double rows = leftCost.rowCount * rightCost.rowCount * JOIN_SELECTIVITY;
            double cpu = leftCost.cpu + rightCost.cpu 
                       + (leftCost.rowCount + rightCost.rowCount) * CPU_PER_HASH;
            double io = leftCost.io + rightCost.io;
            
            return new Cost(rows, cpu, io);
        }
        // ... other visits
    }
}
```

### Volcano Optimizer Algorithm

```java
class VolcanoOptimizer {
    private final CostModel costModel;
    
    PhysicalPlan optimize(LogicalPlan logical) {
        // Base case: leaf nodes
        if (logical instanceof LogicalScan) {
            return new SeqScanPlan((LogicalScan) logical);
        }
        
        // Recursive: optimize children first
        List<PhysicalPlan> optChildren = new ArrayList<>();
        for (LogicalPlan child : logical.children()) {
            optChildren.add(optimize(child));
        }
        
        // Generate alternatives for this node
        List<PhysicalPlan> alternatives = generateAlternatives(logical, optChildren);
        
        // Pick lowest cost
        return alternatives.stream()
            .min(Comparator.comparing(p -> costModel.estimate(p).total()))
            .orElseThrow();
    }
    
    private List<PhysicalPlan> generateAlternatives(LogicalPlan logical, List<PhysicalPlan> children) {
        if (logical instanceof LogicalJoin) {
            LogicalJoin join = (LogicalJoin) logical;
            PhysicalPlan left = children.get(0);
            PhysicalPlan right = children.get(1);
            
            return List.of(
                new NestedLoopJoinPlan(left, right, join.condition()),
                new NestedLoopJoinPlan(right, left, join.condition()),  // Commuted
                new HashJoinPlan(left, right, join.condition()),
                new HashJoinPlan(right, left, join.condition()),
                new SortMergeJoinPlan(left, right, join.condition()),
                new SortMergeJoinPlan(right, left, join.condition())
            );
        }
        // ... other operators
    }
}
```

---

## Design Patterns Used

### 1. Strategy Pattern

**Purpose**: Pluggable algorithms

**Examples**:
- `EvictionPolicy`: LRU, CLOCK (future)
- `PageFormat`: Slotted, Fixed (future)
- `CostModel`: Default, StatsBased (future)

**Structure**:
```java
interface Strategy {
    Result execute(Input input);
}

class Context {
    private Strategy strategy;
    
    void setStrategy(Strategy s) {
        this.strategy = s;
    }
    
    Result doSomething(Input input) {
        return strategy.execute(input);
    }
}
```

### 2. Visitor Pattern

**Purpose**: Operations on tree structures

**Examples**:
- AST traversal (validation, binding)
- Logical plan traversal (optimization)
- Expression evaluation

**Structure**:
```java
interface Visitor<R> {
    R visit(NodeA node);
    R visit(NodeB node);
}

interface Node {
    <R> R accept(Visitor<R> visitor);
}

class NodeA implements Node {
    public <R> R accept(Visitor<R> visitor) {
        return visitor.visit(this);
    }
}
```

### 3. Iterator Pattern

**Purpose**: Sequential access without exposing internals

**Examples**:
- HeapFile scan
- Volcano operators (open/next/close)

**Structure**:
```java
interface Iterator<T> {
    boolean hasNext();
    T next();
}

class HeapFile {
    Iterator<Tuple> scan() {
        return new HeapFileIterator();
    }
}
```

### 4. Factory Pattern

**Purpose**: Object creation abstraction

**Examples**:
- RecordManager (creates HeapFiles)
- PhysicalPlan.create() (creates operators)

**Structure**:
```java
class Factory {
    Product create(Specification spec) {
        // Construction logic
        return new ConcreteProduct();
    }
}
```

### 5. Template Method Pattern

**Purpose**: Define algorithm skeleton

**Examples**:
- PhysicalOperator (open/next/close lifecycle)

**Structure**:
```java
abstract class AbstractClass {
    // Template method
    final void algorithm() {
        step1();
        step2();
        step3();
    }
    
    abstract void step1();
    abstract void step2();
    protected void step3() {
        // Default implementation
    }
}
```

### 6. Facade Pattern

**Purpose**: Simplified interface to complex system

**Examples**:
- `Database` class (hides all subsystems)
- `Table` class (wraps HeapFile + RowCodec)

**Structure**:
```java
class Facade {
    private SubsystemA a;
    private SubsystemB b;
    private SubsystemC c;
    
    void simpleOperation() {
        a.operation1();
        b.operation2();
        c.operation3();
    }
}
```

### 7. Builder Pattern

**Purpose**: Construct complex objects step-by-step

**Examples**:
- Schema construction
- AST node construction (in parser)

**Structure**:
```java
class Builder {
    private Field1 field1;
    private Field2 field2;
    
    Builder withField1(Field1 f) {
        this.field1 = f;
        return this;
    }
    
    Product build() {
        validate();
        return new Product(field1, field2);
    }
}
```

---

## SOLID Principles Application

### Single Responsibility Principle (SRP)

**Each class has one reason to change**:

- `DiskManager`: Only changes if I/O strategy changes
- `BufferPool`: Only changes if caching strategy changes
- `RowCodec`: Only changes if encoding format changes
- `Binder`: Only changes if name resolution rules change

### Open/Closed Principle (OCP)

**Open for extension, closed for modification**:

- New `EvictionPolicy` implementations without changing `BufferPool`
- New `PhysicalOperator` types without changing executor
- New `Rule` implementations without changing `RuleEngine`

### Liskov Substitution Principle (LSP)

**Subtypes are substitutable**:

- Any `EvictionPolicy` can replace `LruEvictionPolicy`
- Any `PageFormat` can replace `SlottedPageFormat`
- Any `DiskManager` can replace `NioDiskManager`

### Interface Segregation Principle (ISP)

**Clients depend on minimal interfaces**:

- `PhysicalOperator` exposes only `open/next/close/schema`
- `PageFormat` exposes only page-level operations
- No fat interfaces with unused methods

### Dependency Inversion Principle (DIP)

**Depend on abstractions, not concretions**:

- `BufferPool` depends on `DiskManager` interface
- `HeapFile` depends on `PageFormat` interface
- `Optimizer` depends on `CostModel` interface

---

## Important Algorithms

### 1. LRU Eviction

```java
class LruEvictionPolicy {
    private LinkedHashMap<PageId, Void> accessOrder = 
        new LinkedHashMap<>(16, 0.75f, true);  // Access order
    
    PageId evictCandidate(Predicate<PageId> canEvict) {
        for (PageId pageId : accessOrder.keySet()) {
            if (canEvict.test(pageId)) {
                return pageId;
            }
        }
        return null;  // All pinned
    }
}
```

**Complexity**: O(n) worst case, typically O(1) amortized

### 2. Slotted Page Insert

```java
RecordId insert(Page page, byte[] record) {
    int slotCount = page.getShort(OFF_SLOT_COUNT);
    int freeStart = page.getShort(OFF_FREE_START);
    
    int required = record.length + SLOT_ENTRY_SIZE;
    if (freeSpace(page) < required) {
        compact(page);  // Try compaction
        if (freeSpace(page) < required) {
            throw new PageFullException();
        }
    }
    
    // Copy record to payload area
    page.position(freeStart);
    page.put(record);
    
    // Add slot entry at end
    int slotOffset = pageSize - (slotCount + 1) * SLOT_ENTRY_SIZE;
    page.putShort(slotOffset, (short) freeStart);
    page.putShort(slotOffset + 2, (short) record.length);
    
    // Update header
    page.putShort(OFF_SLOT_COUNT, (short) (slotCount + 1));
    page.putShort(OFF_FREE_START, (short) (freeStart + record.length));
    
    return new RecordId(pageId, (short) slotCount);
}
```

**Complexity**: O(1) without compaction, O(records) with compaction

### 3. Hash Join Build Phase

```java
void buildHashTable(PhysicalOperator right, Expr rightKey) {
    Map<Object, List<Tuple>> hashTable = new HashMap<>();
    
    right.open();
    try {
        while (true) {
            Tuple tuple = right.next();
            if (tuple == null) break;
            
            Object key = evaluator.eval(rightKey, tuple);
            hashTable.computeIfAbsent(key, k -> new ArrayList<>())
                     .add(tuple);
        }
    } finally {
        right.close();
    }
    
    return hashTable;
}
```

**Complexity**: O(m) where m = right side rows

### 4. Predicate Pushdown

```java
LogicalPlan pushdown(LogicalPlan plan) {
    if (plan instanceof LogicalFilter) {
        LogicalFilter filter = (LogicalFilter) plan;
        if (filter.child() instanceof LogicalJoin) {
            LogicalJoin join = (LogicalJoin) filter.child();
            
            // Split predicates by side
            List<Expr> leftPreds = predicatesReferencingOnly(filter.predicate(), join.left());
            List<Expr> rightPreds = predicatesReferencingOnly(filter.predicate(), join.right());
            List<Expr> joinPreds = predicatesReferencingBoth(filter.predicate(), join);
            
            // Push down to appropriate side
            LogicalPlan newLeft = leftPreds.isEmpty() ? join.left() 
                : new LogicalFilter(join.left(), and(leftPreds));
            LogicalPlan newRight = rightPreds.isEmpty() ? join.right()
                : new LogicalFilter(join.right(), and(rightPreds));
            
            LogicalPlan newJoin = new LogicalJoin(newLeft, newRight, join.type(), join.condition());
            
            return joinPreds.isEmpty() ? newJoin 
                : new LogicalFilter(newJoin, and(joinPreds));
        }
    }
    return plan;
}
```

---

## Code Organization

### Package Structure

```
io.github.anupam.evolvdb/
├── common/               # Exceptions, utilities
├── config/               # Configuration
├── types/                # Type system, Schema, Tuple
├── storage/
│   ├── disk/            # DiskManager
│   ├── buffer/          # BufferPool, eviction
│   ├── page/            # PageFormat
│   └── record/          # HeapFile, RecordManager
├── catalog/              # CatalogManager, TableMeta
├── sql/
│   ├── parser/          # Tokenizer, Parser
│   ├── ast/             # AST nodes
│   └── validate/        # AstValidator
├── planner/
│   ├── analyzer/        # Binder, Analyzer
│   ├── logical/         # Logical plan nodes
│   └── rules/           # Logical rules
├── exec/
│   ├── op/              # Physical operators
│   ├── expr/            # Expression evaluator
│   ├── plan/            # Physical plan nodes
│   └── optimizer/       # Volcano optimizer, cost model
├── core/                 # Database facade
└── cli/                  # Command-line interface
```

---

## Key Takeaways

### Design Patterns

Seven patterns heavily used:
1. **Strategy**: Pluggable algorithms
2. **Visitor**: Tree traversal
3. **Iterator**: Sequential access
4. **Factory**: Object creation
5. **Template Method**: Algorithm skeleton
6. **Facade**: Simplified interface
7. **Builder**: Complex construction

### SOLID Principles

All five principles applied consistently:
- **S**RP: One responsibility per class
- **O**CP: Open for extension
- **L**SP: Substitutable subtypes
- **I**SP: Minimal interfaces
- **D**IP: Depend on abstractions

### Code Quality

- **Immutability**: Tuple, Schema, AST nodes
- **Validation**: Constructor preconditions
- **Error handling**: Specific exception types
- **Thread safety**: Synchronized where needed (coarse-grained in M11)

---

## Next Steps

You now understand the low-level design. In **Phase 11**, we'll explore what's missing and why:
- Indexes (B+Tree)
- Transactions
- Durability (WAL)
- NULL support
- How they would integrate

**Continue to**: [`phase-11-missing-features.md`](./phase-11-missing-features.md)
