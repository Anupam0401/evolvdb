# Phase 4: SQL Frontend (M8)

**Goal**: Understand how SQL text becomes a structured Abstract Syntax Tree (AST).

**Prerequisites**: Phase 0-3 (Foundations + Storage + Types)

---

## Overview: SQL Processing Pipeline

```
SQL String: "SELECT name FROM users WHERE age > 25"
    ↓
Tokenizer: Break into tokens
    ↓
Parser: Build Abstract Syntax Tree
    ↓
Validator: Check semantic correctness
    ↓
AST: Structured representation ready for planning
```

---

## What Is Parsing?

**Parsing** converts unstructured text into a structured tree that represents the query's meaning.

**Analogy**: Reading an English sentence
- Text: "The cat sat on the mat"
- Structure: Subject (cat) + Verb (sat) + Prepositional phrase (on the mat)

**SQL parsing**:
- Text: `SELECT name FROM users WHERE age > 25`
- Structure: SELECT statement with projection (name), table (users), condition (age > 25)

---

## Stage 1: Tokenization (Lexical Analysis)

**Module**: `evolvdb-sql`  
**Class**: `io.github.anupam.evolvdb.sql.parser.Tokenizer`

### Purpose

Break SQL string into **tokens** (atomic units).

**Example**:
```sql
SELECT name FROM users WHERE age > 25
```

**Tokens**:
```
[SELECT] [name] [FROM] [users] [WHERE] [age] [>] [25]
```

### Token Types

```java
enum TokenType {
    // Keywords
    SELECT, FROM, WHERE, INSERT, INTO, VALUES,
    CREATE, TABLE, DROP, AND, OR, NOT,
    
    // Operators
    PLUS, MINUS, STAR, SLASH,
    EQ, NEQ, LT, LTE, GT, GTE,
    
    // Literals
    NUMBER, STRING, TRUE, FALSE,
    
    // Identifiers
    IDENTIFIER,
    
    // Punctuation
    LPAREN, RPAREN, COMMA, SEMICOLON,
    
    // Special
    EOF
}
```

### Token Structure

```java
record Token(
    TokenType type,
    String text,
    int position
) {}
```

**Example**:
```java
new Token(SELECT, "SELECT", 0)
new Token(IDENTIFIER, "name", 7)
new Token(FROM, "FROM", 12)
new Token(IDENTIFIER, "users", 17)
```

### Tokenization Rules

**Keywords**: Case-insensitive
- `SELECT`, `select`, `SeLeCt` → `TokenType.SELECT`

**Identifiers**: Letters, digits, underscore
- `user_id`, `table1`, `_temp` → `TokenType.IDENTIFIER`

**Numbers**: Integer literals
- `42`, `1000` → `TokenType.NUMBER`

**Strings**: Single-quoted
- `'Alice'`, `'Hello World'` → `TokenType.STRING`
- Escaped quotes: `'It''s working'` → "It's working"

**Operators**: Multi-character support
- `<=` → `TokenType.LTE`
- `<>` or `!=` → `TokenType.NEQ`

### Position Tracking

Each token tracks its **character position** in the original SQL string.

**Why?**
- Error reporting: "Syntax error at position 23"
- IDE integration: Highlight errors in the editor

### Example Tokenization

**Input**:
```sql
SELECT id, name FROM users WHERE age >= 18
```

**Output**:
```
Token(SELECT, "SELECT", 0)
Token(IDENTIFIER, "id", 7)
Token(COMMA, ",", 9)
Token(IDENTIFIER, "name", 11)
Token(FROM, "FROM", 16)
Token(IDENTIFIER, "users", 21)
Token(WHERE, "WHERE", 27)
Token(IDENTIFIER, "age", 33)
Token(GTE, ">=", 37)
Token(NUMBER, "18", 40)
Token(EOF, "", 42)
```

---

## Stage 2: Parsing (Syntax Analysis)

**Module**: `evolvdb-sql`  
**Class**: `io.github.anupam.evolvdb.sql.parser.SqlParser`

### Purpose

Convert token stream into **Abstract Syntax Tree (AST)**.

### Parsing Strategy: Recursive Descent

**Recursive descent** is a top-down parsing technique where:
- Each grammar rule becomes a function
- Functions call each other recursively

**Example grammar rule**:
```
selectStmt := SELECT selectItems FROM tableRefs [WHERE expr]
```

**Becomes function**:
```java
SelectStmt parseSelectStmt() {
    expect(SELECT);
    List<SelectItem> items = parseSelectItems();
    expect(FROM);
    List<TableRef> tables = parseTableRefs();
    Expr where = null;
    if (match(WHERE)) {
        where = parseExpr();
    }
    return new SelectStmt(items, tables, where);
}
```

### Grammar Hierarchy

**Statements** (top-level):
```
statement := createTable | dropTable | insert | select
```

**Expressions** (recursive, operator precedence):
```
expr         := orExpr
orExpr       := andExpr (OR andExpr)*
andExpr      := notExpr (AND notExpr)*
notExpr      := [NOT] comparisonExpr
comparisonExpr := addExpr [(= | != | < | <= | > | >=) addExpr]
addExpr      := mulExpr ((+ | -) mulExpr)*
mulExpr      := primary ((* | /) primary)*
primary      := number | string | TRUE | FALSE | identifier | ( expr )
```

**Why this hierarchy?**
- Encodes **operator precedence**
- `*` binds tighter than `+`
- `AND` binds tighter than `OR`

**Example**: `a OR b AND c` parses as `a OR (b AND c)`

### Abstract Syntax Tree (AST)

The AST is a **tree of nodes** representing the query structure.

**Node types**:

```java
// Top-level statements
interface Statement {}
class CreateTable implements Statement { ... }
class DropTable implements Statement { ... }
class Insert implements Statement { ... }
class Select implements Statement { ... }

// Expressions
interface Expr {}
class BinaryExpr implements Expr { Expr left, Op op, Expr right; }
class ComparisonExpr implements Expr { Expr left, CompOp op, Expr right; }
class LogicalExpr implements Expr { Expr left, LogicalOp op, Expr right; }
class NotExpr implements Expr { Expr expr; }
class ColumnRef implements Expr { String qualifier, String name; }
class Literal implements Expr { Object value; }

// Other structures
record SelectItem(Expr expr, String alias) {}
record TableRef(String name, String alias) {}
```

### AST Example

**SQL**:
```sql
SELECT name FROM users WHERE age > 25
```

**AST**:
```
Select
├─ selectItems: [SelectItem(ColumnRef("name"), null)]
├─ tables: [TableRef("users", null)]
└─ where: ComparisonExpr(
      left: ColumnRef("age"),
      op: GT,
      right: Literal(25)
   )
```

**Tree visualization**:
```
           Select
          /   |   \
    [name]  users  (age > 25)
                      /  |  \
                   age  GT  25
```

---

## Stage 3: Validation

**Module**: `evolvdb-sql`  
**Class**: `io.github.anupam.evolvdb.sql.validate.AstValidator`

### Purpose

Check **semantic correctness** before execution.

**Syntax vs. Semantics**:
- **Syntax**: "Is this grammatically correct SQL?"
- **Semantics**: "Does this query make sense?"

### Validation Rules

#### CREATE TABLE

✅ **Valid**:
```sql
CREATE TABLE users (id INT, name VARCHAR(50))
```

❌ **Invalid**:
```sql
CREATE TABLE users (id INT, id VARCHAR(50))  -- Duplicate column
CREATE TABLE users (id INT, name VARCHAR(-5))  -- Invalid length
CREATE TABLE users (id VARCHAR(50))  -- VARCHAR without length
CREATE TABLE users (id INT(10))  -- INT with length (not allowed)
```

#### INSERT

✅ **Valid**:
```sql
INSERT INTO users (id, name) VALUES (1, 'Alice')
INSERT INTO users VALUES (1, 'Alice', 30)  -- All columns
```

❌ **Invalid**:
```sql
INSERT INTO nonexistent VALUES (1)  -- Table doesn't exist
INSERT INTO users (id, invalid) VALUES (1, 2)  -- Column doesn't exist
INSERT INTO users (id, name) VALUES (1)  -- Arity mismatch
INSERT INTO users (id) VALUES ('Alice')  -- Type mismatch
```

#### SELECT

✅ **Valid**:
```sql
SELECT id, name FROM users
SELECT u.id FROM users u
SELECT * FROM users WHERE age > 18
```

❌ **Invalid**:
```sql
SELECT name FROM nonexistent  -- Table doesn't exist
SELECT invalid FROM users  -- Column doesn't exist
SELECT u.name FROM users v  -- Qualifier mismatch
SELECT id FROM users u, orders o WHERE x.id = 1  -- Invalid qualifier 'x'
```

#### DROP TABLE

✅ **Valid**:
```sql
DROP TABLE users
```

❌ **Invalid**:
```sql
DROP TABLE nonexistent  -- Table doesn't exist
```

### Catalog Integration

Validator uses `CatalogManager` to check:
- Table existence
- Column existence
- Schema structure

```java
class AstValidator {
    CatalogManager catalog;
    
    void validate(Statement stmt) {
        if (stmt instanceof Select) {
            validateSelect((Select) stmt);
        } else if (stmt instanceof Insert) {
            validateInsert((Insert) stmt);
        }
        // ...
    }
    
    void validateSelect(Select stmt) {
        for (TableRef table : stmt.tables()) {
            Optional<TableMeta> meta = catalog.getTable(table.name());
            if (meta.isEmpty()) {
                throw new ValidationException("Table not found: " + table.name());
            }
        }
        // Validate column references...
    }
}
```

### Type Compatibility Checking

**Literal type inference**:
- `42` → INT
- `1000000000000` → BIGINT (too large for INT)
- `'Alice'` → STRING
- `TRUE`, `FALSE` → BOOLEAN

**Type compatibility rules**:
- INT column can accept INT literal ✅
- VARCHAR column can accept STRING literal ✅
- INT column cannot accept STRING literal ❌

**Example validation**:
```sql
INSERT INTO users (id, name) VALUES ('Alice', 42)
```

**Error**: Type mismatch
- Column `id` is INT, but value `'Alice'` is STRING
- Column `name` is VARCHAR, but value `42` is INT

---

## Supported SQL Subset (M8)

### DDL (Data Definition Language)

**CREATE TABLE**:
```sql
CREATE TABLE table_name (
    column1 type1,
    column2 type2,
    ...
)
```

**Types**: INT, BIGINT, BOOLEAN, FLOAT, VARCHAR(N), STRING

**DROP TABLE**:
```sql
DROP TABLE table_name
```

### DML (Data Manipulation Language)

**INSERT**:
```sql
INSERT INTO table_name VALUES (val1, val2, ...)
INSERT INTO table_name (col1, col2) VALUES (val1, val2)
INSERT INTO table_name VALUES (1, 'a'), (2, 'b'), (3, 'c')  -- Multi-row
```

**SELECT**:
```sql
SELECT column1, column2 FROM table_name
SELECT * FROM table_name
SELECT expr1 AS alias1, expr2 FROM table_name
SELECT col FROM table1, table2 WHERE condition  -- Implicit join
SELECT t1.col FROM table1 t1, table2 t2 WHERE t1.id = t2.fk  -- Qualified refs
```

**WHERE clause** supports:
- Comparisons: `=`, `!=`, `<>`, `<`, `<=`, `>`, `>=`
- Logical: `AND`, `OR`, `NOT`
- Arithmetic: `+`, `-`, `*`, `/`
- Parentheses for grouping

**GROUP BY and aggregates**:
```sql
SELECT dept, COUNT(*), SUM(salary) FROM employees GROUP BY dept
```

**Aggregates**: COUNT, SUM, AVG, MIN, MAX

### What's NOT Supported (Yet)

❌ UPDATE, DELETE (planned M12)  
❌ ANSI JOIN syntax (`JOIN...ON`)  
❌ OUTER JOIN  
❌ ORDER BY, LIMIT, OFFSET  
❌ HAVING  
❌ Subqueries  
❌ UNION, INTERSECT, EXCEPT  
❌ DISTINCT  
❌ Window functions  
❌ CTEs (WITH)  

---

## Error Handling

### Syntax Errors

**Example**:
```sql
SELECT FROM users  -- Missing select items
```

**Error**:
```
Syntax error at position 7: Expected identifier or *, found FROM
```

### Semantic Errors

**Example**:
```sql
SELECT invalid_column FROM users
```

**Error**:
```
Validation error: Column 'invalid_column' not found in table 'users'
Available columns: id, name, age
```

### Position Tracking

Errors include **character position** for IDE integration:

```java
class ParserException extends RuntimeException {
    int position;
    String sql;
}
```

Allows highlighting the exact error location in the editor.

---

## Visitor Pattern for AST Traversal

**Why Visitor?**
- AST has many node types
- Many operations need to traverse the tree (validation, planning, optimization)
- Visitor separates traversal logic from node definitions

**Interface**:
```java
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
    R visit(ColumnRef expr);
    R visit(Literal expr);
    // ...
}
```

**Usage**:
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
    // ...
}
```

---

## Complete Example

**SQL**:
```sql
CREATE TABLE users (id INT, name VARCHAR(50), age INT);
INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25);
SELECT name FROM users WHERE age > 27;
```

**Processing**:

1. **Tokenize CREATE TABLE**:
   ```
   [CREATE] [TABLE] [users] [(] [id] [INT] [,] [name] [VARCHAR] [(] [50] [)] ...
   ```

2. **Parse CREATE TABLE**:
   ```
   CreateTable {
     name: "users",
     columns: [
       ColumnDef("id", INT),
       ColumnDef("name", VARCHAR(50)),
       ColumnDef("age", INT)
     ]
   }
   ```

3. **Validate CREATE TABLE**:
   - Check: No duplicate columns ✅
   - Check: VARCHAR has length ✅
   - Check: INT has no length ✅

4. **Execute CREATE TABLE**:
   ```java
   catalog.createTable("users", schema);
   ```

5. **Parse INSERT**:
   ```
   Insert {
     table: "users",
     columns: null,  // All columns
     rows: [
       [Literal(1), Literal("Alice"), Literal(30)],
       [Literal(2), Literal("Bob"), Literal(25)]
     ]
   }
   ```

6. **Validate INSERT**:
   - Check: Table "users" exists ✅
   - Check: Arity matches (3 values, 3 columns) ✅
   - Check: Types compatible ✅

7. **Parse SELECT**:
   ```
   Select {
     items: [SelectItem(ColumnRef("name"))],
     tables: [TableRef("users")],
     where: ComparisonExpr(ColumnRef("age"), GT, Literal(27))
   }
   ```

8. **Validate SELECT**:
   - Check: Table "users" exists ✅
   - Check: Column "name" exists ✅
   - Check: Column "age" exists ✅

9. **Ready for planning** → Next phase!

---

## Key Takeaways

### Parsing Phases

1. **Tokenization**: Text → Tokens
2. **Parsing**: Tokens → AST
3. **Validation**: AST → Validated AST

### Parser Design

- **Recursive descent**: Simple, readable, extensible
- **Operator precedence**: Encoded in grammar hierarchy
- **Error recovery**: Position tracking for good errors

### AST Benefits

- **Structured representation**: Easy to analyze and transform
- **Type-safe**: Compiler checks node types
- **Visitor pattern**: Extensible operations on tree

### Validation

- **Catalog-aware**: Checks table/column existence
- **Type checking**: Literal type inference and compatibility
- **Early error detection**: Fail fast before execution

---

## Self-Test Questions

1. **What's the difference between tokenization and parsing?**
   - Tokenization: Text → Tokens (lexical analysis)
   - Parsing: Tokens → AST (syntax analysis)

2. **Why does expression grammar have multiple levels (orExpr, andExpr, etc.)?**
   - Encodes operator precedence (AND before OR, * before +)

3. **What does the Visitor pattern enable?**
   - Multiple operations on AST without modifying node classes

4. **Why validate before execution?**
   - Catch errors early, better error messages, avoid partial execution

5. **What's the difference between syntax and semantic errors?**
   - Syntax: Grammar violations (e.g., missing FROM)
   - Semantic: Meaningful violations (e.g., nonexistent table)

---

## Next Steps

You now understand how SQL becomes an AST. In **Phase 5**, we'll explore logical planning:
- What is a logical plan?
- Relational algebra operators
- Name resolution (Binder)
- Type inference (Analyzer)

**Continue to**: [`phase-5-logical-planning.md`](./phase-5-logical-planning.md)
