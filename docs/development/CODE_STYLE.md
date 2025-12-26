# Code Style Guide

EvolvDB follows the Google Java Style Guide with automated formatting via Spotless.

## Automated Formatting

We use **Spotless** with **Google Java Format** to ensure consistent code style across the project.

### Quick Commands

```bash
# Check if code is formatted correctly
./gradlew spotlessCheck

# Auto-format all code
./gradlew spotlessApply

# Format specific module
./gradlew :evolvdb-sql:spotlessApply

# Format before committing
./gradlew spotlessApply && git add -u
```

### Pre-Commit Hook (Optional)

Add this to `.git/hooks/pre-commit`:

```bash
#!/bin/bash
./gradlew spotlessCheck
if [ $? -ne 0 ]; then
  echo "Code formatting check failed. Run './gradlew spotlessApply' to fix."
  exit 1
fi
```

Then make it executable:
```bash
chmod +x .git/hooks/pre-commit
```

## Formatting Rules

### 1. Google Java Format (AOSP variant)
- **Line length:** 100 characters
- **Indentation:** 4 spaces (no tabs)
- **Continuation indent:** 8 spaces
- **Reflow long strings:** Enabled

### 2. Import Organization
```java
// 1. java/javax imports
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

// 2. Blank line

// 3. All other imports (alphabetically)
import io.github.anupam.evolvdb.catalog.CatalogManager;
import io.github.anupam.evolvdb.sql.ast.Expr;

// 4. Blank line

// 5. Static imports
import static org.junit.jupiter.api.Assertions.*;
```

### 3. Whitespace Rules
- ✅ Trim trailing whitespace
- ✅ End files with newline
- ✅ No multiple consecutive blank lines

### 4. Annotations
- Format annotations consistently
- One annotation per line for methods/classes
- Inline annotations OK for parameters

## Code Style Guidelines

### Naming Conventions

**Classes:**
```java
public class TableMeta { }           // PascalCase
public interface PhysicalOperator { } // PascalCase
```

**Methods:**
```java
public void executeQuery() { }       // camelCase
private int calculateCost() { }      // camelCase
```

**Variables:**
```java
int rowCount;                        // camelCase
String tableName;                    // camelCase
```

**Constants:**
```java
public static final int MAX_SIZE = 100;        // UPPER_SNAKE_CASE
private static final String DEFAULT_NAME = ""; // UPPER_SNAKE_CASE
```

**Packages:**
```java
package io.github.anupam.evolvdb.sql.parser;  // lowercase
```

### Class Structure Order

```java
public class Example {
    // 1. Static constants
    private static final int CONSTANT = 1;
    
    // 2. Static variables
    private static String staticVar;
    
    // 3. Instance variables
    private final String name;
    private int count;
    
    // 4. Constructors
    public Example(String name) {
        this.name = name;
    }
    
    // 5. Static factory methods
    public static Example create() {
        return new Example("default");
    }
    
    // 6. Public methods
    public void publicMethod() { }
    
    // 7. Package-private methods
    void packageMethod() { }
    
    // 8. Protected methods
    protected void protectedMethod() { }
    
    // 9. Private methods
    private void privateMethod() { }
    
    // 10. Static nested classes
    public static class NestedClass { }
    
    // 11. Inner classes
    private class InnerClass { }
}
```

### Javadoc Requirements

**All public classes and methods must have Javadoc:**

```java
/**
 * Executes a SQL UPDATE statement.
 * 
 * <p>This operator scans the table, applies WHERE filtering via child operators,
 * and updates matching rows with new values from assignment expressions.
 *
 * @see DeleteExec
 * @see LogicalUpdate
 */
public final class UpdateExec implements PhysicalOperator {
    
    /**
     * Creates a new UPDATE executor.
     *
     * @param child the child operator (typically FilterExec)
     * @param catalog the catalog manager for table access
     * @param update the logical update plan
     */
    public UpdateExec(PhysicalOperator child, CatalogManager catalog, LogicalUpdate update) {
        // ...
    }
    
    /**
     * Opens the operator and prepares for execution.
     *
     * @throws Exception if table cannot be opened
     */
    @Override
    public void open() throws Exception {
        // ...
    }
}
```

### Comments

**Good comments explain WHY, not WHAT:**

```java
// ❌ BAD: Explains what the code does (obvious)
// Increment counter
counter++;

// ✅ GOOD: Explains why we're doing it
// Skip first row as it contains column headers
counter++;

// ❌ BAD: Redundant comment
// Get the table name
String tableName = getTableName();

// ✅ GOOD: Explains non-obvious behavior
// Table names are case-insensitive in catalog but stored lowercase
String tableName = getTableName().toLowerCase();
```

### Method Length

- **Preferred:** 10-20 lines
- **Maximum:** 50 lines
- **If longer:** Extract helper methods

```java
// ❌ BAD: Long method doing too much
public void processQuery() {
    // 100 lines of code...
}

// ✅ GOOD: Broken into focused methods
public void processQuery() {
    validateQuery();
    parseQuery();
    optimizeQuery();
    executeQuery();
}
```

### Line Length

- **Maximum:** 100 characters
- **Long strings:** Use multi-line strings or concatenation
- **Long method chains:** Break after dots

```java
// ✅ GOOD: Broken at 100 characters
String message = String.format(
        "Failed to execute UPDATE on table %s: column %s not found",
        tableName,
        columnName);

// ✅ GOOD: Chained method calls
List<String> result = catalog.getTables()
        .stream()
        .filter(t -> t.name().startsWith("test_"))
        .map(TableMeta::name)
        .collect(Collectors.toList());
```

### Braces

Always use braces, even for single-line blocks:

```java
// ❌ BAD
if (condition)
    doSomething();

// ✅ GOOD
if (condition) {
    doSomething();
}
```

### Exception Handling

```java
// ✅ GOOD: Specific exceptions
try {
    table.insert(tuple);
} catch (IOException e) {
    throw new ExecutionException("Failed to insert tuple", e);
}

// ❌ BAD: Swallowing exceptions
try {
    table.insert(tuple);
} catch (Exception e) {
    // ignore
}

// ❌ BAD: Catching generic Exception when specific exists
try {
    table.insert(tuple);
} catch (Exception e) {
    // ...
}
```

### Immutability

Prefer immutable objects:

```java
// ✅ GOOD: Immutable
public final class Schema {
    private final List<ColumnMeta> columns;
    
    public Schema(List<ColumnMeta> columns) {
        this.columns = List.copyOf(columns); // Defensive copy
    }
    
    public List<ColumnMeta> columns() {
        return Collections.unmodifiableList(columns);
    }
}

// ❌ BAD: Mutable
public class Schema {
    private List<ColumnMeta> columns;
    
    public void setColumns(List<ColumnMeta> columns) {
        this.columns = columns;
    }
}
```

## IDE Setup

### IntelliJ IDEA

1. Install Google Java Format plugin:
   - Settings → Plugins → Search "google-java-format"
   - Install and restart

2. Enable the plugin:
   - Settings → google-java-format Settings
   - Check "Enable google-java-format"
   - Select "AOSP style (4 space indent)"

3. Import order:
   - Settings → Editor → Code Style → Java → Imports
   - Set order: `java`, `javax`, blank, `*`, blank, `static *`

4. On save actions:
   - Settings → Tools → Actions on Save
   - Enable "Reformat code"
   - Enable "Optimize imports"

### VS Code

1. Install extensions:
   ```
   Extension Pack for Java
   Language Support for Java by Red Hat
   ```

2. Add to `settings.json`:
   ```json
   {
     "java.format.settings.url": "https://raw.githubusercontent.com/google/styleguide/gh-pages/eclipse-java-google-style.xml",
     "java.format.settings.profile": "GoogleStyle",
     "editor.formatOnSave": true,
     "editor.codeActionsOnSave": {
       "source.organizeImports": true
     }
   }
   ```

### Eclipse

1. Download Google Java Style:
   https://github.com/google/styleguide/blob/gh-pages/eclipse-java-google-style.xml

2. Import:
   - Window → Preferences → Java → Code Style → Formatter
   - Import → Select downloaded XML
   - Apply

## CI/CD Integration

### GitHub Actions

Spotless checks run automatically on:
- ✅ Every push to `main` or `develop`
- ✅ Every pull request
- ❌ Blocks merge if formatting issues exist

### Fix Formatting Issues

If CI fails with formatting errors:

```bash
# Auto-fix all issues
./gradlew spotlessApply

# Commit the fixes
git add -u
git commit -m "style: Apply Spotless formatting"
git push
```

## Exceptions

Some files may be excluded from formatting:

```kotlin
// In build.gradle.kts
spotless {
    java {
        targetExclude(
            "src/test/resources/**",
            "build/generated/**"
        )
    }
}
```

## Questions?

- Spotless docs: https://github.com/diffplug/spotless
- Google Style Guide: https://google.github.io/styleguide/javaguide.html
- Ask in pull request reviews

---

**Remember:** Consistent code style makes collaboration easier! 🎨
