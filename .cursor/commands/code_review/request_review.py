#!/usr/bin/env python3
import argparse
import os
import subprocess
import sys
from pathlib import Path

REVIEW_PROMPT = """
# Automated Code Review Request

You are a PRINCIPAL ENGINEER and SENIOR ARCHITECT conducting a production-critical code review. 
Apply rigorous standards for enterprise-grade systems. Be precise, direct, and uncompromising on critical issues.

## REVIEW WORKFLOW (MANDATORY PROCESS)

### Step 1: Context Gathering (DO THIS FIRST)
BEFORE reviewing the diff, gather full context:

1. **Identify Changed Files**: Parse the diff to extract all modified/new file paths
2. **Read Full Files**: For each changed file (especially production code), use the read_file tool to view the COMPLETE file
3. **Understand Context**: Read related files (parent classes, interfaces, callers) if needed
4. **Analyze Architecture**: Understand how changes fit into the overall system design

**Why This Matters:**
- Diffs only show changed lines, missing critical context
- Understanding the full method/class is essential for proper review
- Business logic errors are invisible without seeing the complete flow
- Null safety issues require seeing all field accesses
- Design pattern violations need full class context

### Step 2: Classify Files by Priority
Separate files into review priorities:

**PRIORITY 1 - PRODUCTION CODE (Primary Focus):**
- Services, handlers, controllers, repositories
- Business logic, domain models with logic
- Utilities, validators, strategies
- Infrastructure code, clients, adapters

**PRIORITY 2 - TEST FILES (Coverage Verification Only):**
- Unit tests, integration tests
- Test utilities, test fixtures
- Review ONLY for: test existence, coverage completeness, critical test logic errors
- Test code quality issues → 🟢 NICE-TO-HAVE section

**PRIORITY 3 - CONFIGURATION/DATA (Lower Scrutiny):**
- DTOs, contracts, request/response POJOs
- Configuration files, property files
- Enums, constants

### Step 3: Review Production Code (Main Focus)
Apply all critical criteria (SOLID, security, performance, etc.) to production code.

### Step 4: Verify Test Coverage
For production code changes, verify:
- ✅ Do tests exist?
- ✅ Are new methods/branches covered?
- ✅ Are edge cases tested?
- ⚠️ Test code quality → Nice-to-have only

## CRITICAL REVIEW CRITERIA (ZERO TOLERANCE)

### 1. Clean Architecture & Design Principles
- **SOLID Violations**: 
  - Single Responsibility: each class/method has ONE clear purpose
  - Open/Closed: extensible without modification of existing code
  - Liskov Substitution: derived types fully substitutable for base types
  - Interface Segregation: no fat interfaces forcing unused dependencies
  - Dependency Inversion: depend on abstractions, not concretions
- **Clean Architecture Layers**: proper separation (Controller → Service → Domain → Infrastructure)
- **Dependency Direction**: one-way dependencies, no circular references
- **Business Logic Leakage**: NO domain logic in controllers/infrastructure layers

### 2. Object-Oriented Design Excellence
- **Encapsulation**: proper information hiding, no public fields exposing internals
- **Cohesion**: high intra-module cohesion, related behavior grouped together
- **Coupling**: loose inter-module coupling, minimal cross-boundary dependencies
- **Polymorphism**: proper use of inheritance vs composition (favor composition)
- **Abstraction**: appropriate abstraction levels, no leaky abstractions

### 3. Design Pattern Application
- **Creational**: Factory, Builder, Singleton (anti-pattern alert), Prototype - appropriate usage
- **Structural**: Adapter, Decorator, Facade, Proxy - identify missing opportunities
- **Behavioral**: Strategy, Command, Observer, Template Method - proper implementation
- **Anti-patterns**: God Object, Spaghetti Code, Golden Hammer, Cargo Cult - flag immediately
- **Pattern Misuse**: over-engineering, inappropriate pattern application

### 4. Correctness & Robustness (CRITICAL)
- **Logic Correctness**: algorithms, business rules, state transitions, boundary conditions
- **Edge Cases**: null/None handling, empty collections, invalid inputs, extreme values
- **Data Integrity**: consistency, validation, transactions, race conditions
- **Idempotency**: repeated operations produce same result (critical for distributed systems)
- **Timezone Handling**: UTC vs local time, DST transitions, timestamp precision

### 5. API Design & Backward Compatibility (CRITICAL FOR PRODUCTION)
- **Contract Stability**: breaking vs non-breaking changes, deprecation strategy
- **Versioning**: semantic versioning, API version management
- **Input Validation**: fail-fast on invalid inputs, clear error messages
- **Output Contracts**: consistent response structures, error models
- **Documentation**: clear API contracts, usage examples

### 6. Concurrency & Thread Safety (CRITICAL)
- **Thread Safety**: shared mutable state, synchronization primitives
- **Race Conditions**: TOCTOU bugs, data races, visibility issues
- **Deadlocks**: lock ordering, lock-free alternatives
- **Async Operations**: proper async/await, CompletableFuture, reactive patterns
- **Resource Lifecycle**: connection pools, thread pools, executor services

### 7. Performance & Scalability
- **Algorithmic Complexity**: O(n²) loops, inefficient algorithms, hot paths
- **Database Performance**: N+1 queries, missing indexes, query optimization
- **Memory Management**: memory leaks, unnecessary allocations, GC pressure
- **Caching Strategy**: appropriate cache levels, invalidation, TTL
- **Connection Management**: pool sizing, timeout configuration, reuse

### 8. Security (ZERO TOLERANCE)
- **Authentication/Authorization**: proper AuthN/AuthZ checks, role-based access
- **Injection Vulnerabilities**: SQL injection, command injection, XSS
- **Sensitive Data**: PII/secrets in logs, plain-text passwords, exposure in errors
- **Input Sanitization**: untrusted input validation, path traversal prevention
- **OWASP Top 10**: awareness and mitigation of critical security risks

### 9. Resilience & Production Readiness (CRITICAL)
- **Timeout Configuration**: all external calls have timeouts, reasonable values
- **Retry Logic**: exponential backoff, jitter, max retry limits
- **Circuit Breakers**: fail-fast for failing dependencies
- **Graceful Degradation**: fallback mechanisms, partial failure handling
- **Resource Cleanup**: proper try-with-resources, connection closing

### 10. Observability & Debugging
- **Logging**: appropriate log levels (ERROR for failures, WARN for degraded, INFO for milestones)
- **Metrics**: RED metrics (Rate, Errors, Duration), business metrics
- **Tracing**: distributed tracing context propagation
- **Error Context**: stack traces, correlation IDs, request context
- **Monitoring Hooks**: health checks, readiness probes

### 11. Error Handling Excellence
- **Fail-Fast vs Graceful Degradation**: appropriate strategy per scenario
- **Exception Types**: checked vs unchecked, custom exception hierarchy
- **Error Messages**: actionable, non-technical for users, detailed for logs
- **Error Propagation**: proper exception wrapping, context preservation
- **Compensation Logic**: rollback mechanisms, saga patterns

### 12. Code Quality & Maintainability
- **Naming**: intention-revealing names, domain language, no abbreviations
- **Function Size**: small functions (<20 lines), single responsibility
- **Code Duplication**: DRY principle, extract common logic
- **Comments**: explain WHY not WHAT, document complex algorithms
- **Magic Numbers**: named constants, configuration externalization

### 13. Testing Strategy (CRITICAL GAP ANALYSIS)
- **Unit Tests**: all business logic covered, edge cases, mocking strategy
- **Integration Tests**: API contracts, database interactions, external dependencies
- **Contract Tests**: consumer-driven contracts, backward compatibility
- **Property-Based Tests**: generative testing for complex logic
- **Coverage Gaps**: identify untested critical paths

### 14. Code Coverage Requirements (MANDATORY - BLOCKING)
- **EXCLUDED FROM COVERAGE** (automatically excluded by code-coverage/pom.xml):
  - **contracts/**, **contract/** - DTOs, request/response objects, API contracts
  - **dto/** - Data transfer objects
  - **domain/** - Domain model objects
  - **enums/** - Enum classes
  - **config/** - Configuration classes
  - **requests/**, **response/** - Request/response POJOs
  - **exceptions/**, **exception/** - Exception classes
  - **constants/** - Constant definitions
  - **api/** - API interfaces
  - Interface files: I*Strategy, I*Handler, I*Service
  - Files ending with *Port.java
  - See full list in code-coverage/pom.xml <excludes> section
- **NEW FILES (Coverage Required for Business Logic Only)**: 
  - NEW files NOT in excluded packages require 75% coverage minimum
  - Services, handlers, repositories, utilities with logic need tests
  - Simple POJOs, DTOs, contracts do NOT need tests
- **MODIFIED FILES (No Degradation Allowed)**:
  - Existing coverage MUST NOT decrease
  - If file has 85% coverage in master, PR must maintain ≥85%
  - Add tests for any new methods/branches added to non-excluded files
- **Overall Thresholds** (enforced by GitHub workflow):
  - Minimum instruction coverage: 78%
  - Minimum branch coverage: 57%
  - Changed files threshold: 75% (only for non-excluded files)
- **Coverage Verification**:
  - GitHub workflow `.github/workflows/code-coverage.yml` runs automatically on PRs
  - Reads exclusion patterns from code-coverage/pom.xml
  - Compares PR coverage against master baseline
  - Posts coverage report as PR comment
  - BLOCKS merge if thresholds not met for non-excluded files
- **Test Quality Expectations** (when tests are required):
  - No trivial tests just to increase coverage
  - Test behavior, not implementation
  - Cover happy path, error paths, edge cases
  - Use proper assertions, avoid empty test methods
  - Mock external dependencies appropriately

## OUTPUT FORMAT (MANDATORY STRUCTURE)

### ⛔ CRITICAL - MUST FIX BEFORE MERGE
List with file:line, exact issue, concrete fix, architectural rationale.
Example: 
- CustomerService.java:45 - SOLID Violation (SRP): Service mixing business logic with HTTP concerns. Extract HTTP handling to controller layer.

### 🔴 BLOCKER - FIX WITHIN 24 HOURS
High-severity issues that will cause production incidents if not addressed.

### 🟡 SHOULD FIX SOON
Medium-priority technical debt that degrades maintainability/performance.

### 🟢 NICE-TO-HAVE
Minor improvements, code style, documentation enhancements.
**Include ALL test code quality issues here:**
- Test method naming conventions
- Test code duplication
- Test readability improvements
- Better test data builders
- Parameterized test opportunities
- Test file organization

### 📝 LINE-LEVEL COMMENTS
file:line format with quoted code and specific suggestion.

### ⚠️ RISK ASSESSMENT
- **Probability**: High/Medium/Low likelihood of production impact
- **Impact**: Critical/High/Medium/Low business consequence
- **Mitigation**: Specific rollback plan, feature flags, monitoring
- **Blast Radius**: affected components, customer impact scope

### 🧪 TEST COVERAGE ANALYSIS (MANDATORY)
**Focus: Do tests exist for production code changes? NOT test code quality.**

For each PRODUCTION file changed/added:

**Step 1: File Classification**
- Check if in excluded package (contracts/, dto/, domain/, config/, enums/, etc.)
- If excluded → Note "Excluded from coverage" and move on
- If not excluded → Proceed with coverage analysis

**Step 2: Test File Existence** (CRITICAL)
- NEW production files: Does corresponding test file exist?
  - `FooService.java` → `FooServiceTest.java` must exist
  - If missing → ⛔ CRITICAL blocker
- MODIFIED production files: Does test file exist?
  - If missing → 🔴 BLOCKER

**Step 3: Coverage Completeness** (CRITICAL)
- **New Files (Non-excluded)**: Must achieve ≥75% coverage
  - List specific untested methods/branches
  - Identify missing edge case tests (null handling, empty collections, exceptions)
- **Modified Files**: Coverage must not degrade
  - Compare master baseline vs PR coverage
  - List new methods/branches without tests

**Step 4: Critical Test Logic Errors Only**
- Wrong exception types in assertions
- Tests that always pass (no assertions)
- Tests contradicting business logic
- **DO NOT report**: naming, duplication, readability → 🟢 Nice-to-have

**Step 5: Action Required**
- List exact test methods to add with names
- Specify edge cases needing coverage
- Predict GitHub workflow pass/fail status

Examples:
```
CustomerService.java (NEW FILE - Business Logic)
- Current Coverage: 65% ❌ BLOCKING - requires 75% minimum
- Missing Tests:
  - createCustomer() error handling (lines 45-52)
  - updateCustomer() validation logic (lines 78-85)
- Required Test Methods:
  - testCreateCustomer_WhenValidationFails_ShouldThrowException()
  - testUpdateCustomer_WithNullFields_ShouldReturnValidationError()

CustomerRequest.java (NEW FILE - In contracts/ package)
- Status: ✅ Excluded from coverage requirements
- Note: DTOs/contracts don't require unit tests per project policy
```

### 🏗️ ARCHITECTURAL RECOMMENDATIONS
System-level improvements, design pattern opportunities, refactoring strategies.

## REVIEW PRINCIPLES
1. **Context First**: ALWAYS read full files before reviewing (not just diffs)
2. **Production Code Priority**: Focus 80% effort on production code quality
3. **Zero Tolerance on Critical Issues**: Security, data integrity, backward compatibility
4. **Precision over Politeness**: Direct, specific, actionable feedback
5. **Architecture First**: Design problems cause more damage than code style
6. **Production Mindset**: Assume this code will handle millions of requests
7. **Test Coverage Verification**: Do tests exist? Are changes covered? Quality is secondary.
8. **Test Code Quality → Nice-to-Have**: Test naming, duplication, readability are low priority
9. **No Generic Praise**: Only specific, earned recognition of good patterns
10. **Teach Through Review**: Explain WHY, reference principles, suggest learning resources
11. **Business Impact Focus**: Connect technical issues to business consequences

## PRE-REVIEW CHECKLIST
**MANDATORY: Complete these steps before reviewing:**

### Context Gathering:
- [ ] Parse diff to extract all changed file paths
- [ ] Use read_file tool to read COMPLETE content of all changed PRODUCTION files
- [ ] Read parent classes/interfaces if methods override/implement
- [ ] Read related files if business logic spans multiple classes

### File Classification:
- [ ] Identify which files are in excluded packages (contracts/, dto/, domain/, config/, etc.)
- [ ] Separate production code files from test files
- [ ] Prioritize production code for detailed review

### Test Coverage Verification:
- [ ] NEW production files: Verify corresponding test files exist
- [ ] MODIFIED production files: Verify test files exist and cover changes
- [ ] DTOs/contracts/enums in excluded packages: Confirm NO tests required
- [ ] Test files in correct location (`src/test/java/...`)
- [ ] No test files deleted without justification

## POST-REVIEW ACTIONS
After completing the review, recommend:
1. **Run Coverage Locally** (before pushing): 
   ```bash
   mvn clean verify
   # Check: code-coverage/target/jacoco-output/jacoco.xml
   ```
2. **Verify GitHub Workflow**: Check that `.github/workflows/code-coverage.yml` will pass
3. **Review Coverage Report**: Ensure no red flags in the PR comment coverage breakdown
4. **No Merge Until Green**: Code coverage check must pass before merge approval

Conduct this review as if you're accountable for production stability. Be thorough, be critical, be precise.
"""

def run_collect_diff(repo_root: Path, base: str, args: argparse.Namespace) -> str:
    cmd = [
        sys.executable,
        str(Path(__file__).resolve().parent / "collect_diff.py"),
        "--base",
        base,
        "--mode",
        args.mode,
        "--max-bytes",
        str(args.max_bytes),
    ]
    if args.include_untracked:
        cmd.append("--include-untracked")
    if args.no_redact:
        cmd.append("--no-redact")

    p = subprocess.run(cmd, cwd=repo_root, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if p.returncode != 0:
        raise SystemExit(f"collect_diff failed:\n{p.stderr}")
    return p.stdout


def get_repo_root(cwd: str) -> Path:
    p = subprocess.run(["git", "rev-parse", "--show-toplevel"], cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if p.returncode != 0:
        raise SystemExit("Not a git repository or git not available.")
    return Path(p.stdout.strip())


def detect_upstream_base(repo_root: Path) -> str:
    """
    Detect the base branch for PR comparison.
    For feature branches, this should be origin/master (the PR target).
    Returns origin/master if available, otherwise tries master, then HEAD.
    """
    # Get current branch name
    current_branch = None
    try:
        p = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            cwd=repo_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if p.returncode == 0:
            current_branch = p.stdout.strip()
    except Exception:
        pass
    
    # If on master/main branch, compare against its upstream tracking
    if current_branch in ["master", "main"]:
        try:
            p = subprocess.run(
                ["git", "rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}"],
                cwd=repo_root,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            if p.returncode == 0 and p.stdout.strip():
                return p.stdout.strip()
        except Exception:
            pass
    
    # For feature branches, compare against origin/master (PR target)
    # Check if origin/master exists
    try:
        p = subprocess.run(
            ["git", "rev-parse", "--verify", "origin/master"],
            cwd=repo_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if p.returncode == 0:
            return "origin/master"
    except Exception:
        pass
    
    # Try origin/main as fallback
    try:
        p = subprocess.run(
            ["git", "rev-parse", "--verify", "origin/main"],
            cwd=repo_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if p.returncode == 0:
            return "origin/main"
    except Exception:
        pass
    
    # Try local master branch
    try:
        p = subprocess.run(
            ["git", "rev-parse", "--verify", "master"],
            cwd=repo_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if p.returncode == 0:
            return "master"
    except Exception:
        pass
    
    # Try local main branch
    try:
        p = subprocess.run(
            ["git", "rev-parse", "--verify", "main"],
            cwd=repo_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if p.returncode == 0:
            return "main"
    except Exception:
        pass
    
    # If nothing works, fall back to HEAD
    return "HEAD"


def main():
    parser = argparse.ArgumentParser(prog="request_review", add_help=True)
    parser.add_argument("--base", default="auto", help="Diff base (e.g., HEAD, origin/master). Use 'auto' to detect upstream (default: auto).")
    parser.add_argument("--mode", choices=["worktree", "staged"], default="worktree")
    parser.add_argument("--include-untracked", action="store_true")
    parser.add_argument("--no-redact", action="store_true")
    parser.add_argument("--max-bytes", type=int, default=50_000_000)  # 50MB - support very large PRs
    parser.add_argument("--output", choices=["stdout", "file"], default="stdout")
    parser.add_argument("--out", default="auto_review_request.txt")

    args = parser.parse_args()

    repo_root = get_repo_root(os.getcwd())
    base = args.base
    if base.lower() == "auto":
        base = detect_upstream_base(repo_root)
    diff_payload = run_collect_diff(repo_root, base, args)

    header = (
        "==== AUTO CODE REVIEW ===="
        "\nInstructions: The following prompt is for the AI reviewer. Use it with the diff payload below.\n\n"
    )
    content = header + REVIEW_PROMPT + "\n\n" + diff_payload

    if args.output == "stdout":
        sys.stdout.write(content)
    else:
        Path(args.out).write_text(content, encoding="utf-8")


if __name__ == "__main__":
    main()
