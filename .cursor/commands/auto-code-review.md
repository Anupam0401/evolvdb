---
description: review code like a senior developer
---

# /auto-code-review

Context-aware, production-focused code review for the current workspace. Reads full files for context, prioritizes production code quality, and verifies test coverage.

Steps:

1. Generate a comprehensive code review request with automatic base branch detection:

   - Run the automated review script:

     ```bash
     bash .cursor/commands/code_review/auto_review.sh
     ```

   - This script automatically:
     - Detects the correct base branch (origin/master for feature branches, upstream tracking for master/main)
     - Collects a safe, redacted diff of worktree changes vs the base
     - Includes untracked files
     - Skips binary files automatically
     - Redacts suspicious secrets (API keys, tokens, private keys)
     - Includes the enhanced review rubric with SOLID principles, design patterns, and test coverage requirements
     - Supports very large PRs up to 50MB (no truncation)
     - Writes to `/tmp/cascade_code_review_latest.txt` for complete diff capture
     - Shows file size and line count statistics

   - After running, the script outputs the temp file location. **Read it directly**:
     ```
     The complete diff is written to: /tmp/cascade_code_review_latest.txt
     Use read_file tool to read the complete diff without truncation
     ```

   - The output file includes:
     - Complete review criteria (Clean Architecture, SOLID, Design Patterns, Security, etc.)
     - Test coverage requirements (NEW files: 75% min, NO degradation for modified files)
     - Coverage exclusions (contracts/, dto/, domain/, config/, etc.)
     - The complete unified diff with all changes (no truncation)

   **Alternative**: If you need to specify a custom base branch:
   ```bash
   bash .cursor/commands/code_review/auto_review.sh --base origin/master
   ```

2. **CRITICAL: Gather Context Before Reviewing**

   Before analyzing the diff, YOU MUST:
   
   a. **Parse the diff** to identify all changed files
   
   b. **Read full production files** using the `read_file` tool:
      - For each `.java` file in the diff (except test files)
      - Read the COMPLETE file, not just changed lines
      - Read parent classes/interfaces if needed
      - Read related files for business logic understanding
   
   c. **Classify files by priority**:
      - **PRIORITY 1**: Production code (services, handlers, controllers, repositories)
      - **PRIORITY 2**: Test files (verify coverage only, not code quality)
      - **PRIORITY 3**: Config/DTOs (lower scrutiny)

3. Produce a detailed professional code review:

   **Act as a PRINCIPAL ENGINEER and SENIOR ARCHITECT** conducting a production-critical code review.

   - **Focus 80% effort on production code quality**, 20% on test coverage verification
   
   - Use the comprehensive rubric included in the payload with **MANDATORY sections**:
   
     **⛔ CRITICAL - MUST FIX BEFORE MERGE**
     - SOLID violations, security issues, data integrity risks
     - Include file:line, exact issue, concrete fix, architectural rationale
     
     **🔴 BLOCKER - FIX WITHIN 24 HOURS**
     - High-severity issues causing production incidents
     
     **🟡 SHOULD FIX SOON**
     - Technical debt, maintainability issues
     
     **🟢 NICE-TO-HAVE**
     - Minor improvements, style enhancements
     - **ALL test code quality issues go here** (naming, duplication, readability)
     
     **📝 LINE-LEVEL COMMENTS**
     - file:line format with quoted code and specific suggestions
     
     **⚠️ RISK ASSESSMENT**
     - Probability, Impact, Mitigation, Blast Radius
     
     **🧪 TEST COVERAGE ANALYSIS (MANDATORY)**
     - Focus: Do tests exist for production changes? NOT test code quality
     - For EACH production file changed/added:
       - File classification: Excluded package check
       - Test file existence: CRITICAL blocker if missing
       - Coverage completeness: ≥75% for new files, no degradation for modified
       - Missing test methods: List exact names needed
       - **DO NOT** report test naming/duplication/readability → Nice-to-have only
     
     **🏗️ ARCHITECTURAL RECOMMENDATIONS**
     - System-level improvements, design pattern opportunities

   - Verify all critical review criteria **FOR PRODUCTION CODE**:
     - **Clean Architecture & SOLID** (SRP, OCP, LSP, ISP, DIP)
     - **Design Patterns** (appropriate usage, anti-patterns)
     - **OOP Excellence** (encapsulation, cohesion, coupling)
     - **Correctness & Robustness** (edge cases, null handling, idempotency)
     - **Null Safety** (comprehensive null checks, NPE prevention)
     - **API Design** (backward compatibility, contracts)
     - **Concurrency & Thread Safety** (race conditions, deadlocks)
     - **Performance** (algorithmic complexity, N+1 queries)
     - **Security** (AuthN/AuthZ, injection, PII/secrets in logs) - ZERO TOLERANCE
     - **Resilience** (timeouts, retries, circuit breakers)
     - **Observability** (logging, metrics, tracing)
     - **Error Handling** (fail-fast, graceful degradation)
     - **Test Coverage** (Do tests exist? Are changes covered? Quality is secondary)

4. If the diff is empty:

   - Respond: "No changes detected for review." and stop.

## Review Focus Guidelines

### Production Code (80% of review effort):
- **Architecture & Design**: SOLID principles, design patterns, clean architecture
- **Business Logic**: Correctness, edge cases, validation
- **Null Safety**: Comprehensive null checks, NPE prevention
- **Error Handling**: Proper exception handling, fail-fast principles
- **Security**: AuthN/AuthZ, injection prevention, secrets management
- **Performance**: Algorithmic complexity, database queries, caching
- **Resilience**: Timeouts, retries, circuit breakers
- **Code Quality**: Naming, function size, duplication

### Test Files (20% of review effort):
- **Coverage Verification**: Do tests exist? Are changes covered?
- **Critical Test Errors**: Wrong assertions, tests that always pass
- **Test Quality Issues**: Naming, duplication → 🟢 Nice-to-have ONLY

## Important Notes

### Workflow Execution:
- Commands run in workspace root. Approve terminal prompts as needed.
- The script automatically detects origin/master for PR reviews.
- Manual override: `bash .cursor/commands/code_review/auto_review.sh --base origin/master`
- **No Truncation**: Complete diff written to `/tmp/cascade_code_review_latest.txt` (supports up to 50MB)
- **Direct File Access**: Use `read_file` tool to read complete diff without terminal truncation
- **Manual Cleanup**: Delete temp file with `rm /tmp/cascade_code_review_latest.txt` when done

### Context-Aware Review:
- **ALWAYS** read full production files using `read_file` tool before reviewing
- Don't rely on diff alone - diffs hide critical context
- Understanding complete methods/classes is essential for proper review

### Test Coverage:
- Requirements align with `.github/workflows/code-coverage.yml`
- DTOs/contracts/domain classes in excluded packages don't need tests
- Test code quality issues go to "Nice-to-have" section only
- Focus is: Do tests exist? Not: Are tests perfectly written?

### Priority Focus:
- 80% effort on production code quality (architecture, logic, security)
- 20% effort on test coverage verification (existence, completeness)
- Test code quality is secondary concern