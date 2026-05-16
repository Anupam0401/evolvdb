# CI/CD Documentation

This document describes the Continuous Integration and Continuous Deployment setup for EvolvDB.

## GitHub Actions Workflows

### 1. CI Build and Test (`.github/workflows/ci.yml`)

**Triggers:**
- Push to `main` or `develop` branches
- Pull requests to `main` or `develop`

**Jobs:**

**a. Build Matrix (Java 17 & 21)**
- Checks out code
- Sets up JDK (both 17 and 21)
- Runs clean build
- Executes all tests
- Generates test reports
- Uploads artifacts (test results, build JARs)

**b. Code Quality Checks**
- Runs checkstyle (if configured)
- Runs spotbugs (if configured)
- Checks for dependency updates

**c. Build Status Check**
- Final status aggregation
- Fails if any required job fails

**Artifacts Generated:**
- `test-results-java-17` and `test-results-java-21`
- `build-artifacts-java-17` and `build-artifacts-java-21`

---

### 2. PR Checks (`.github/workflows/pr-checks.yml`)

**Triggers:**
- Pull request opened, synchronized, or reopened to `main`

**Jobs:**

**a. PR Validation**
- Validates PR title format (conventional commits)
- Checks for merge conflicts
- Warns if too many files changed (>100)

**b. Build Verification**
- Clean build from scratch
- Verifies all modules compile
- Checks for compilation warnings

**c. Test Coverage**
- Runs tests with JaCoCo coverage
- Uploads coverage reports

**d. Documentation Check**
- Warns if large code changes lack doc updates
- Runs markdown linting

**e. Dependency Security Check**
- Scans for vulnerable dependencies (OWASP)

**f. PR Status**
- Aggregates all check results
- Fails PR if critical checks fail

---

### 3. CodeQL Security Analysis (`.github/workflows/codeql.yml`)

**Triggers:**
- Push to `main` or `develop`
- Pull requests to `main`
- Weekly schedule (Sundays at midnight)

**Analysis:**
- Static security analysis
- Detects common vulnerabilities
- Checks for security anti-patterns
- Generates security reports

---

### 4. Release Workflow (`.github/workflows/release.yml`)

**Triggers:**
- Git tags matching `v*.*.*` (e.g., `v1.0.0`)
- Manual workflow dispatch

**Steps:**
1. Builds release artifacts
2. Runs full test suite
3. Creates distribution packages
4. Generates changelog from git log
5. Creates GitHub release with artifacts

**Release Artifacts:**
- Distribution ZIPs and TARs
- JAR files for all modules

---

## Workflow Configuration

### Environment Variables
```yaml
GRADLE_OPTS: -Dorg.gradle.daemon=false
JAVA_VERSION: 17
GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
```

### Caching
- Gradle dependencies cached via `actions/setup-java@v4`
- Speeds up builds by ~50%

### Parallel Execution
- Java 17 and 21 builds run in parallel
- Multiple check jobs run concurrently
- Reduces total CI time

---

## Local Simulation

### Simulate CI Build Locally
```bash
# Clean build (like CI)
./gradlew clean build --no-daemon --stacktrace

# Run tests
./gradlew test --no-daemon

# Generate test report
./gradlew testReport --no-daemon

# Check code style
./gradlew checkstyleMain checkstyleTest

# Check dependencies
./gradlew dependencyUpdates
```

### Test with Multiple Java Versions
```bash
# Using SDKMAN
sdk use java 17.0.9-tem
./gradlew clean build

sdk use java 21.0.1-tem
./gradlew clean build
```

---

## Branch Protection Setup

### Required Status Checks for `main` Branch

**Via GitHub UI:**
1. Repository Settings → Branches
2. Add rule for `main`
3. Enable:
   - ✅ Require pull request before merging
   - ✅ Require approvals: 1
   - ✅ Require status checks to pass:
     - `build (17)`
     - `build (21)`
     - `pr-status`
   - ✅ Require branches to be up to date
   - ✅ Require conversation resolution

**Via GitHub CLI:**
```bash
# Set branch protection
gh api repos/:owner/:repo/branches/main/protection \
  --method PUT \
  -f required_status_checks='{"strict":true,"contexts":["build (17)","build (21)","pr-status"]}' \
  -F enforce_admins=true \
  -f required_pull_request_reviews='{"required_approving_review_count":1}' \
  -f restrictions=null

# Verify protection
gh api repos/:owner/:repo/branches/main/protection
```

---

## CI Failure Troubleshooting

### Build Failures

**Compilation Errors:**
```bash
# View detailed error
./gradlew clean build --stacktrace --info

# Check for missing dependencies
./gradlew dependencies
```

**Test Failures:**
```bash
# Run specific test
./gradlew :module:test --tests ClassName

# Run with debug output
./gradlew test --debug

# View test report
open build/reports/tests/test/index.html
```

### Common Issues

**1. Gradle Daemon Issues**
```bash
# Kill daemon
./gradlew --stop

# Rebuild
./gradlew clean build --no-daemon
```

**2. Cache Issues**
```bash
# Clear Gradle cache
rm -rf ~/.gradle/caches

# Clear build directories
./gradlew clean
```

**3. Dependency Resolution**
```bash
# Refresh dependencies
./gradlew build --refresh-dependencies
```

---

## Performance Optimization

### Current CI Times
- **Build Matrix (parallel):** ~3-5 minutes
- **Code Quality:** ~2-3 minutes
- **PR Checks:** ~4-6 minutes
- **Total PR Time:** ~6-8 minutes

### Optimization Strategies
1. **Gradle Build Cache:** Enabled via `cache: gradle`
2. **Parallel Builds:** Matrix strategy for Java versions
3. **No Daemon Mode:** Faster for CI environments
4. **Artifact Caching:** Reuses build outputs

---

## Monitoring & Notifications

### GitHub Actions Dashboard
- View all workflow runs: Actions tab
- Filter by workflow, status, branch
- Download artifacts from successful runs

### Notifications
- GitHub sends notifications on:
  - Build failures on your branches
  - PR check failures
  - Required reviews
  - Merge conflicts

### Status Badges
Add to README:
```markdown
![CI](https://github.com/OWNER/REPO/workflows/CI%20Build%20and%20Test/badge.svg)
![CodeQL](https://github.com/OWNER/REPO/workflows/CodeQL%20Security%20Analysis/badge.svg)
```

---

## Future Enhancements

### Planned Improvements
1. **Performance Benchmarking:** Add workflow to track performance metrics
2. **Docker Integration:** Build and publish Docker images
3. **Deployment:** Auto-deploy to staging environment
4. **Mutation Testing:** Add PIT mutation testing
5. **Integration Tests:** Separate workflow for E2E tests
6. **Nightly Builds:** Comprehensive nightly test runs

### Additional Tools to Consider
- SonarQube for code quality
- Dependabot for dependency updates
- Codecov for coverage reporting
- Renovate for automated dependency PRs

---

## References

- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [Gradle CI Best Practices](https://docs.gradle.org/current/userguide/ci_best_practices.html)
- [Branch Protection Rules](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/defining-the-mergeability-of-pull-requests/about-protected-branches)
