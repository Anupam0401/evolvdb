# Contributing to EvolvDB

Thank you for your interest in contributing to EvolvDB! This document provides guidelines and instructions for contributing.

## Table of Contents
- [Getting Started](#getting-started)
- [Development Workflow](#development-workflow)
- [Code Guidelines](#code-guidelines)
- [Testing Requirements](#testing-requirements)
- [Pull Request Process](#pull-request-process)
- [Branch Protection Rules](#branch-protection-rules)

## Getting Started

### Prerequisites
- JDK 17 or higher
- Git
- Gradle (wrapper included)

### Setup Development Environment
```bash
# Clone the repository
git clone https://github.com/YOUR_USERNAME/evolvdb.git
cd evolvdb

# Build the project
./gradlew build

# Run tests
./gradlew test
```

## Development Workflow

### 1. Create a Feature Branch
```bash
git checkout -b feat/your-feature-name
# or
git checkout -b fix/bug-description
```

### 2. Make Your Changes
- Follow the code style guidelines
- Write tests for new functionality
- Update documentation as needed
- Keep commits atomic and well-documented

### 3. Commit Your Changes
Use conventional commit format:
```bash
git commit -m "feat: Add new feature description"
git commit -m "fix: Fix bug description"
git commit -m "docs: Update documentation"
```

Commit types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting)
- `refactor`: Code refactoring
- `perf`: Performance improvements
- `test`: Adding or updating tests
- `build`: Build system changes
- `ci`: CI/CD changes
- `chore`: Other changes

### 4. Push to Your Fork
```bash
git push origin feat/your-feature-name
```

### 5. Create a Pull Request
- Go to the GitHub repository
- Click "New Pull Request"
- Fill out the PR template completely
- Wait for CI checks to pass
- Address review comments

## Code Guidelines

### Java Code Style
- Use **English** for all code, comments, and documentation
- Follow **camelCase** for methods and variables
- Follow **PascalCase** for class names
- Use **UPPER_SNAKE_CASE** for constants
- Write **meaningful, descriptive names**
- Keep functions **small and focused** (Single Responsibility Principle)
- **Comment the "why", not the "what"**

### Architecture Principles
- Follow **Clean Architecture** layers (SQL → AST → Logical → Physical → Execution)
- Apply **SOLID principles**
- Use **design patterns** appropriately (Visitor, Strategy, Factory, etc.)
- Maintain **zero external dependencies** (pure Java)
- Keep code **DRY** (Don't Repeat Yourself)

### Documentation
- Add Javadoc for all public classes and methods
- Update relevant markdown docs in `docs/` folder
- Include examples in documentation
- Update README.md if adding major features

## Testing Requirements

### Test Coverage
- All new features **must** have unit tests
- Aim for **>80% code coverage** on new code
- Write integration tests for end-to-end flows
- Test edge cases and error conditions

### Running Tests
```bash
# Run all tests
./gradlew test

# Run tests for specific module
./gradlew :evolvdb-sql:test

# Run with coverage report
./gradlew test jacocoTestReport

# View coverage report
open build/reports/jacoco/test/html/index.html
```

### Test Structure
```java
@Test
public void givenCondition_whenAction_thenExpectedResult() {
    // Arrange
    // ... setup test data
    
    // Act
    // ... execute the action
    
    // Assert
    // ... verify the results
}
```

## Pull Request Process

### Before Creating a PR
1. **Ensure all tests pass**
   ```bash
   ./gradlew clean build
   ```

2. **Update documentation**
   - Add/update docs in `docs/` folder
   - Update README if needed
   - Add code comments

3. **Check code style**
   ```bash
   ./gradlew checkstyleMain checkstyleTest
   ```

4. **Rebase on main**
   ```bash
   git fetch origin
   git rebase origin/main
   ```

### PR Requirements
All PRs to `main` branch must:
- ✅ Pass all CI checks (build + test)
- ✅ Have approval from at least one reviewer
- ✅ Have no merge conflicts
- ✅ Follow commit message conventions
- ✅ Include tests for new functionality
- ✅ Update documentation
- ✅ Pass CodeQL security analysis

### PR Review Process
1. **Automated Checks** run on PR creation
   - Build verification (Java 17 & 21)
   - Test execution
   - Code quality checks
   - Security analysis (CodeQL)

2. **Manual Review** by maintainers
   - Code quality and style
   - Architecture consistency
   - Test coverage
   - Documentation completeness

3. **Address Feedback**
   - Make requested changes
   - Push updates to the same branch
   - CI checks will re-run automatically

4. **Merge**
   - Once approved and all checks pass
   - Squash commits if needed
   - PR will be merged to main

## Branch Protection Rules

### Main Branch Protection
The `main` branch has the following protections:

**Required Status Checks:**
- ✅ CI Build (Java 17)
- ✅ CI Build (Java 21)
- ✅ All tests must pass
- ✅ Code quality checks
- ✅ CodeQL security analysis

**Branch Rules:**
- ✅ Require pull request before merging
- ✅ Require at least 1 approval
- ✅ Dismiss stale reviews on new commits
- ✅ Require status checks to pass
- ✅ Require branches to be up to date before merging
- ✅ Require conversation resolution before merging
- ✅ Require signed commits (recommended)
- 🚫 Direct push to main is disabled

### Setting Up Branch Protection

Repository maintainers can set up branch protection via:

**GitHub UI:**
1. Go to Settings → Branches
2. Add rule for `main` branch
3. Enable:
   - "Require a pull request before merging"
   - "Require status checks to pass before merging"
     - Select: `build (17)`, `build (21)`, `pr-status`
   - "Require branches to be up to date before merging"
   - "Require conversation resolution before merging"

**GitHub CLI:**
```bash
gh api repos/OWNER/REPO/branches/main/protection \
  --method PUT \
  --field required_status_checks='{"strict":true,"contexts":["build (17)","build (21)","pr-status"]}' \
  --field enforce_admins=true \
  --field required_pull_request_reviews='{"required_approving_review_count":1,"dismiss_stale_reviews":true}' \
  --field restrictions=null
```

## Milestone Development

We follow a structured milestone approach:
- Each milestone (M12-M25) has clear deliverables
- See `docs/milestones/ROADMAP_M12-M25.md` for details
- Create issues for each milestone task
- Link PRs to milestone issues

## Questions?

- Open an issue for bugs or feature requests
- Start a discussion for questions
- Review existing documentation in `docs/`

## License

By contributing, you agree that your contributions will be licensed under the project's license.

---

**Happy Contributing! 🚀**
