---
trigger: always_on
description: 
globs: 
---

# IntelliJ Backend Workspace Rule
Ensures CLEAN ARCHITECTURE, SOLID PRINCIPLES, and MAINTAINABLE BACKEND DEVELOPMENT
for Java/Kotlin projects (Spring Boot, Micronaut, Ktor) in IntelliJ.

## Architecture Guidelines
<architecture_guidelines_backend>
- FOLLOW CLEAN ARCHITECTURE PRINCIPLES:
    - **Controller / API Layer:** Handles HTTP/REST requests, input/output mapping.
    - **Service / Application Layer:** Business logic, orchestration of use cases.
    - **Domain Layer:** Core business entities, rules, and domain services.
    - **Infrastructure Layer:** Persistence, integrations, and messaging.
- KEEP DEPENDENCIES ONE-DIRECTIONAL (Controller → Service → Domain → Infrastructure).
- DEPEND ON ABSTRACTIONS (Interfaces), NOT IMPLEMENTATIONS.
- ENFORCE CLEAR PACKAGE STRUCTURE AND MODULE BOUNDARIES.
</architecture_guidelines_backend>

## SOLID and OOP Principles
<oop_and_solid_principles_backend>
- APPLY SOLID PRINCIPLES:
    - **S**: Single responsibility.
    - **O**: Open for extension, closed for modification.
    - **L**: Subtypes should be substitutable for base types.
    - **I**: Use small, focused interfaces.
    - **D**: Depend on abstractions, not concretions.
- PREFER COMPOSITION OVER INHERITANCE.
</oop_and_solid_principles_backend>

## Design Patterns
<design_patterns_backend>
- USE APPROPRIATE DESIGN PATTERNS:
    - Factory / Builder → Object creation.
    - Strategy / Command → Behavioral flexibility.
    - Repository / Specification → Data access abstraction.
    - Adapter / Facade / Decorator → Structural enhancement.
    - Observer / Event → Decoupled communication.
</design_patterns_backend>

## Clean Code Practices
<clean_code_principles_backend>
- WRITE SMALL, FOCUSED FUNCTIONS AND CLASSES.
- USE MEANINGFUL NAMES AND CONSISTENT FORMATTING.
- COMMENT ONLY TO EXPLAIN **WHY**, NOT **WHAT**.
- REMOVE DUPLICATION (DRY).
- AVOID STATIC UTILITY CLASSES; USE DEPENDENCY INJECTION.
</clean_code_principles_backend>

## Scalability & Extensibility
<scalability_and_extensibility_backend>
- ABSTRACT DEPENDENCIES (DB, Kafka, External APIs).
- USE DEPENDENCY INJECTION CONSISTENTLY.
- DESIGN MODULES FOR INDEPENDENT DEPLOYMENT AND TESTING.
- AVOID SHARED MUTABLE STATE FOR SCALABILITY.
</scalability_and_extensibility_backend>

## Testing Guidelines
<testing_guidelines_backend>
- FOLLOW TDD/BDD PRACTICES WHERE POSSIBLE.
- TEST BUSINESS LOGIC, REPOSITORIES, AND APIS.
- USE JUnit5, Mockito, MockK, and Testcontainers.
- STRUCTURE TESTS AS **ARRANGE–ACT–ASSERT**.
</testing_guidelines_backend>

## Performance Guidelines
<performance_guidelines_backend>
- AVOID UNNECESSARY OBJECT CREATION OR SYNCHRONIZATION.
- USE CACHING AND CONNECTION POOLS WISELY.
- ADD TIMEOUTS/RETRIES FOR NETWORK CALLS.
- MEASURE BEFORE OPTIMIZING.
</performance_guidelines_backend>

## Security Guidelines
<security_guidelines_backend>
- VALIDATE INPUTS, SANITIZE OUTPUTS.
- NEVER LOG SENSITIVE DATA.
- USE ENCRYPTION FOR SECRETS, TOKENS, AND CREDENTIALS.
- IMPLEMENT AUTHENTICATION/AUTHORIZATION (JWT, OAuth2).
- FOLLOW OWASP TOP 10.
</security_guidelines_backend>

## Documentation Guidelines
<documentation_guidelines_backend>
- ADD JAVADOC/KDOC FOR PUBLIC CLASSES & METHODS.
- DOCUMENT MODULE STRUCTURE, DESIGN DECISIONS, AND DEPENDENCIES.
- MAINTAIN ARCHITECTURE DIAGRAMS & READMEs PER MODULE.
- DOCUMENT EXTERNAL SERVICE CONTRACTS (API, Kafka, DB).
</documentation_guidelines_backend>

## AI Behavior
<ai_behavior_backend>
- WHEN GENERATING CODE:
    - FOLLOW CLEAN ARCHITECTURE STRUCTURE (Controller → Service → Domain → Repository).
    - NEVER MIX BUSINESS LOGIC INTO CONTROLLERS.
    - DEFAULT TO DEPENDENCY INJECTION AND INTERFACES.
    - AUTO-GENERATE TEST STUBS FOR NEW BUSINESS LOGIC.
    - JUSTIFY DESIGN PATTERN USAGE.
    - PROPOSE REFACTORING IF CODE SMELLS ARE FOUND.
    - ASK FOR CLARIFICATION IF REQUIREMENTS ARE UNCLEAR.
</ai_behavior_backend>
