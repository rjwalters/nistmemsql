# Architecture Decision Records (ADRs) Summary

This directory contains Architecture Decision Records documenting important technical decisions made during the project. Each ADR captures the context, alternatives considered, decision made, and consequences.

## 📋 Decision Records

### Core Technical Decisions

- **[0001-language-choice.md](0001-language-choice.md)** - **Choose Rust as Implementation Language**
  - *Decision*: Use Rust for type safety, pattern matching, and correctness guarantees
  - *Date*: 2024-10-25
  - *Status*: Accepted

- **[0002-parser-strategy.md](0002-parser-strategy.md)** - **Parser Strategy for SQL:1999**
  - *Decision*: Use hand-written recursive descent parser with nom combinators
  - *Date*: 2024-10-25
  - *Status*: Accepted
  - *Rationale*: Handles SQL:1999 grammar complexity, context-sensitive parsing, and error recovery

- **[0003-rebranding.md](0003-rebranding.md)** - **Project Rebranding to "vibesql"**
  - *Decision*: Plan to rebrand from "nistmemsql" to "vibesql" to reflect AI-generated nature
  - *Date*: Not yet implemented
  - *Status*: Proposed
  - *Note*: Planned for future when appropriate

### Decision Log

- **[DECISION_LOG.md](DECISION_LOG.md)** - Comprehensive index of all architectural decisions with summaries and cross-references

## 📖 ADR Format

Each ADR follows a standard template:
- **Title**: Clear description of the decision
- **Status**: Proposed, Accepted, Deprecated, Superseded
- **Date**: When the decision was made
- **Deciders**: Who participated in the decision
- **Context**: The situation and problem requiring a decision
- **Decision**: What was decided and why
- **Alternatives Considered**: Other options that were evaluated
- **Consequences**: Positive and negative outcomes
- **Related Documents**: Links to relevant documentation

## 🔍 Finding Decisions

**Language and tools?** → See ADR-0001 (Language Choice) and ADR-0002 (Parser Strategy)

**Project direction?** → See ADR-0003 (Rebranding) and DECISION_LOG.md

**All decisions at a glance?** → Read DECISION_LOG.md for comprehensive overview

## 💡 When to Create an ADR

Create a new ADR when:
- Making a decision that is hard to reverse (technology choices, architecture patterns)
- Choosing between multiple viable approaches with trade-offs
- Establishing standards or conventions that will affect the codebase
- Making decisions that future contributors will need to understand

**ADR Template**: See [../templates/ADR_TEMPLATE.md](../templates/ADR_TEMPLATE.md)

## 🔗 Related Documentation

- [Documentation Guide](../README.md) - Overview of all documentation
- [Lessons Learned](../lessons/LESSONS_LEARNED.md) - Insights from implementing these decisions
- [Archive](../archive/) - Historical context for decisions

---

**Last Updated**: 2025-11-03
**Total ADRs**: 3 accepted/proposed
