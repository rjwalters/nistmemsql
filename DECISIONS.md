# Architecture Decision Records Index

This document provides a quick reference to all architectural decisions made during the nistmemsql project.

## What are ADRs?

Architecture Decision Records (ADRs) document important architectural decisions, their context, and consequences. Each ADR is numbered sequentially and stored in `docs/decisions/`.

## Format

Each ADR follows this structure:
- **Context**: What problem are we solving?
- **Options Considered**: What alternatives did we evaluate?
- **Decision**: What did we choose?
- **Consequences**: What are the implications?

See `docs/templates/ADR_TEMPLATE.md` for the full template.

## Current Decisions

| ID | Title | Status | Date | Summary |
|----|-------|--------|------|---------|
| 0001 | **Language Choice: Rust** | ✅ **Accepted** | 2024-10-25 | **Rust chosen** for type safety, pattern matching, compiler feedback, and correctness focus |
| 0002 | **Parser Strategy: Hand-Written** | ✅ **Accepted** | 2024-10-25 | **Hand-written recursive descent + Pratt parser** chosen for full control, TDD alignment, and SQL:1999 flexibility |

## Decisions by Category

### Core Technology Stack
- [ADR-0001](docs/decisions/0001-language-choice.md) - **Language Choice: Rust** ✅ (Accepted 2024-10-25)
- [ADR-0002](docs/decisions/0002-parser-strategy.md) - **Parser Strategy: Hand-Written** ✅ (Accepted 2024-10-25)

## Decision Process

### When to Create an ADR

Create an ADR when:
- Making a choice between multiple viable approaches
- The decision is hard to reverse
- The decision significantly impacts the system architecture
- Future developers will need context on "why did we do it this way?"

### ADR Lifecycle

1. **Proposed**: Decision is being considered
2. **Accepted**: Decision is approved and implementation begins
3. **Deprecated**: Decision no longer applies (document why)
4. **Superseded**: Replaced by a new decision (link to successor)

### How to Create a New ADR

1. Copy `docs/templates/ADR_TEMPLATE.md`
2. Number it sequentially (next number in sequence)
3. Fill in the template
4. Discuss with team (if applicable) or AI assistant
5. Update this index when status changes
6. Link from related documents

## Quick Reference

### Recently Accepted
- **ADR-0001**: Language Choice - Rust (2024-10-25)
- **ADR-0002**: Parser Strategy - Hand-Written Recursive Descent (2024-10-25)

### Pending Review
[None yet - future ADRs will be created as architectural decisions arise]

### Superseded/Deprecated
[None yet]

## Related Documentation

- [Documentation Guide](docs/README.md) - Overview of all documentation
- [Lessons Learned](LESSONS_LEARNED.md) - Insights from implementation
- [Architecture Docs](docs/architecture/) - Detailed component designs

## Contributing

When making architectural decisions:

1. **Document early**: Create the ADR when you start evaluating options, not after
2. **Be thorough**: Explain context, options, trade-offs
3. **Update status**: Keep this index current
4. **Link liberally**: Reference ADRs from code comments and other docs
5. **Review regularly**: ADRs inform future decisions

---

**Last Updated**: 2025-10-26
**Total ADRs**: 2 accepted, 0 pending
**Status**: Foundation established - language and parser strategy implemented!
