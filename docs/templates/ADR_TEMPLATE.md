# ADR-XXXX: [Short Title of Decision]

**Status**: Proposed | Accepted | Deprecated | Superseded by ADR-YYYY

**Date**: YYYY-MM-DD

**Deciders**: [List who was involved in the decision]

**Related**: [Links to related ADRs or documents]

## Context and Problem Statement

[Describe the context and the problem we're trying to solve. What forces are at play? What constraints exist?]

Example:
"We need to choose a parsing strategy for SQL:1999. The grammar is complex and context-sensitive, with over 500 production rules. We need something that can handle the full grammar, provides good error messages, and can be maintained as we discover edge cases."

## Decision Drivers

* [Driver 1: e.g., Performance requirements]
* [Driver 2: e.g., Maintainability]
* [Driver 3: e.g., Time to implement]
* [Driver 4: e.g., Compatibility with existing code]

## Considered Options

### Option 1: [Name]

**Description**: [Brief description]

**Pros**:
* [Good point 1]
* [Good point 2]

**Cons**:
* [Bad point 1]
* [Bad point 2]

**Code Example** (if applicable):
```rust
// Show what this would look like
```

### Option 2: [Name]

[Same structure as Option 1]

### Option 3: [Name]

[Same structure as Option 1]

## Decision Outcome

**Chosen option**: "[Option name]"

**Rationale**: [Explain why this option was chosen. What made it the best choice given the drivers and constraints?]

Example:
"We chose ANTLR because it has existing SQL grammars we can adapt, provides excellent error recovery, and is widely used in production parsers. While it has a learning curve, the long-term maintainability benefits outweigh the initial complexity."

## Consequences

### Positive
* [Good consequence 1]
* [Good consequence 2]

### Negative
* [Bad consequence 1 - and how we'll mitigate it]
* [Bad consequence 2 - and how we'll mitigate it]

### Neutral
* [Impact 1]
* [Impact 2]

## Implementation Notes

[Any practical notes about implementing this decision]

Example:
"We'll need to add ANTLR as a build dependency. The parser generation step will be part of the build process. See docs/implementation/PARSER_SETUP.md for details."

## Validation

[How will we know if this decision was correct?]

Example:
"Success criteria: Parser handles all SQL:1999 test cases within 100ms per query, provides helpful error messages for invalid SQL, and grammar is maintainable by team."

## References

* [Link to relevant documentation]
* [Research papers, blog posts, etc.]
* [Related code or examples]

## Updates

### YYYY-MM-DD
[Note any significant updates to this decision or its implementation]
