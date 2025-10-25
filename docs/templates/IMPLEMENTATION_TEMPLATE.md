# [Feature Name] Implementation Guide

**Feature ID**: [e.g., E011, F031, T131]

**SQL:1999 Reference**: Part X, Section Y

**Status**: Not Started | In Progress | Complete | Tested

**Last Updated**: YYYY-MM-DD

**Difficulty**: Low | Medium | High | Very High

**Related**:
- [Architecture doc]
- [Related ADRs]
- [Related features]

## Feature Overview

### SQL:1999 Specification

[What does the SQL:1999 standard say about this feature?]

Quote from standard (if available):
> "The exact text from the specification"

### Practical Description

[Explain in plain English what this feature does and why it matters]

Example:
"The BOOLEAN data type (T031) allows columns to store TRUE, FALSE, or UNKNOWN values. This is essential for SQL:1999 compliance and enables cleaner WHERE clause logic."

### Examples

```sql
-- Example 1: Basic usage
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    is_active BOOLEAN
);

-- Example 2: In WHERE clauses
SELECT * FROM users WHERE is_active IS TRUE;

-- Example 3: Edge case
SELECT * FROM users WHERE is_active IS UNKNOWN;
```

## Prerequisites

### Dependencies

This feature requires:
* [Feature/component 1] - because [reason]
* [Feature/component 2] - because [reason]

### Knowledge Required

Developers should understand:
* [Concept 1]
* [Concept 2]

### Prior Reading

Recommended:
* [Architecture doc to read]
* [ADR to review]

## Implementation Plan

### High-Level Approach

[Describe the overall strategy for implementing this feature]

### Steps

1. **[Step 1 name]**
   - Description: [What to do]
   - Location: [Where in codebase]
   - Estimated time: [How long]

2. **[Step 2 name]**
   - Description
   - Location
   - Estimated time

3. **[Step 3 name]**
   - [Continue pattern]

### Component Changes

#### Parser Changes

**Files**: `crates/parser/src/[file].rs`

**Changes**:
```rust
// Add to grammar
pub enum DataType {
    Integer,
    Varchar(usize),
    Boolean,  // <- Add this
}
```

#### Type System Changes

**Files**: `crates/types/src/[file].rs`

**Changes**:
[Describe type system additions]

#### Storage Changes

**Files**: `crates/storage/src/[file].rs`

**Changes**:
[Describe storage modifications]

#### Execution Changes

**Files**: `crates/execution/src/[file].rs`

**Changes**:
[Describe execution engine updates]

## Detailed Implementation

### Data Structures

```rust
// Define key data structures for this feature
pub struct FeatureData {
    // fields
}
```

### Algorithms

```rust
// Show key algorithms or logic
pub fn feature_operation() -> Result<Output> {
    // implementation
}
```

### Error Handling

```rust
// What errors can occur?
pub enum FeatureError {
    // error variants
}
```

## Testing

### Unit Tests

**Location**: `crates/[component]/tests/`

```rust
#[test]
fn test_basic_functionality() {
    // test code
}

#[test]
fn test_edge_case_1() {
    // test code
}
```

### Integration Tests

**Location**: `tests/sql1999-[core|optional]/[feature]/`

```sql
-- Test 1: Basic case
CREATE TABLE t1 (b BOOLEAN);
INSERT INTO t1 VALUES (TRUE);
SELECT * FROM t1 WHERE b IS TRUE;
-- Expected: 1 row

-- Test 2: NULL handling
INSERT INTO t1 VALUES (NULL);
SELECT * FROM t1 WHERE b IS UNKNOWN;
-- Expected: 1 row
```

### ODBC Tests

**Location**: `tests/odbc/[feature]/`

[Describe ODBC-specific test requirements]

### JDBC Tests

**Location**: `tests/jdbc/[feature]/`

[Describe JDBC-specific test requirements]

## Edge Cases and Gotchas

### Edge Case 1: [Name]

**Issue**: [What's tricky about this?]

**Solution**: [How to handle it]

**Test**: [Test to verify correct behavior]

### Edge Case 2: [Name]

[Continue pattern]

## SQL:1999 Compliance Notes

### Mandatory Requirements

- [ ] Requirement 1 from spec
- [ ] Requirement 2 from spec
- [ ] Requirement 3 from spec

### Optional Requirements

- [ ] Optional feature 1
- [ ] Optional feature 2

### Known Gaps

[Any spec requirements we're not implementing yet]

## Performance Considerations

### Expected Impact

[How does this feature affect performance?]

### Optimizations

[Any optimizations to consider]

## Examples from Other Databases

### PostgreSQL

```sql
-- How PostgreSQL implements this
```

**Notes**: [Observations about PostgreSQL's approach]

### SQLite

```sql
-- How SQLite implements this
```

**Notes**: [Observations]

## Common Mistakes

1. **Mistake**: [What not to do]
   **Why**: [Reason it's wrong]
   **Instead**: [Correct approach]

2. **Mistake**: [Another pitfall]
   [Continue pattern]

## Debugging Tips

* **Issue**: [Common problem]
  **Debug approach**: [How to investigate]

## References

### SQL:1999 Standard
* Part X, Section Y.Z: [Specific reference]

### Helpful Resources
* [Links to relevant documentation, blog posts, papers]

### Related Implementations
* PostgreSQL: [Link to source code]
* SQLite: [Link to source code]

## Progress Tracking

### Checklist

- [ ] Parser grammar updated
- [ ] Type system supports feature
- [ ] Storage layer updated
- [ ] Execution engine implements logic
- [ ] Unit tests written and passing
- [ ] Integration tests written and passing
- [ ] ODBC tests passing
- [ ] JDBC tests passing
- [ ] Documentation updated
- [ ] Code reviewed
- [ ] COMPLIANCE.md updated

### Blocking Issues

[Any issues blocking completion]

### Next Steps

1. [Immediate next action]
2. [Following action]

## Lessons Learned

[Update this section as you implement. What did you learn? What was harder than expected? What was easier?]

## Changelog

### YYYY-MM-DD - [Author]
[Implementation updates]
