# select5.test Memory Issue - Complete Documentation Index

## Overview

On November 8, 2025, an investigation identified that `select5.test` (31,948 lines of SQLLogicTest conformance tests) contains pathological queries with 64-table joins that cause memory exhaustion at 6.48 GB. The root cause has been identified as lack of predicate pushdown in the query optimizer. A comprehensive architectural proposal has been filed.

## Documents Created

### 1. **ISSUE_SELECT5_MEMORY.md** ⭐ MAIN ISSUE
   - **Purpose**: Complete technical specification for GitHub Issue #1036
   - **Contains**:
     - Problem summary with examples
     - Root cause analysis
     - Proposed predicate pushdown solution with algorithm
     - 3-phase implementation plan with checkboxes
     - Risk assessment and edge cases
     - Test file analysis
   - **Audience**: Developers implementing the solution
   - **Status**: Submitted as GitHub Issue #1036

### 2. **ANALYSIS_SELECT5_INVESTIGATION.md**
   - **Purpose**: Detailed investigation findings
   - **Contains**:
     - Test file analysis (31,948 lines, ~360 pathological queries)
     - Memory analysis (why 10^64 intermediate rows explode)
     - Code flow investigation (where WHERE clause is applied)
     - Memory limits check (current safeguards insufficient)
     - Why predicate pushdown is the solution
     - Detection method (how the issue was found)
     - Performance impact analysis with table
     - Design decisions and alternatives considered
   - **Audience**: Technical leads, architects
     - Technical depth: High
   - **Best for**: Understanding why this approach is necessary

### 3. **SELECT5_QUICK_REFERENCE.md**
   - **Purpose**: Quick start guide for implementation
   - **Contains**:
     - Problem statement (1 sentence)
     - Root cause (1 sentence)
     - Before/after flow diagrams
     - Key files to modify with descriptions
     - Example: 2-table join optimization (Before/After code)
     - Implementation phases overview
     - Success criteria (checkmark list)
     - Testing instructions
   - **Audience**: Developers starting implementation
   - **Best for**: Quick orientation before coding
   - **Technical depth**: Medium

### 4. **SELECT5_DOCUMENTATION_INDEX.md** (This file)
   - **Purpose**: Navigation guide for all documentation
   - **Contains**: Overview of all documents and how to use them

---

## How to Use This Documentation

### If you want to...

**Understand the problem quickly**
→ Start with: `SELECT5_QUICK_REFERENCE.md` (5 min read)

**Implement the solution**
→ Start with: `SELECT5_QUICK_REFERENCE.md`, then `ISSUE_SELECT5_MEMORY.md` Phase 1
→ Reference: Code locations in `ANALYSIS_SELECT5_INVESTIGATION.md`

**Learn the deep technical details**
→ Start with: `ANALYSIS_SELECT5_INVESTIGATION.md`
→ Then: `ISSUE_SELECT5_MEMORY.md` for solution design

**Manage the implementation**
→ Use: Checklist in `ISSUE_SELECT5_MEMORY.md` (Phase 1-3)
→ Track in: GitHub Issue #1036

**Estimate effort/timeline**
→ See: "Implementation Plan" in `SELECT5_QUICK_REFERENCE.md`
→ Details: "Implementation Plan" in `ISSUE_SELECT5_MEMORY.md`

---

## GitHub Issue

**#1036**: Predicate Pushdown: select5.test Memory Exhaustion (6.48 GB)
- Full technical specification (from ISSUE_SELECT5_MEMORY.md)
- 3-phase implementation plan with checkboxes
- Links to code files
- Risk assessment
- Status: Open, labeled as `loom:architect`, `loom:urgent`, `testing`

---

## Key Findings Summary

| Aspect | Finding |
|--------|---------|
| **Problem** | select5.test queries cause 6.48 GB memory exhaustion |
| **Root Cause** | WHERE clauses applied after JOINs, not during |
| **Test File** | 31,948 lines, ~360 pathological queries, max 64-table join |
| **Current Flow** | Cartesian product (10^64 rows) → WHERE filter |
| **Proposed Flow** | Table scan → Filter → Join → Filter → ... → Result |
| **Memory Impact** | 6.48 GB → <100 MB |
| **Performance Impact** | 100-1000x faster |
| **Solution** | Predicate pushdown (standard DB technique) |
| **Implementation** | 3 phases, 2-3 days estimated |
| **Risk** | Low (localized changes, standard technique) |

---

## Code Files to Modify

### Primary Files
```
crates/executor/src/select/executor/nonagg.rs
  ├─ Current: WHERE applied after FROM (lines 50-74)
  └─ Change: Decompose WHERE before FROM

crates/executor/src/select/scan.rs
  ├─ Current: No WHERE predicates at table scan
  └─ Change: Accept and apply table-local predicates

crates/executor/src/select/join/mod.rs
  ├─ Current: Generic join execution
  └─ Change: Receive extracted equijoin conditions
```

### New Files
```
crates/executor/src/optimizer/where_pushdown.rs (NEW)
  ├─ analyze_where_predicates()
  ├─ extract_table_local_predicates()
  └─ extract_equijoin_conditions()
```

### Reference Files
```
crates/executor/src/limits.rs
  └─ Memory limits (MAX_MEMORY_BYTES = 10 GB)

crates/executor/src/select/join/nested_loop.rs
  └─ Nested loop join implementation
```

---

## Testing Strategy

### Unit Tests (Phase 3)
- WHERE predicate decomposition
- Condition classification (table-local, equijoin, complex)
- Edge cases (NULLs, OR conditions, etc.)

### Integration Tests (Phase 3)
- 2-table joins with various predicates
- 4-table joins
- 8-table joins
- 64-table joins (select5.test validation)

### Validation
- All 360+ pathological queries return correct results
- Memory usage <100 MB for 64-table join
- No regression on existing test suite
- Performance benchmarks (100-1000x improvement)

---

## Timeline

```
Day 1-2: Phase 1 - WHERE Predicate Analysis
  ├─ Create where_pushdown.rs module
  ├─ Implement predicate classifier
  └─ Add unit tests

Day 2: Phase 2 - Execution Flow Changes
  ├─ Modify scan.rs to accept predicates
  ├─ Modify join/mod.rs for conditions
  ├─ Update nonagg.rs to decompose WHERE
  └─ Integration testing

Day 3: Phase 3 - Validation
  ├─ Verify select5.test passes
  ├─ Check memory usage (<100 MB)
  ├─ Run full test suite (no regressions)
  └─ Performance benchmarking
```

---

## Success Criteria

- [x] Issue identified and analyzed
- [x] Root cause documented
- [x] Solution proposed and approved
- [ ] Phase 1 implementation (WHERE analyzer)
- [ ] Phase 2 implementation (execution changes)
- [ ] Phase 3 implementation (testing)
- [ ] select5.test passes without hanging
- [ ] Memory usage <100 MB
- [ ] No regressions in existing tests
- [ ] Performance improvement verified (100-1000x)

---

## Related Documentation

- **SQLLOGICTEST_SUITE_STATUS.md** - Overall conformance test status
- **SQLLOGICTEST_ROADMAP.md** - Roadmap for passing conformance tests
- **SLOW_TEST_FILES.md** - Performance issues with test files
- **docs/performance/OPTIMIZATION.md** - Performance optimization strategies

---

## Contact

This investigation was completed on November 8, 2025.

For questions about this issue:
1. Review the relevant documentation above
2. Check GitHub Issue #1036 for updates
3. Refer to code comments in implementation files

---

## Document Status

| Document | Status | Last Updated | Completeness |
|----------|--------|--------------|--------------|
| ISSUE_SELECT5_MEMORY.md | ✓ Complete | Nov 8, 2025 | 100% |
| ANALYSIS_SELECT5_INVESTIGATION.md | ✓ Complete | Nov 8, 2025 | 100% |
| SELECT5_QUICK_REFERENCE.md | ✓ Complete | Nov 8, 2025 | 100% |
| SELECT5_DOCUMENTATION_INDEX.md | ✓ Complete | Nov 8, 2025 | 100% |
| GitHub Issue #1036 | ✓ Submitted | Nov 8, 2025 | 100% |

---

*This documentation index was created as part of the select5.test investigation. All referenced documents are complete and ready for implementation.*
