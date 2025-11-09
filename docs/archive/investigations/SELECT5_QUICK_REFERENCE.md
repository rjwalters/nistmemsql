# Quick Reference: select5.test Memory Issue

## Problem
`select5.test` contains queries with 64-table joins that cause memory exhaustion (6.48 GB) before returning results.

## Root Cause
WHERE clauses are applied **after** the entire FROM clause (all JOINs), not during join evaluation.

## Current Flow
```
FROM t1,t2,...,t64 → [Cartesian Product: 10^64 rows] → WHERE filter → Result: 35 rows
                        ↑ EXPLODES TO 6.48 GB HERE
```

## Proposed Flow (Predicate Pushdown)
```
FROM t1 → Filter t1 → Join t2 → Filter join result → ... → Result: 35 rows
         ~5 rows    ~5 rows    [kept small at each step]
```

## Key Files to Modify

| File | Purpose | Change |
|------|---------|--------|
| `crates/executor/src/select/executor/nonagg.rs` | Main executor | Decompose WHERE before FROM |
| `crates/executor/src/select/scan.rs` | FROM execution | Accept table-local WHERE predicates |
| `crates/executor/src/select/join/mod.rs` | Join dispatcher | Pass equijoin conditions to join operators |
| (new) `crates/executor/src/optimizer/where_pushdown.rs` | WHERE analyzer | Classify and extract pushable predicates |

## Example: 2-Table Join Optimization

### Before
```rust
// nested_loop.rs: evaluate all combinations first
for left_row in &left.rows {  // 10 rows
    for right_row in &right.rows {  // 10 rows
        let combined = combine_rows(left_row, right_row);  // 100 combinations
        if condition.eval(&combined) { result.push(...) }  // Only 5 match
    }
}
// Memory: 100 temporary combined rows
```

### After
```rust
// WHERE is: a1=5 AND b2=10 AND a1=b1
// Extract:
//   - a1=5 → push to t1 scan
//   - b2=10 → push to t2 scan
//   - a1=b1 → keep as join condition

// scan.rs: apply table-local filters first
let t1_filtered = t1.scan().filter(|r| r.a1 == 5);  // 5 rows
let t2_filtered = t2.scan().filter(|r| r.b2 == 10); // 3 rows

// join/mod.rs: join with equijoin condition
for t1_row in &t1_filtered {  // 5 rows
    for t2_row in &t2_filtered {  // 3 rows
        if t1_row.a1 == t2_row.b1 { result.push(...) }  // 2 match
    }
}
// Memory: 15 temporary combined rows (not 100)
```

## Testing select5.test

### Current State
```bash
$ cargo test --test sqllogictest -- select5
# Hangs indefinitely, memory climbs to 6.48 GB
# Worker 2 reports warning
```

### After Fix (Expected)
```bash
$ cargo test --test sqllogictest -- select5
# Completes in ~1 second
# Memory stays <100 MB
# All 360 pathological queries pass
```

## Implementation Phases

### Phase 1: Predicate Analysis
- Create WHERE decomposer
- Classify conditions (table-local, equijoin, complex)
- Unit test classification logic

### Phase 2: Execution Changes
- Pass predicates through FROM clause execution
- Apply table-local filters in scan.rs
- Pass equijoin conditions to join operators
- Keep complex predicates for post-join WHERE

### Phase 3: Testing & Validation
- Integration tests (2, 4, 8, 64 table joins)
- Verify select5.test passes
- Performance benchmarks
- No regression on existing tests

## Success Criteria

✓ select5.test completes without hanging  
✓ Memory usage <100 MB for 64-table queries  
✓ All 360+ pathological queries return correct results  
✓ No regression on existing test suite  
✓ Query latency improves 100-1000x for multi-table joins  

## Related GitHub Issue
**#1036**: Predicate Pushdown: select5.test Memory Exhaustion (6.48 GB)
- Full technical specification
- Implementation plan with checkboxes
- Risk assessment
- Code references

## Timeline
- Analysis: ✓ Complete
- Design: ✓ Complete  
- Implementation: Pending (Phase 1-3)
- Testing: Pending
- Validation: Pending

## Questions?
See `ISSUE_SELECT5_MEMORY.md` for full technical details or `ANALYSIS_SELECT5_INVESTIGATION.md` for investigation findings.
