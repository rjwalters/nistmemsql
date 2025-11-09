# Phase 3.2.2 Join Tree Reordering - Plan Summary

## Quick Overview

**Phase 3.2.1** ✅ (Complete): Join order analysis - detects optimal join order
**Phase 3.2.2** ⏳ (Planning): Join tree execution - applies the optimal order

## The Problem

Optimal order computed:  `t3 → t2 → t1` (50% filtering first)  
But executed as:          `t1 → t2 → t3` (massive cartesian products)

Schema tracking challenge: Join conditions reference tables that move through combined schema.

## Three Implementation Options

| Option | Approach | Complexity | Risk | Benefit |
|--------|----------|-----------|------|---------|
| **A** | Expression rewriting to column indices | Medium | Medium | Full optimization, clean code |
| **B** | Row reordering after joins | Low | Low | Some benefit, simpler |
| **C** | Step-by-step join reconstruction | High | High | Full optimization, complex |

## Recommended Path: Hybrid

### Phase 3.2.2a: Foundation (Low Risk)
- Create `ExpressionMapper` struct to track schema evolution
- No behavior changes, just infrastructure

### Phase 3.2.2b: Selective Reordering (Medium Risk)
- Handle linear join chains only: `A ⋈ B ⋈ C`
- All conditions must be equijoins
- Graceful fallback to standard order if unsafe

### Phase 3.2.2c: General (High Risk, Optional)
- Support branching joins: `(A ⋈ B) ⋈ (C ⋈ D)`
- Complex expression remapping
- Foundation for cost-based optimizer

## Key Design Decisions

1. **Graceful Degradation**: If any condition cannot be remapped, fall back to standard order
2. **Safety First**: Only reorder when provably safe (linear chains, all equijoins)
3. **Progressive Benefit**: Start with simple cases, expand complexity over time
4. **No Breaking Changes**: All existing tests continue to pass

## Files to Create/Modify

### New
- `crates/executor/src/select/join/expression_mapper.rs` - Schema tracking
- `crates/executor/src/select/join/reorder_executor.rs` - Reordering logic
- `crates/executor/src/tests/phase3_2_2_reordering.rs` - Test suite

### Modified
- `crates/executor/src/select/join/mod.rs` - Add reordering path
- `crates/executor/src/select/scan.rs` - Pass mapper through joins

## Timeline

- **Phase 3.2.2a**: 3-4 hours (infrastructure)
- **Phase 3.2.2b**: 4-5 hours (selective reordering)
- **Phase 3.2.2c**: 6-8 hours (general case)
- **Total**: 12-17 hours focused work

## Decision Required

Choose one:

1. **Implement Full** (3.2.2a + b + c)
   - Time: 2-3 days focused work
   - Benefit: Complete optimization
   - Risk: Moderate complexity

2. **Implement Selective** (3.2.2a + b only)
   - Time: 1 day focused work
   - Benefit: Most common cases (linear chains)
   - Risk: Low
   - ✅ **RECOMMENDED**

3. **Defer**
   - Keep Phase 3.2.1 (detection infrastructure)
   - Keep Phase 3.1 (hash join optimization - still substantial)
   - Ship with current capability
   - Revisit for future cost-based optimizer

## Performance Impact

Expected 50-1000x improvement for multi-table joins with filters on later tables.

Example:
```
Query: t1 (10K) ⋈ t2 (100K) ⋈ t3 (10K, filtered to 100 rows)

Standard:  1B × 10K = 10T operations
Reordered: 100 × 100K = 10M operations  (1000x better!)
```

## SQLite Architecture Reference

SQLite doesn't rewrite expressions; it manages **execution context** through:
- `sqlite3WhereBegin` API tracks which table is outer/inner loop
- Query flattening restructures loops at compilation time
- Cost model drives table ordering decisions

**Key learning**: Context management (which table is "current" schema) is more important than expression rewriting.

## Documentation

Created:
- `PHASE_3_2_2_PLANNING.md` - Detailed analysis of three options
- `PHASE_3_2_2_EXAMPLES.md` - Concrete code examples
- `PHASE_3_2_2_SUMMARY.md` - This document

---

**Status**: Ready for decision and implementation
**Current Branch**: `feature/issue-1036` (clean, 5 commits, compiles)
**Next Step**: Review plan → Make decision → Begin implementation
