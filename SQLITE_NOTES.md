# SQLite Performance Analysis Notes

This document records findings from analyzing SQLite's source code to understand why their performance is superior to ours in microbenchmarks. These notes will guide optimization efforts.

**SQLite Submodule**: `docs/reference/sqlite/` (commit: eaacb4ac37ae111758f25711eff268a3575ae020)

---

## Table of Contents

1. [INSERT Performance](#insert-performance)
2. [SELECT COUNT(*) Performance](#select-count-performance)
3. [UPDATE Performance](#update-performance)
4. [DELETE Performance](#delete-performance)
5. [Priority Optimizations](#priority-optimizations)
6. [References](#references)

---

## INSERT Performance

**Key Files**: `docs/reference/sqlite/src/insert.c` (45,999 lines), `vdbe.c`, `btree.c`

### 1. Register-Based Virtual Machine Architecture

**SQLite's Approach** (`insert.c:923-932`, `1046-1055`):
```c
// Register allocations - all data flows through registers
int regRowid;         /* registers holding insert rowid */
int regData;          /* register holding first column to insert */
int *aRegIdx = 0;     /* One register allocated to each index */

// Allocate a block registers to hold the rowid and the values
regRowid = regIns = pParse->nMem+1;
regData = regRowid+1;
```

**Benefits**:
- **Zero-copy data flow**: Values move through numbered registers without allocation
- **Compile-time planning**: All register allocations done during query compilation
- **Minimal memory overhead**: Fixed register block per operation
- **Cache-friendly**: Sequential register access patterns

**Our Current Approach** (`crates/executor/src/insert/execution.rs:59-143`):
```rust
let mut validated_rows = Vec::new();
let mut primary_key_values: Vec<Vec<types::SqlValue>> = Vec::new();
let mut unique_constraint_values = vec![Vec::new(); ...];

// Multiple allocations per row
for value_exprs in &rows_to_insert {
    let mut full_row_values = vec![types::SqlValue::Null; schema.columns.len()];
    // ... clone and copy operations throughout
}
```

**Performance Gap**: We allocate 3-5 temporary Vec structures per row with multiple clones.

---

### 2. Batch Constraint Checking Strategy

**SQLite's Approach** (`insert.c:1895-1909`, `2711-2719`):
```c
void sqlite3GenerateConstraintChecks(
  Parse *pParse,
  Table *pTab,
  int *aRegIdx,        /* Register indices for constraints */
  int regNewData,      /* First register with new data */
  // ...
) {
  // All constraints checked in single pass
  // NOT NULL checks (lines 1959-2059)
  // CHECK constraints (lines 2061-2099)
  // UNIQUE constraints (lines 2100+)

  // Generate table record DURING constraint checks (line 2714)
  sqlite3VdbeAddOp3(v, OP_MakeRecord, regNewData+1, pTab->nNVCol, regRec);
```

**Key Pattern**:
- **Single-pass validation**: All constraints checked together in one VM bytecode sequence
- **Record generation during validation**: Avoids reprocessing data
- **Deferred constraint state**: Tracks what needs rechecking after triggers

**Our Current Approach** (`execution.rs:86-131`):
```rust
// Multiple separate validation passes per row
super::constraints::enforce_not_null_constraints(&schema, ...)?;
super::constraints::enforce_primary_key_constraint(db, &schema, ...)?;
super::constraints::enforce_unique_constraints(db, &schema, ...)?;
super::constraints::enforce_check_constraints(&schema, ...)?;
super::foreign_keys::validate_foreign_key_constraints(db, ...)?;
```

**Performance Gap**: 5 separate function calls per row with full context switch overhead.

---

### 3. Transfer Optimization (Bulk INSERT SELECT)

**SQLite's xferOptimization** (`insert.c:2988-3387`):
```c
// INSERT INTO tab1 SELECT * FROM tab2;
// Transfers raw records without decode/re-encode

// For VACUUM: use PREFORMAT flag to avoid record parsing
insFlags = OPFLAG_APPEND|OPFLAG_USESEEKRESULT|OPFLAG_PREFORMAT;
sqlite3VdbeAddOp3(v, OP_RowCell, iDest, iSrc, regRowid);  // Direct cell copy
sqlite3VdbeAddOp3(v, OP_Insert, iDest, regData, regRowid);
```

**Benefits**:
- **Zero serialization overhead**: Raw cell/record transfer
- **No type checking**: Assumes compatible schemas
- **B-tree page-level operations**: Bulk transfers

**Our Current Approach** (`execution.rs:30-51`):
```rust
ast::InsertSource::Select(select_stmt) => {
    let select_result = select_executor.execute_with_columns(select_stmt)?;
    // Convert each row to Expression literals (full deserialization)
    select_result.rows.into_iter()
        .map(|row| row.values.into_iter()
            .map(ast::Expression::Literal).collect())
        .collect()
}
```

**Performance Gap**: Full deserialization, AST conversion, and re-serialization. **No bulk transfer path at all**.

---

### 4. Append Optimization

**SQLite's Approach** (`insert.c:1515-1540`, `2836-2841`):
```c
// Detect monotonically increasing rowid patterns
if( pIpk->op==TK_NULL && !IsVirtual(pTab) ){
    sqlite3VdbeAddOp3(v, OP_NewRowid, regRowid, regAutoinc);
    appendFlag = 1;  // Signal append optimization
}

// Later during insertion (line 2837):
if( appendBias ){
    pik_flags |= OPFLAG_APPEND;  // B-tree append mode
}
```

**Benefits**:
- **B-tree optimization**: `OPFLAG_APPEND` tells B-tree to skip seek operations
- **O(1) insertion**: Append to rightmost leaf instead of searching
- **Cache-friendly**: Sequential page access
- **Auto-detected**: No user intervention required

**Our Storage Layer** (`crates/storage/src/table.rs:31-55`):
```rust
pub fn insert(&mut self, row: Row) -> Result<(), StorageError> {
    let row_index = self.rows.len();
    self.rows.push(normalized_row.clone());  // Always append to Vec
    self.update_indexes_for_insert(&normalized_row, row_index);
    Ok(())
}
```

**Status**:
- ✅ We already do Vec append (O(1) for main storage)
- ❌ But HashMap index updates are O(log n) per index
- ❌ No signal to skip uniqueness checks when monotonic

---

### 5. Memory Allocation Patterns

**SQLite's Strategy**:

1. **Parser-managed allocation** (`insert.c:439-449`):
   - Arena allocation: Batch allocations, batch frees
   - Automatic cleanup when parse completes

2. **Affinity string caching** (`insert.c:75-113`, `204-211`):
   ```c
   // Compute once, cache in Table/Index structure
   if( !pIdx->zColAff ) return computeIndexAffStr(db, pIdx);
   return pIdx->zColAff;  // Reuse cached string
   ```

3. **Register reuse** (`insert.c:2454`):
   - Register pools: Reuse virtual registers across operations
   - Single allocator for VM execution

**Our Approach**:
```rust
// Multiple independent allocations
let mut full_row_values = vec![types::SqlValue::Null; schema.columns.len()];
let new_pk_values: Vec<types::SqlValue> = pk_indices.iter()
    .map(|&idx| full_row_values[idx].clone()).collect();
```

**Performance Gap**: Standard Rust allocator overhead per Vec, many intermediate allocations, no pooling.

---

### INSERT Recommendations

#### Immediate (High ROI, Low Effort):

1. **Merge constraint checking passes** into single loop
   - Combine NOT NULL, CHECK, UNIQUE validation
   - Expected: 30-40% INSERT performance improvement
   - Related: Issue #815

2. **Implement bulk INSERT-SELECT fast path**
   - Detect compatible schema transfers
   - Skip validation for known-good data
   - Expected: 10-50x for bulk data loading

3. **Add append detection** for sequential inserts
   - Track monotonic primary key patterns
   - Skip HashMap lookups when safe
   - Expected: 2-5x for sequential workloads

#### Medium-Term (High ROI, Medium Effort):

4. **Introduce validation context object**
   - Pre-allocate once per batch
   - Reuse for all rows
   - Expected: 20-30% reduction in allocations

5. **Cache column metadata** in table schema
   - Type affinities, default expressions
   - Avoid recomputation per row
   - Expected: 10-15% improvement

---

## SELECT COUNT(*) Performance

**Key Files**: `docs/reference/sqlite/src/select.c`, `vdbe.c`, `btree.c`

### 1. COUNT(*) Fast Path Detection

**Pattern Detection** (`select.c:5557-5587`):

SQLite has a dedicated `isSimpleCount()` function that detects:
```sql
SELECT count(*) FROM <tbl>
```

**Eligibility Criteria**:
```c
static Table *isSimpleCount(Select *p, AggInfo *pAggInfo){
  // Must pass ALL these checks:
  if( p->pWhere                          // ❌ No WHERE clause
   || p->pEList->nExpr!=1                // ❌ Only one expression
   || p->pSrc->nSrc!=1                   // ❌ Single table
   || p->pSrc->a[0].fg.isSubquery        // ❌ Not a subquery
   || pAggInfo->nFunc!=1                 // ❌ Exactly one aggregate
   || p->pHaving                         // ❌ No HAVING clause
  ){
    return 0;
  }

  // Must be count() function
  // Must NOT be DISTINCT or window function

  return pTab;  // ✅ Optimization approved!
}
```

---

### 2. Index Selection Optimization

**Choose Smallest Index** (`select.c:8880-8904`):

```c
// Search for the index with lowest scan cost
Index *pBest = 0;
Pgno iRoot = pTab->tnum;  // Default: use main table

// For WITHOUT ROWID tables, prefer primary key index
if( !HasRowid(pTab) )
  pBest = sqlite3PrimaryKeyIndex(pTab);

// Find smallest suitable index
for(pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext){
  if( pIdx->bUnordered==0              // Must be ordered
   && pIdx->szIdxRow<pTab->szTabRow    // Smaller than table
   && pIdx->pPartIdxWhere==0           // Not a partial index
   && (!pBest || pIdx->szIdxRow<pBest->szIdxRow)
  ){
    pBest = pIdx;  // This index is better!
  }
}
```

**Key Insight**: Indexes are "almost always spread across less pages than their corresponding tables" because index entries contain only key columns.

---

### 3. Specialized VDBE Opcode: OP_Count

**Implementation** (`vdbe.c:3795-3812`):

```c
/* Opcode: OP_Count P1 P2 P3 * *
** Synopsis: r[P2]=count()
**
** Store the number of entries (an integer value) in the table or index
** opened by cursor P1 in register P2.
*/
case OP_Count: {
  i64 nEntry;
  BtCursor *pCrsr = p->apCsr[pOp->p1]->uc.pCursor;

  if( pOp->p3 ){
    // Fast estimate
    nEntry = sqlite3BtreeRowCountEst(pCrsr);
  } else {
    // Exact count
    rc = sqlite3BtreeCount(db, pCrsr, &nEntry);
  }

  pOut->u.i = nEntry;  // Store result
}
```

**Generated VDBE bytecode**:
```c
sqlite3VdbeAddOp4Int(v, OP_OpenRead, iCsr, iRoot, iDb, 1);
sqlite3VdbeAddOp2(v, OP_Count, iCsr, AggInfoFuncReg(pAggInfo,0));
sqlite3VdbeAddOp1(v, OP_Close, iCsr);
```

Just **3 opcodes**: Open cursor → Count → Close!

---

### 4. Efficient B-Tree Counting Algorithm

**Page-Level Traversal** (`btree.c:10465-10528`):

```c
int sqlite3BtreeCount(sqlite3 *db, BtCursor *pCur, i64 *pnEntry){
  i64 nEntry = 0;

  rc = moveToRoot(pCur);
  if( rc==SQLITE_EMPTY ){
    *pnEntry = 0;
    return SQLITE_OK;
  }

  // Traverse B-tree pages (not individual rows!)
  while( rc==SQLITE_OK && !db->u1.isInterrupted ){
    MemPage *pPage = pCur->pPage;

    // Count cells on this page
    if( pPage->leaf || !pPage->intKey ){
      nEntry += pPage->nCell;  // ⚡ Add entire page's count
    }

    // Navigate to next page
    if( pPage->leaf ){
      // ... move to parent and next sibling ...
    } else {
      // ... descend to child ...
    }
  }

  *pnEntry = nEntry;
  return SQLITE_OK;
}
```

**Key Optimization**:
- Does NOT iterate individual rows
- Reads `nCell` (cell count) from each page's metadata
- Walks B-tree structure visiting each page **once**

---

### 5. Page Structure Exploitation

**MemPage Structure** (`btreeInt.h:273-304`):

```c
struct MemPage {
  u8 isInit;
  u8 intKey;      // True for table b-trees
  u8 leaf;        // True if leaf page

  u16 nCell;      // ⭐ Number of cells on this page!

  u8 *aData;      // Pointer to page data
  // ...
};
```

**How it works**:
1. SQLite B-tree pages have a **header** containing metadata
2. The header includes `nCell` - the count of entries on that page
3. Instead of decoding individual rows, SQLite:
   - Reads the page header
   - Extracts `nCell`
   - Accumulates: `nEntry += pPage->nCell`

**Performance Impact**:
- **No row deserialization** needed
- **No column parsing** needed
- **No aggregate accumulation** logic needed
- Just simple integer addition per page!

---

### 6. Comparison: Optimized vs General Path

#### Optimized COUNT(*) Path:
1. Detect pattern with `isSimpleCount()`
2. Choose smallest index
3. Open cursor (`OP_OpenRead`)
4. Execute `OP_Count` → traverse pages, sum nCell values
5. Close cursor (`OP_Close`)
6. Done!

#### General Aggregate Path:
1. Initialize aggregate accumulator registers
2. Create WHERE loop with `sqlite3WhereBegin()`
3. For each row:
   - Decode row data
   - Evaluate expressions
   - Update accumulator with `OP_AggStep`
4. Finalize with `OP_AggFinal`
5. `sqlite3WhereEnd()`

**Speed difference**: Optimized path is **orders of magnitude faster** because it avoids row-by-row iteration.

---

### COUNT(*) Recommendations

#### Immediate (High ROI, Low Effort):

1. **Implement fast path for `SELECT COUNT(*) FROM table`**
   - Detect pattern in query planner
   - Return `table.rows.len()` directly
   - Expected: 10-100x improvement for simple COUNT(*)
   - Related: Issue #814

2. **Consider caching row counts**
   - Update counter on INSERT/DELETE
   - Instant COUNT(*) queries
   - Trade-off: Memory vs query speed

#### Long-Term:

3. **Index-based counting**
   - When WHERE clause exists, use indexes
   - Count matching entries without loading rows

---

## UPDATE Performance

**Key Files**: `docs/reference/sqlite/src/update.c`, `vdbe.c`, `where.c`

### 1. UPDATE Execution Strategy

**Three-Tier Strategy** (`update.c:732-741`):

```c
// Strategy decision
flags = WHERE_ONEPASS_DESIRED;
if( !pParse->nested
 && !pTrigger
 && !hasFK
 && !chngKey
 && !bReplace
 && (pWhere==0 || !ExprHasProperty(pWhere, EP_Subquery))
){
  flags |= WHERE_ONEPASS_MULTIROW;
}
```

**Execution Modes**:
- **ONEPASS_SINGLE**: Single row update, direct modification
- **ONEPASS_MULTI**: Multiple rows, update during scan (no temp storage)
- **ONEPASS_OFF**: Two-pass required (collect rowids first, then update)

---

### 2. In-Place vs Delete-Insert Strategy

**OPFLAG_ISNOOP Optimization** (`update.c:1075-1090`):

```c
#ifdef SQLITE_ENABLE_PREUPDATE_HOOK
sqlite3VdbeAddOp3(v, OP_Delete, iDataCur,
    OPFLAG_ISUPDATE | ((hasFK>1 || chngKey) ? 0 : OPFLAG_ISNOOP),
    regNewRowid
);
#else
if( hasFK>1 || chngKey ){
  sqlite3VdbeAddOp2(v, OP_Delete, iDataCur, 0);
}
#endif
```

**Key Insight**: When rowid/primary key is NOT changing and there are no complex FK constraints, the `OP_Delete` is marked with `OPFLAG_ISNOOP` - it only fires hooks but doesn't actually delete. The subsequent insert then overwrites in-place at the B-tree level.

---

### 3. Selective Index Maintenance

**Index Update Detection** (`update.c:86-135`):

```c
static int indexColumnIsBeingUpdated(
  Index *pIdx,      /* The index to check */
  int iCol,         /* Which column of the index to check */
  int *aXRef,       /* aXRef[j]>=0 if column j is being updated */
  int chngRowid     /* true if the rowid is being updated */
){
  i16 iIdxCol = pIdx->aiColumn[iCol];
  if( iIdxCol>=0 ){
    return aXRef[iIdxCol]>=0;
  }
  return sqlite3ExprReferencesUpdatedColumn(...);
}
```

**Selective Index Processing** (`update.c:567-589`):
```c
for(nAllIdx=0, pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext, nAllIdx++){
  int reg;
  // Check if index is affected by update
  if( chngKey || hasFK>1 || pIdx==pPk
   || indexWhereClauseMightChange(pIdx,aXRef,chngRowid)
  ){
    reg = ++pParse->nMem;  // Allocate registers
    pParse->nMem += pIdx->nColumn;
  }else{
    reg = 0;
    for(i=0; i<pIdx->nKeyCol; i++){
      if( indexColumnIsBeingUpdated(pIdx, i, aXRef, chngRowid) ){
        reg = ++pParse->nMem;
        pParse->nMem += pIdx->nColumn;
        break;
      }
    }
  }
  if( reg==0 ) aToOpen[nAllIdx+1] = 0;  // Don't even open this index!
  aRegIdx[nAllIdx] = reg;
}
```

**Key Optimizations**:
1. If an index doesn't reference any updated columns → **don't open it**
2. If a partial index's WHERE clause isn't affected → **skip it**
3. Registers only allocated for indexes that need updating

---

### 4. ONEPASS Optimization Advantages

**ONEPASS_MULTI** (`update.c:1127-1129`):
```c
else if( eOnePass==ONEPASS_MULTI ){
  sqlite3VdbeResolveLabel(v, labelContinue);
  sqlite3WhereEnd(pWInfo);
}
```

**Benefits**:
- Multiple rows updated during scan
- No ephemeral table needed
- No intermediate storage
- Used when: no triggers, no FK, primary key not changing

---

### UPDATE Recommendations

#### Immediate (High ROI, Low Effort):

1. **Selective index updating**
   - Only update indexes that reference changed columns
   - Track which columns are modified
   - Expected: 2-5x for tables with many indexes
   - Related: Issue #815

2. **In-place update detection**
   - When primary key unchanged, update in Vec directly
   - Avoid remove + insert
   - Expected: 20-30% improvement

#### Medium-Term:

3. **ONEPASS optimization**
   - Update rows during scan when safe
   - Avoid collecting rowids in temporary storage
   - Expected: 30-50% improvement for multi-row updates

---

## DELETE Performance

**Key Files**: `docs/reference/sqlite/src/delete.c`, `btree.c`

### 1. DELETE Execution Strategy

**Two-Phase Approach** (`delete.c:518-664`):

**Phase 1: Collection** (lines 518-585):
```c
if( HasRowid(pTab) ){
  /* For rowid table: use RowSet */
  iRowSet = ++pParse->nMem;
  sqlite3VdbeAddOp2(v, OP_Null, 0, iRowSet);
}else{
  /* For WITHOUT ROWID: use ephemeral table */
  pPk = sqlite3PrimaryKeyIndex(pTab);
  nPk = pPk->nKeyCol;
  iPk = pParse->nMem+1;
  pParse->nMem += nPk;
  iEphCur = pParse->nTab++;
  addrEphOpen = sqlite3VdbeAddOp2(v, OP_OpenEphemeral, iEphCur, nPk);
}
```

**Phase 2: Deletion Loop** (lines 607-664):
- Iterate through collected keys
- Delete each row and its index entries
- Handle triggers and foreign key constraints

---

### 2. TRUNCATE Optimization

**Fast Path for "DELETE FROM table"** (`delete.c:471-493`):

```c
if( rcauth==SQLITE_OK
 && pWhere==0
 && !bComplex
 && !IsVirtual(pTab)
#ifdef SQLITE_ENABLE_PREUPDATE_HOOK
 && db->xPreUpdateCallback==0
#endif
){
  sqlite3TableLock(pParse, iDb, pTab->tnum, 1, pTab->zName);
  if( HasRowid(pTab) ){
    sqlite3VdbeAddOp4(v, OP_Clear, pTab->tnum, iDb, memCnt ? memCnt : -1,
                      pTab->zName, P4_STATIC);
  }
  for(pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext){
    sqlite3VdbeAddOp2(v, OP_Clear, pIdx->tnum, iDb);
  }
}
```

**Benefits**:
- **No WHERE evaluation** - skips entire scan
- **No rowid collection** - deletes all pages directly
- **Single operation per table/index** - uses `OP_Clear` instead of looping
- **O(1) complexity** instead of O(N)

---

### 3. ONEPASS Delete Optimization

**Three Execution Modes** (`delete.c:555-585`):

```c
if( eOnePass!=ONEPASS_OFF ){
  /* For ONEPASS, no need to store the rowid/primary-key. There is only
  ** one, so just keep it in its register(s) and fall through to the
  ** delete code.  */
  nKey = nPk;
  // ... delete immediately
}else{
  // Two-pass: collect keys, then delete
  if( pPk ){
    sqlite3VdbeAddOp4Int(v, OP_IdxInsert, iEphCur, iKey, iPk, nPk);
  }else{
    sqlite3VdbeAddOp2(v, OP_RowSetAdd, iRowSet, iKey);
  }
}
```

**Benefits of ONEPASS**:
- Rows deleted immediately as WHERE finds them
- Zero memory overhead for rowid collection
- Single pass through data

---

### 4. RowSet Data Structure

**Efficient Storage** (`rowset.c`):

SQLite uses a custom RowSet structure for collecting rowids:
- Binary tree forest for sorted rowid storage
- ~32 bytes per rowid
- More efficient than ephemeral table for large deletes

---

### 5. Skip Unnecessary B-tree Balancing

**Optimization** (`btree.c:9963-9968`):

```c
// Avoid unnecessary rebalancing
if( pCur->pPage->nFree*3<=(int)pCur->pBt->usableSize*2 ){
  /* Optimization: If the free space is less than 2/3rds of the page,
  ** then balance() will always be a no-op.  No need to invoke it. */
  rc = SQLITE_OK;
}else{
  rc = balance(pCur);
}
```

**Impact**: Avoids expensive B-tree rebalancing when not needed.

---

### DELETE Recommendations

#### Immediate (High ROI, Low Effort):

1. **Implement TRUNCATE optimization**
   - Fast path for `DELETE FROM table` (no WHERE)
   - Clear entire table without iteration
   - Expected: 100-1000x for full table deletes

2. **ONEPASS delete mode**
   - Delete rows during scan when safe
   - Avoid intermediate rowid collection
   - Expected: 2-3x improvement

#### Medium-Term:

3. **Efficient rowid collection**
   - Custom data structure instead of Vec
   - Sorted storage for better cache locality
   - Expected: 20-30% for large deletes

---

## Priority Optimizations

Based on the analysis above, here are the **highest impact optimizations** we should implement:

### P0 - Critical (Implement First)

1. **COUNT(*) Fast Path** (Issue #814)
   - **Impact**: 10-100x improvement
   - **Effort**: Low (few hours)
   - **Implementation**: Detect `SELECT COUNT(*) FROM table` pattern, return `rows.len()`
   - **File**: `crates/executor/src/select/executor/aggregation/detection.rs`

2. **TRUNCATE Optimization**
   - **Impact**: 100-1000x for full table deletes
   - **Effort**: Low
   - **Implementation**: Fast path for `DELETE FROM table` without WHERE
   - **File**: `crates/executor/src/delete/execution.rs`

3. **Merge Constraint Checking** (Issue #815)
   - **Impact**: 30-40% INSERT improvement
   - **Effort**: Medium (1-2 days)
   - **Implementation**: Single-pass validation in INSERT
   - **File**: `crates/executor/src/insert/constraints.rs`

### P1 - High Priority

4. **Bulk INSERT-SELECT Fast Path**
   - **Impact**: 10-50x for bulk transfers
   - **Effort**: Medium
   - **Implementation**: Detect schema-compatible transfers, skip validation

5. **Selective Index Updating** (Issue #815)
   - **Impact**: 2-5x for multi-index tables
   - **Effort**: Medium
   - **Implementation**: Only update indexes that reference changed columns

6. **Append Detection for Sequential Inserts**
   - **Impact**: 2-5x for sequential workloads
   - **Effort**: Medium
   - **Implementation**: Track monotonic primary keys, skip uniqueness checks

### P2 - Medium Priority

7. **ONEPASS Update/Delete Modes**
   - **Impact**: 2-3x for multi-row operations
   - **Effort**: High (requires execution engine changes)

8. **Validation Context Pooling**
   - **Impact**: 20-30% reduction in allocations
   - **Effort**: Medium

9. **Column Metadata Caching**
   - **Impact**: 10-15% improvement
   - **Effort**: Low-Medium

---

## References

### SQLite Source Files Analyzed

- **INSERT**: `docs/reference/sqlite/src/insert.c` (45,999 lines)
- **SELECT**: `docs/reference/sqlite/src/select.c`
- **UPDATE**: `docs/reference/sqlite/src/update.c`
- **DELETE**: `docs/reference/sqlite/src/delete.c`
- **VDBE**: `docs/reference/sqlite/src/vdbe.c`, `vdbeaux.c`
- **B-tree**: `docs/reference/sqlite/src/btree.c`, `btreeInt.h`
- **WHERE**: `docs/reference/sqlite/src/where.c`

### Related Issues

- Issue #814: COUNT(*) optimization
- Issue #815: UPDATE performance optimization
- Issue #789: Hash join implementation (completed)

### Additional Reading

- **SQLite Docs**: https://www.sqlite.org/docs.html
- **VDBE Opcodes**: https://www.sqlite.org/opcode.html
- **B-tree Implementation**: https://www.sqlite.org/fileformat2.html
- **Query Planner**: https://www.sqlite.org/queryplanner.html

---

**Document Created**: 2025-11-02
**Last Updated**: 2025-11-02
**Analysis by**: Claude (Sonnet 4.5)
