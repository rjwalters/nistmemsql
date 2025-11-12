# Trigger Implementation Status

## Summary

This document tracks the implementation status of SQL trigger support in VibeSQL as per issue #1373.

## Current Status (2025-01-12)

### ✅ COMPLETED - Phases 1 & 2

The following infrastructure is **already fully implemented**:

#### Phase 1: Storage & Catalog
- ✅ `TriggerDefinition` exists in `crates/vibesql-catalog/src/trigger.rs`
- ✅ Catalog has trigger storage: `crates/vibesql-catalog/src/store/mod.rs:48`
- ✅ Catalog trigger operations: `crates/vibesql-catalog/src/store/advanced/triggers.rs`
  - `create_trigger()`
  - `get_trigger()`
  - `drop_trigger()`
  - `get_triggers_for_table()` - finds triggers by table and event
- ✅ Catalog errors: `TriggerAlreadyExists`, `TriggerNotFound`

#### Phase 2: DDL Executor
- ✅ CREATE TRIGGER executor: `crates/vibesql-executor/src/advanced_objects.rs:347-366`
- ✅ DROP TRIGGER executor: `crates/vibesql-executor/src/advanced_objects.rs:368-375`
- ✅ Tests passing: `crates/vibesql-executor/src/tests/trigger_tests.rs`
  - `test_create_trigger` ✅
  - `test_create_trigger_duplicate_error` ✅
  - `test_drop_trigger` ✅
  - `test_drop_trigger_not_found` ✅
  - `test_create_trigger_all_variations` ✅

### ❌ NOT IMPLEMENTED - Phases 3+

The following functionality still needs to be implemented:

#### Phase 3: Trigger Firing (MAIN WORK NEEDED)
- ❌ Trigger firing mechanism
- ❌ Hook points in INSERT executor (`crates/vibesql-executor/src/insert/execution.rs`)
- ❌ Hook points in UPDATE executor
- ❌ Hook points in DELETE executor
- ❌ BEFORE trigger support
- ❌ AFTER trigger support
- ❌ FOR EACH ROW execution
- ❌ WHEN condition evaluation

#### Phase 4: OLD/NEW Context
- ❌ OLD pseudo-record support (for UPDATE/DELETE)
- ❌ NEW pseudo-record support (for INSERT/UPDATE)
- ❌ Expression evaluator modifications
- ❌ Trigger action parsing (currently stored as RawSql)

#### Phase 5: Persistence
- ❌ Trigger serialization in `crates/vibesql-storage/src/persistence/binary/catalog.rs`
- ❌ Trigger deserialization

#### Phase 6: Advanced Features (Future)
- ❌ FOR EACH STATEMENT triggers
- ❌ INSTEAD OF triggers (for views)
- ❌ UPDATE OF specific columns
- ❌ Trigger ordering (multiple triggers on same event)
- ❌ Recursion handling

## Implementation Plan

###  Priority 1: Trigger Firing (Phase 3)

This is the core functionality that makes triggers actually work.

**Implementation steps:**

1. Create `crates/vibesql-executor/src/trigger_execution.rs`
2. Add `find_and_fire_triggers()` helper function
3. Add hook points in INSERT executor:
   - Before insertion: fire BEFORE INSERT triggers
   - After insertion: fire AFTER INSERT triggers
4. Add hook points in UPDATE executor:
   - Before update: fire BEFORE UPDATE triggers
   - After update: fire AFTER UPDATE triggers
5. Add hook points in DELETE executor:
   - Before deletion: fire BEFORE DELETE triggers
   - After deletion: fire AFTER DELETE triggers

**Test cases needed:**
```sql
-- Basic AFTER INSERT trigger
CREATE TABLE audit (msg VARCHAR(100));
CREATE TRIGGER log_insert AFTER INSERT ON audit FOR EACH ROW
BEGIN
  INSERT INTO audit VALUES ('triggered');
END;

INSERT INTO audit VALUES ('test');
SELECT * FROM audit;
-- Expected: 'test' and 'triggered'
```

### Priority 2: OLD/NEW Context (Phase 4)

Required for triggers to access row data.

**Implementation challenges:**
- Parse trigger action SQL into statements
- Create execution context with OLD/NEW virtual tables
- Modify expression evaluator to resolve OLD.col and NEW.col

### Priority 3: Persistence (Phase 5)

Required for triggers to survive database restarts.

**Implementation:** Follow pattern from indexes in `catalog.rs`:
- Write trigger count
- For each trigger: serialize all fields
- Read triggers back on database load

## Testing Strategy

1. **Unit tests** (executor level) - PARTIALLY DONE
   - ✅ CREATE/DROP TRIGGER tests pass
   - ❌ Need trigger firing tests

2. **Integration tests** (end-to-end)
   - ❌ Need tests with actual INSERT/UPDATE/DELETE
   - ❌ Need tests with trigger actions modifying data

3. **Edge cases**
   - ❌ Multiple triggers on same table
   - ❌ Trigger recursion prevention
   - ❌ Trigger failures and rollback

## References

- Issue: #1373
- Parser: `crates/vibesql-parser/src/parser/trigger.rs`
- AST: `crates/vibesql-ast/src/ddl/schema.rs:100-144`
- Catalog: `crates/vibesql-catalog/src/store/advanced/triggers.rs`
- Executor: `crates/vibesql-executor/src/advanced_objects.rs:347-375`
- Tests: `crates/vibesql-executor/src/tests/trigger_tests.rs`
