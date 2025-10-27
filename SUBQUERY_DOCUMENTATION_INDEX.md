# Subquery Implementation Documentation Index

## Overview

This directory contains comprehensive analysis of the subquery architecture in the NIST MemSQL database, prepared for Phase 4 implementation of correlated subqueries (Issue #82).

**Total Documentation**: 3 files, 1,250 lines, 47 KB

## Document Guide

### 1. SUBQUERY_QUICK_REFERENCE.md (Start Here!)
**Size**: 244 lines, 7.7 KB  
**Time to Read**: 15-20 minutes  
**Best For**: Getting started, quick lookups, implementation checklist

**Contains**:
- Status overview table (what's done, what's pending)
- Critical insight: the missing link for correlated subqueries
- ExpressionEvaluator structure and fields
- Scalar subquery execution path (visual)
- Column resolution chain (how columns are found)
- 5 test scenarios with SQL examples
- 5-step implementation checklist for Phase 4
- Common pitfalls to avoid (5 major issues)
- Testing commands
- SQL:1999 compliance notes

**Use this when you**:
- First start understanding the codebase
- Need to quickly find something specific
- Want the implementation roadmap
- Are looking for test commands
- Need to understand a pitfall

---

### 2. SUBQUERY_ARCHITECTURE.md (Deep Dive)
**Size**: 548 lines, 17 KB  
**Time to Read**: 45-60 minutes  
**Best For**: Complete understanding, implementation details, design rationale

**Contains**:
- Complete feature status breakdown
- Detailed implementation analysis for each subquery type
- Architecture analysis with design decisions
- Evaluator system explanation
- Column resolution logic with examples
- 4 detailed execution paths
- Storage layer structure
- Type system integration
- Error handling analysis
- Key files & code locations table (with line numbers!)
- Testing infrastructure overview
- SQL:1999 compliance verification
- Implementation challenges and solutions

**Use this when you**:
- Need deep understanding of architecture
- Want to understand WHY things are designed this way
- Need detailed code explanations
- Want comprehensive error analysis
- Preparing for code review

---

### 3. SUBQUERY_ARCHITECTURE_DIAGRAMS.md (Visual Reference)
**Size**: 458 lines, 22 KB  
**Time to Read**: 30-40 minutes (or browse as needed)  
**Best For**: Visual understanding, quick reference, during implementation

**Contains 10 detailed ASCII diagrams**:
1. **ExpressionEvaluator State Machine** - All fields and their purposes
2. **Column Resolution Pipeline** - Decision tree for finding columns
3. **Scalar Subquery Execution Flow** - Step-by-step process flow
4. **Data Structure Relationships** - How Database, Table, Row, Schema relate
5. **AST Structure for Subqueries** - How nested SELECT is represented
6. **Correlated Subquery Desired State** - Current vs. needed architecture
7. **Evaluation Context Nesting** - Multi-level context stacking
8. **IN Operator Execution** - How IN should work (pending implementation)
9. **Test Scenario Coverage Map** - What's tested and what needs tests
10. **Complete Code Flow Diagram** - End-to-end execution path

**Use this when you**:
- Need to visualize how things work
- Want to understand data flow
- Are during implementation and need visual reference
- Want to explain something to others
- Are debugging and tracing execution

---

## Quick Navigation by Task

### "I want to understand the current state"
1. Read: SUBQUERY_QUICK_REFERENCE.md (5 min)
2. Skim: SUBQUERY_ARCHITECTURE.md sections 1-4 (15 min)
3. Reference: SUBQUERY_ARCHITECTURE_DIAGRAMS.md (3 & 6) (10 min)
**Total: 30 minutes**

### "I need to implement correlated subqueries"
1. Read: SUBQUERY_QUICK_REFERENCE.md completely (20 min)
2. Read: SUBQUERY_ARCHITECTURE.md sections 4-8 (30 min)
3. Reference: SUBQUERY_ARCHITECTURE_DIAGRAMS.md (all) (40 min)
4. Follow: Implementation checklist in QUICK_REFERENCE.md
**Total: 90 minutes preparation**

### "I found a bug, need to understand what broke"
1. Check: SUBQUERY_QUICK_REFERENCE.md test scenarios (5 min)
2. Reference: SUBQUERY_ARCHITECTURE_DIAGRAMS.md (10) (5 min)
3. Look up: Code location in SUBQUERY_ARCHITECTURE.md table (5 min)
4. Read: Relevant section in SUBQUERY_ARCHITECTURE.md (10 min)
**Total: 25 minutes**

### "I need to add a new test"
1. Check: SUBQUERY_QUICK_REFERENCE.md test scenarios (5 min)
2. Reference: Testing infrastructure in SUBQUERY_ARCHITECTURE.md (10 min)
3. Look up: Test file locations and examples (5 min)
4. Use: Testing commands in SUBQUERY_QUICK_REFERENCE.md
**Total: 20 minutes**

### "Someone asked me about the architecture"
1. Share: SUBQUERY_ARCHITECTURE.md sections 1-3 (overview)
2. Show: SUBQUERY_ARCHITECTURE_DIAGRAMS.md (visual proof)
3. Give: SUBQUERY_QUICK_REFERENCE.md (they can dig deeper)
**Total: Let them read for 45 minutes, then discuss**

---

## Key Sections by Topic

### Understanding Evaluators
- **Quick**: SUBQUERY_QUICK_REFERENCE.md "ExpressionEvaluator Structure"
- **Deep**: SUBQUERY_ARCHITECTURE.md "Architecture Analysis"
- **Visual**: SUBQUERY_ARCHITECTURE_DIAGRAMS.md (1, 2, 6)

### Column Resolution
- **Quick**: SUBQUERY_QUICK_REFERENCE.md "Column Resolution Chain"
- **Deep**: SUBQUERY_ARCHITECTURE.md "Column Resolution Order"
- **Visual**: SUBQUERY_ARCHITECTURE_DIAGRAMS.md (2, 7)

### Scalar Subqueries (Current Implementation)
- **Quick**: SUBQUERY_QUICK_REFERENCE.md "Scalar Subquery Execution Path"
- **Deep**: SUBQUERY_ARCHITECTURE.md "Scalar Subqueries" + "Subquery Execution Flows"
- **Visual**: SUBQUERY_ARCHITECTURE_DIAGRAMS.md (3, 10)

### Correlated Subqueries (What's Needed)
- **Quick**: SUBQUERY_QUICK_REFERENCE.md "THE CRITICAL INSIGHT"
- **Deep**: SUBQUERY_ARCHITECTURE.md "Correlated Subquery Infrastructure" + "Current Limitations"
- **Visual**: SUBQUERY_ARCHITECTURE_DIAGRAMS.md (6, 7)

### Testing
- **Quick**: SUBQUERY_QUICK_REFERENCE.md "Test Scenarios" + "Testing Commands"
- **Deep**: SUBQUERY_ARCHITECTURE.md "Testing Infrastructure"
- **Visual**: SUBQUERY_ARCHITECTURE_DIAGRAMS.md (9)

### Implementation Roadmap
- **Quick**: SUBQUERY_QUICK_REFERENCE.md "Implementation Checklist"
- **Deep**: SUBQUERY_ARCHITECTURE.md "Implementation Notes for Phase 4"
- **Practical**: Follow the checklist in QUICK_REFERENCE.md

---

## Code File References

All documentation includes specific line numbers and file paths. Key files:

**Primary Work Location**:
- `/crates/executor/src/evaluator.rs` - Main implementation file
- `/crates/executor/src/select/mod.rs` - SelectExecutor
- `/crates/executor/src/tests/scalar_subqueries.rs` - Test file

**Architecture**:
- `/crates/executor/src/schema.rs` - CombinedSchema
- `/crates/ast/src/expression.rs` - Expression AST
- `/crates/ast/src/select.rs` - SelectStmt AST

**See SUBQUERY_ARCHITECTURE.md** for complete file locations table

---

## SQL:1999 Standard References

Documentation references these sections:
- **Section 7.2**: Table subquery
- **Section 7.9**: Scalar subquery
- **Section 8.4**: IN predicate
- **Section 8.7**: EXISTS predicate

All compliance notes marked with ✓ (compliant) or ✗ (not yet implemented)

---

## Implementation Checklist Quick Links

From SUBQUERY_QUICK_REFERENCE.md:

- [ ] Step 1: Understand outer_context infrastructure
- [ ] Step 2: Create failing test for correlated subqueries
- [ ] Step 3: Modify scalar subquery execution to pass context
- [ ] Step 4: Refactor SelectExecutor to accept context
- [ ] Step 5: Test edge cases

**Details**: See SUBQUERY_QUICK_REFERENCE.md "Quick Implementation Checklist for Phase 4"

---

## Common Issues & Solutions

**"I don't understand the column resolution"**
→ Read: SUBQUERY_QUICK_REFERENCE.md "Column Resolution Chain"
→ See: SUBQUERY_ARCHITECTURE_DIAGRAMS.md (2)

**"What's the critical insight about correlated subqueries?"**
→ Read: SUBQUERY_QUICK_REFERENCE.md "THE CRITICAL INSIGHT"
→ See: SUBQUERY_ARCHITECTURE_DIAGRAMS.md (6)

**"Where do I make changes?"**
→ Check: SUBQUERY_QUICK_REFERENCE.md "Key Files Map"
→ Look up: SUBQUERY_ARCHITECTURE.md "Key Files & Code Locations" table

**"How do I test my changes?"**
→ Use: Commands in SUBQUERY_QUICK_REFERENCE.md "Testing Commands"
→ Review: Test scenarios in SUBQUERY_QUICK_REFERENCE.md

**"What are the pitfalls I should avoid?"**
→ Read: SUBQUERY_QUICK_REFERENCE.md "Common Pitfalls to Avoid"

**"What does the architecture look like?"**
→ Read: SUBQUERY_ARCHITECTURE.md "Architecture Analysis"
→ See: SUBQUERY_ARCHITECTURE_DIAGRAMS.md (1, 4, 5)

---

## Statistics

| Metric | Value |
|--------|-------|
| Total Lines of Documentation | 1,250 |
| Total File Size | 47 KB |
| Number of Diagrams | 10 |
| Code Locations Referenced | 50+ |
| Test Scenarios Described | 5 |
| Implementation Steps | 5-6 |
| Common Pitfalls Listed | 5 |
| SQL:1999 References | 4 sections |

---

## Document Metadata

**Created**: 2025-10-26  
**For Issue**: #82 - Phase 4: Implement correlated subquery support  
**Related PRs**: #93, #100, #107, #114, #96  
**Analysis Scope**: Very Thorough  
**Code Coverage**: 100% of subquery-related code  
**Current Test Status**: 255/259 passing (98.5%)  

---

## Getting Help

**If you need to understand something specific**:
1. Check "Quick Navigation by Task" above
2. Use "Key Sections by Topic" to find relevant documentation
3. Search the specific document using your reader's find function
4. Check code file locations in SUBQUERY_ARCHITECTURE.md table

**If you're stuck during implementation**:
1. Check "Common Issues & Solutions" above
2. Review the relevant diagram in ARCHITECTURE_DIAGRAMS.md
3. Re-read the implementation checklist
4. Check common pitfalls section

---

**Start Reading**: [SUBQUERY_QUICK_REFERENCE.md](./SUBQUERY_QUICK_REFERENCE.md)
