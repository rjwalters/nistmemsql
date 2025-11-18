# docs/archive/

**Purpose**: Historical documentation preserved for reference

This directory contains only high-value historical documents that provide insight into the project's development journey. Temporary planning documents, issue-specific investigations, and redundant documentation have been removed.

---

## Contents

### 1. ROADMAP_CORE_COMPLIANCE.md (26K)
**Achievement**: SQL:1999 Core Compliance (Oct 25 - Nov 1, 2025)

Historical record of achieving 100% Core SQL:1999 conformance (739/739 tests) in 7 days. Documents the strategic approach, implementation milestones, and key achievements.

**Why preserved**: Major project milestone, referenced in main ROADMAP.md

---

### 2. SQLLogicTest Punchlist Documents (3 files, 22K)

**PUNCHLIST_100_CONFORMANCE_ARCHIVED.md**
- Tracks conformance journey from 13.3% → 100%
- Documents systematic approach to fixing failures
- Shows methodology for achieving 100% conformance

**PUNCHLIST_README_ARCHIVED.md**
- Explains punchlist system used for tracking
- Documents scripts and workflow

**PUNCHLIST_MANIFEST_ARCHIVED.md**
- Complete inventory of all 623 test files
- Status snapshots at different stages

**Achievement**: 100% SQLLogicTest conformance (623/623 files, ~5.9M tests) achieved November 18, 2025 via PR #2048

**Why preserved**: Recent achievement, referenced in main ROADMAP.md, shows methodology for systematic test coverage

---

### 3. MAJOR_SIMPLIFICATIONS.md (14K)
**Date**: October-November 2025

Documents major architectural simplifications made during development:
- Code structure improvements
- Refactoring decisions
- Complexity reduction strategies

**Why preserved**: Valuable for understanding architectural evolution and design decisions

---

## What Was Removed

**Deleted ~85% of archive** (27 files, 2 subdirectories, ~385K):
- Phase planning documents (Phase 2, 3, 4 planning docs)
- Subquery architecture docs (redundant with code)
- SQL1999 research documents (research completed, compliance achieved)
- Issue-specific investigation files (old resolved issues)
- Temporary work plans and implementation summaries
- Archived investigations subdirectory (14 old investigation files)
- Proposals subdirectory (1 file, no longer relevant)

**Rationale**: These documents served their purpose during development but have no future reference value. The knowledge lives in the codebase, tests, and active documentation.

---

## Active Documentation

For current project documentation, see:
- [`docs/ROADMAP.md`](../ROADMAP.md) - Master roadmap and current status
- [`docs/roadmaps/`](../roadmaps/) - Active roadmap documents
- [`docs/investigations/`](../investigations/) - Recent investigation reports
- [`README.md`](../../README.md) - Project overview and quick start

---

**Last Cleanup**: 2025-11-18
**Files Retained**: 5 high-value historical documents
**Storage**: ~65K (down from ~450K, 85% reduction)
