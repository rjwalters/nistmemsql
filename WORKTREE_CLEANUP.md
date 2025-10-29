# Worktree Cleanup - Issue #320

## Summary
Cleaned up 14 orphaned git worktrees from merged issues, freeing **3.8GB** of disk space.

## What Was Done
Removed 14 worktrees for closed issues:
- issue-276, issue-281, issue-282, issue-283, issue-284, issue-285
- issue-291, issue-292, issue-294, issue-295, issue-296
- issue-300, issue-312, issue-318

All issues were verified as CLOSED before removal.

## Commands Executed
```bash
# Verified all issues were closed
for issue in 276 281 282 283 284 285 291 292 294 295 296 300 312 318; do
  gh issue view "$issue" --json state --jq .state
done

# Removed all worktrees
for issue in 276 281 282 283 284 285 291 292 294 295 296 300 312 318; do
  git worktree remove .loom/worktrees/issue-$issue --force
done

# Pruned orphaned references
git worktree prune
```

## Results
- **Before**: 16 worktrees, 3.8GB disk usage
- **After**: 2 worktrees (only active ones), 8.3MB disk usage
- **Space Freed**: ~3.8GB

## GitIgnore Status
Verified that `.loom/worktrees/` is already in `.gitignore` (line 22), so worktrees are not tracked in git.

## Notes on clean.sh Script
The `.loom/scripts/clean.sh` script has the logic to handle this cleanup automatically, but it didn't execute the worktree removal when run with `--force` flag. This may be a bug in the script that should be investigated separately. For now, manual cleanup was successful.

## Recommendation
Consider setting up a periodic cleanup job (weekly/monthly) or adding a post-merge hook to automatically clean up worktrees after PRs are merged.
