#!/bin/bash
# Backup test results database to the repository
# Keeps only the 5 most recent backups

set -euo pipefail

# Get script directory and repo root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Paths
SOURCE_DB="$HOME/.vibesql/test_results/sqllogictest_results.sql"
BACKUP_DIR="$REPO_ROOT/test_results"
TIMESTAMP=$(date +"%Y%m%d-%H%M%S")
BACKUP_FILE="$BACKUP_DIR/sqllogictest_results-$TIMESTAMP.sql"

# Create backup directory if it doesn't exist
mkdir -p "$BACKUP_DIR"

# Check if source database exists
if [ ! -f "$SOURCE_DB" ]; then
    echo "Error: Source database not found at $SOURCE_DB"
    exit 1
fi

# Copy database to backup location
echo "Backing up test results database..."
cp "$SOURCE_DB" "$BACKUP_FILE"
echo "✓ Backup created: $BACKUP_FILE"

# Get file size for display
SIZE=$(du -h "$BACKUP_FILE" | cut -f1)
echo "  Size: $SIZE"

# Keep only the 5 most recent backups
echo ""
echo "Cleaning up old backups (keeping 5 most recent)..."
cd "$BACKUP_DIR"

# List all backup files, sort by modification time (newest first), skip first 5, delete rest
ls -t sqllogictest_results-*.sql 2>/dev/null | tail -n +6 | while read -r old_backup; do
    echo "  Removing old backup: $old_backup"
    rm -f "$old_backup"
done

# Show remaining backups
echo ""
echo "Current backups in repository:"
ls -lh sqllogictest_results-*.sql 2>/dev/null | awk '{print "  " $9 " (" $5 ")"}' || echo "  (no backups found)"

echo ""
echo "✓ Backup complete"
echo ""
echo "To commit this backup to git:"
echo "  git add test_results/"
echo "  git commit -m \"Update test results database backup\""
