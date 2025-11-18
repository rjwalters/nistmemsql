# Test Results Database Backups

This directory contains timestamped backups of the SQLLogicTest results database.

The working database is stored at `~/.vibesql/test_results/sqllogictest_results.sql`
and is periodically backed up here for version control.

## Creating a Backup

Run the backup script:
```bash
./scripts/backup_test_results.sh
```

This will:
- Copy the current database to `test_results/sqllogictest_results-YYYYMMDD-HHMMSS.sql`
- Keep only the 5 most recent backups
- Delete older backups automatically

## Committing Backups

After creating a backup, commit it to git:
```bash
git add test_results/
git commit -m "Update test results database backup"
```
