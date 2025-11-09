import { describe, it, expect, beforeEach } from 'vitest'
import { initDatabase } from '../db/wasm'
import { loadSampleDatabase, sqlLogicTestDatabase, loadSqlDump } from '../data/sample-databases'
import type { Database } from '../db/types'

describe('SQLLogicTest Database', () => {
  let db: Database

  beforeEach(async () => {
    db = await initDatabase()
  })

  it('should load database schema successfully', () => {
    expect(() => loadSampleDatabase(db, sqlLogicTestDatabase)).not.toThrow()
    const tables = db.list_tables()
    expect(tables).toContain('test_files')
    expect(tables).toContain('test_runs')
    expect(tables).toContain('test_results')
  })

  it('should execute conformance summary query', async () => {
    loadSampleDatabase(db, sqlLogicTestDatabase)
    // Load SQL dump data
    await loadSqlDump(db, '/data/sqllogictest_results.sql')

    // Category summary query
    const result = db.query(`
      SELECT
        category,
        COUNT(*) as total,
        SUM(CASE WHEN status='PASS' THEN 1 ELSE 0 END) as passed
      FROM test_files
      GROUP BY category
    `)
    expect(result.rows.length).toBeGreaterThan(0)
    expect(result.columns).toContain('category')
    expect(result.columns).toContain('total')
    expect(result.columns).toContain('passed')
  })

  it('should execute total pass rate query', async () => {
    loadSampleDatabase(db, sqlLogicTestDatabase)
    await loadSqlDump(db, '/data/sqllogictest_results.sql')

    // Total pass rate query
    const result = db.query(`
      SELECT
        SUM(CASE WHEN status='PASS' THEN 1 ELSE 0 END) as passed,
        COUNT(*) as total
      FROM test_files
    `)
    expect(result.rows.length).toBe(1)
    expect(result.columns).toContain('passed')
    expect(result.columns).toContain('total')
  })

  it('should execute failing tests query', async () => {
    loadSampleDatabase(db, sqlLogicTestDatabase)
    await loadSqlDump(db, '/data/sqllogictest_results.sql')

    // Find failing tests
    const result = db.query(`
      SELECT file_path, category
      FROM test_files
      WHERE status='FAIL'
      ORDER BY category, file_path
      LIMIT 10
    `)
    expect(result.columns).toContain('file_path')
    expect(result.columns).toContain('category')
  })

  it('should execute JOIN query for test progress', async () => {
    loadSampleDatabase(db, sqlLogicTestDatabase)
    await loadSqlDump(db, '/data/sqllogictest_results.sql')

    // Test file progress JOIN
    const result = db.query(`
      SELECT
        tf.file_path,
        tf.status as current_status,
        tr.status as last_run_status
      FROM test_files tf
      LEFT JOIN test_results tr ON tf.file_path = tr.file_path
      LIMIT 5
    `)
    expect(result.columns).toContain('file_path')
    expect(result.columns).toContain('current_status')
    expect(result.columns).toContain('last_run_status')
  })
})
