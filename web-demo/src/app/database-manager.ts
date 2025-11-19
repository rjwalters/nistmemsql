import type { Database } from '../db/types'
import { getSampleDatabase, loadSampleDatabase, loadSqlDump } from '../data/sample-databases'

/**
 * Manages database loading and table metadata
 */
export class DatabaseManager {
  private tableNames: string[] = []
  private currentDatabaseId = 'employees'

  constructor(private database: Database | null) {}

  /**
   * Load a sample database by ID
   */
  async loadDatabase(dbId: string): Promise<void> {
    if (!this.database) return

    const sampleDb = getSampleDatabase(dbId)
    if (!sampleDb) {
      console.error(`Unknown database: ${dbId}`)
      return
    }

    try {
      // Load tables and create schema
      loadSampleDatabase(this.database, sampleDb)

      // For SQLLogicTest database, load data from SQL dump file
      if (dbId === 'sqllogictest') {
        await loadSqlDump(this.database, '/data/sqllogictest_results.sql')
      }

      this.tableNames = this.database.list_tables()
      this.currentDatabaseId = dbId
    } catch (error) {
      console.warn(`Failed to load sample database ${dbId}:`, error)
    }
  }

  /**
   * Refresh table metadata from database
   */
  refreshTables(): void {
    if (!this.database) return
    try {
      this.tableNames = this.database.list_tables()
    } catch (error) {
      console.warn('Failed to refresh table metadata', error)
    }
  }

  /**
   * Get current table names
   */
  getTableNames(): string[] {
    return this.tableNames
  }

  /**
   * Get current database ID
   */
  getCurrentDatabaseId(): string {
    return this.currentDatabaseId
  }

  /**
   * Get the database instance
   */
  getDatabase(): Database | null {
    return this.database
  }
}
