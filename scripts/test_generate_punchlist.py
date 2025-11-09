#!/usr/bin/env python3
"""
Test suite for generate_punchlist.py with VibeSQL integration.

Tests the core functionality of storing SQLLogicTest results in VibeSQL
and exporting SQL dumps.
"""

import unittest
import tempfile
import os
from pathlib import Path


class TestVibesqlPunchlist(unittest.TestCase):
    """Test VibeSQL integration for punchlist generation."""

    def setUp(self):
        """Set up test fixtures."""
        try:
            import vibesql
            self.vibesql = vibesql
        except ImportError:
            self.skipTest("vibesql Python bindings not available")

        # Create temporary directory for test files
        self.test_dir = tempfile.mkdtemp()
        self.schema_file = Path(__file__).parent / "schema" / "test_results.sql"

    def tearDown(self):
        """Clean up test files."""
        import shutil
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_create_database_from_schema(self):
        """Test that we can create a database and load the schema."""
        db = self.vibesql.connect()
        cursor = db.cursor()

        # Load schema
        with open(self.schema_file, 'r') as f:
            schema_sql = f.read()

        # Execute each CREATE TABLE statement
        for statement in schema_sql.split(';'):
            statement = statement.strip()
            if statement and statement.upper().startswith('CREATE TABLE'):
                cursor.execute(statement)

        # Verify tables exist by inserting test data
        cursor.execute("""
            INSERT INTO test_files (file_path, category, status)
            VALUES ('test.sql', 'index', 'PASS')
        """)

        cursor.execute("SELECT COUNT(*) FROM test_files")
        count = cursor.fetchone()[0]
        self.assertEqual(count, 1)

        db.close()

    def test_insert_test_results(self):
        """Test inserting test file records."""
        db = self.vibesql.connect()
        cursor = db.cursor()

        # Create schema
        cursor.execute("""
            CREATE TABLE test_files (
                file_path VARCHAR(500) PRIMARY KEY,
                category VARCHAR(50) NOT NULL,
                subcategory VARCHAR(50),
                status VARCHAR(20) NOT NULL,
                last_tested TIMESTAMP,
                last_passed TIMESTAMP
            )
        """)

        # Insert test data
        test_files = [
            ('index/between/1/slt_good_0.test', 'index', 'between', 'PASS'),
            ('index/delete/10/slt_good_0.test', 'index', 'delete', 'FAIL'),
            ('random/select/1000/slt_good_0.test', 'random', 'select', 'UNTESTED'),
        ]

        for file_path, category, subcategory, status in test_files:
            cursor.execute("""
                INSERT INTO test_files (file_path, category, subcategory, status)
                VALUES (?, ?, ?, ?)
            """, (file_path, category, subcategory, status))

        # Verify data
        cursor.execute("SELECT COUNT(*) FROM test_files")
        count = cursor.fetchone()[0]
        self.assertEqual(count, 3)

        cursor.execute("SELECT COUNT(*) FROM test_files WHERE status='PASS'")
        passed = cursor.fetchone()[0]
        self.assertEqual(passed, 1)

        db.close()

    def test_export_sql_dump(self):
        """Test exporting database as SQL dump."""
        db = self.vibesql.connect()
        cursor = db.cursor()

        # Create simple table
        cursor.execute("""
            CREATE TABLE test_files (
                file_path VARCHAR(500) PRIMARY KEY,
                category VARCHAR(50) NOT NULL,
                status VARCHAR(20) NOT NULL
            )
        """)

        # Insert data
        cursor.execute("""
            INSERT INTO test_files (file_path, category, status)
            VALUES ('test.sql', 'index', 'PASS')
        """)

        # Export SQL dump
        dump_file = os.path.join(self.test_dir, 'test_dump.sql')
        db.save_sql_dump(dump_file)

        # Verify file exists
        self.assertTrue(os.path.exists(dump_file))

        # Verify file contains expected content
        with open(dump_file, 'r') as f:
            content = f.read()

        self.assertIn('CREATE TABLE', content)
        self.assertIn('INSERT INTO', content)
        self.assertIn('test.sql', content)
        self.assertIn('PASS', content)

        db.close()

    def test_load_existing_dump(self):
        """Test loading database from SQL dump."""
        # Create and export database
        db1 = self.vibesql.connect()
        cursor1 = db1.cursor()

        cursor1.execute("""
            CREATE TABLE test_files (
                file_path VARCHAR(500) PRIMARY KEY,
                status VARCHAR(20) NOT NULL
            )
        """)
        cursor1.execute("""
            INSERT INTO test_files (file_path, status) VALUES ('test1.sql', 'PASS')
        """)
        cursor1.execute("""
            INSERT INTO test_files (file_path, status) VALUES ('test2.sql', 'FAIL')
        """)

        dump_file = os.path.join(self.test_dir, 'test_dump.sql')
        db1.save_sql_dump(dump_file)
        db1.close()

        # Load into new database
        db2 = self.vibesql.connect()
        cursor2 = db2.cursor()

        with open(dump_file, 'r') as f:
            for statement in f.read().split(';'):
                statement = statement.strip()
                if statement and not statement.startswith('--'):
                    try:
                        cursor2.execute(statement)
                    except Exception as e:
                        # Skip errors (e.g., empty statements, comments)
                        pass

        # Verify data loaded
        cursor2.execute("SELECT COUNT(*) FROM test_files")
        count = cursor2.fetchone()[0]
        self.assertEqual(count, 2)

        cursor2.execute("SELECT file_path FROM test_files ORDER BY file_path")
        rows = cursor2.fetchall()
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0][0], 'test1.sql')
        self.assertEqual(rows[1][0], 'test2.sql')

        db2.close()

    def test_summary_queries_match_old_format(self):
        """Test that SQL queries produce same stats as old JSON format."""
        db = self.vibesql.connect()
        cursor = db.cursor()

        # Create schema
        cursor.execute("""
            CREATE TABLE test_files (
                file_path VARCHAR(500) PRIMARY KEY,
                category VARCHAR(50) NOT NULL,
                status VARCHAR(20) NOT NULL
            )
        """)

        # Insert test data matching known distribution
        test_data = [
            ('index', 'PASS', 75),
            ('index', 'FAIL', 132),
            ('index', 'UNTESTED', 7),
            ('evidence', 'PASS', 6),
            ('evidence', 'FAIL', 6),
            ('random', 'PASS', 2),
            ('random', 'FAIL', 386),
            ('random', 'UNTESTED', 3),
        ]

        file_id = 1
        for category, status, count in test_data:
            for _ in range(count):
                cursor.execute("""
                    INSERT INTO test_files (file_path, category, status)
                    VALUES (?, ?, ?)
                """, (f'{category}/test_{file_id}.sql', category, status))
                file_id += 1

        # Query overall stats
        cursor.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN status='PASS' THEN 1 ELSE 0 END) as passed,
                SUM(CASE WHEN status='FAIL' THEN 1 ELSE 0 END) as failed,
                SUM(CASE WHEN status='UNTESTED' THEN 1 ELSE 0 END) as untested
            FROM test_files
        """)

        row = cursor.fetchone()
        total, passed, failed, untested = row

        # Expected from old JSON format
        self.assertEqual(total, 617)  # Sum of all counts
        self.assertEqual(passed, 83)
        self.assertEqual(failed, 524)
        self.assertEqual(untested, 10)

        db.close()


if __name__ == '__main__':
    unittest.main()
