#!/usr/bin/env python3
"""
Unit tests for process_test_results.py, specifically the sql_escape() function.

Tests ensure proper escaping of SQL strings to prevent parsing errors.
"""

import unittest
import sys
from pathlib import Path

# Add scripts directory to path for importing
sys.path.insert(0, str(Path(__file__).parent))

from process_test_results import sql_escape


class TestSqlEscape(unittest.TestCase):
    """Test sql_escape() function for proper SQL string escaping."""

    def test_none_value(self):
        """Test that None returns NULL keyword."""
        result = sql_escape(None)
        self.assertEqual(result, "NULL")

    def test_empty_string(self):
        """Test that empty string is properly quoted."""
        result = sql_escape("")
        self.assertEqual(result, "''")

    def test_simple_string(self):
        """Test basic string without special characters."""
        result = sql_escape("hello world")
        self.assertEqual(result, "'hello world'")

    def test_single_quotes(self):
        """Test that single quotes are properly escaped by doubling."""
        result = sql_escape("SELECT 'value'")
        self.assertEqual(result, "'SELECT ''value'''")

    def test_backslashes(self):
        """Test that backslashes are properly escaped."""
        result = sql_escape("C:\\path\\to\\file")
        self.assertEqual(result, "'C:\\\\path\\\\to\\\\file'")

    def test_trailing_backslash(self):
        """Test string ending with backslash."""
        result = sql_escape("value\\")
        self.assertEqual(result, "'value\\\\'")

    def test_mixed_quotes_and_backslashes(self):
        """Test combination of single quotes and backslashes."""
        result = sql_escape("it's \"quoted\" with\\path")
        self.assertEqual(result, "'it''s \"quoted\" with\\\\path'")

    def test_newlines(self):
        """Test that newlines are replaced with spaces."""
        result = sql_escape("line1\nline2")
        self.assertEqual(result, "'line1 line2'")

    def test_carriage_returns(self):
        """Test that carriage returns are replaced with spaces."""
        result = sql_escape("line1\rline2")
        self.assertEqual(result, "'line1 line2'")

    def test_tabs(self):
        """Test that tabs are replaced with spaces."""
        result = sql_escape("col1\tcol2")
        self.assertEqual(result, "'col1 col2'")

    def test_multiple_control_characters(self):
        """Test multiple control characters together."""
        result = sql_escape("line1\n\r\tline2")
        self.assertEqual(result, "'line1 line2'")

    def test_null_bytes(self):
        """Test that null bytes are removed."""
        result = sql_escape("before\x00after")
        self.assertEqual(result, "'beforeafter'")

    def test_multiple_spaces_collapsed(self):
        """Test that multiple spaces are collapsed to single space."""
        result = sql_escape("too    many     spaces")
        self.assertEqual(result, "'too many spaces'")

    def test_leading_trailing_spaces(self):
        """Test that leading/trailing spaces are preserved after collapse."""
        result = sql_escape("  spaced  ")
        # After split() and join(), leading/trailing spaces are removed
        self.assertEqual(result, "'spaced'")

    def test_complex_sql_query(self):
        """Test escaping a complex SQL query string."""
        sql_query = "SELECT * FROM table WHERE col='value' AND path='C:\\data'"
        result = sql_escape(sql_query)
        expected = "'SELECT * FROM table WHERE col=''value'' AND path=''C:\\\\data'''"
        self.assertEqual(result, expected)

    def test_error_message_with_sql(self):
        """Test realistic error message containing SQL."""
        error_msg = "Parse error: Unterminated string literal at 'SELECT * FROM'"
        result = sql_escape(error_msg)
        expected = "'Parse error: Unterminated string literal at ''SELECT * FROM'''"
        self.assertEqual(result, expected)

    def test_non_string_type_conversion(self):
        """Test that non-string types are converted to strings."""
        result = sql_escape(42)
        self.assertEqual(result, "'42'")

    def test_unicode_characters(self):
        """Test that unicode characters are preserved."""
        result = sql_escape("Hello 世界 🌍")
        self.assertEqual(result, "'Hello 世界 🌍'")

    def test_sql_injection_attempt(self):
        """Test that SQL injection attempts are safely escaped."""
        injection = "'; DROP TABLE test_files; --"
        result = sql_escape(injection)
        expected = "'''; DROP TABLE test_files; --'"
        self.assertEqual(result, expected)


class TestSqlEscapeIntegration(unittest.TestCase):
    """Integration tests for sql_escape() in realistic scenarios."""

    def test_file_path_escaping(self):
        """Test escaping various file paths."""
        paths = [
            "random/select/slt_good_19.test",
            "C:\\Users\\test\\file.test",
            "path/with spaces/test.file",
            "path'with'quotes.test",
        ]
        for path in paths:
            result = sql_escape(path)
            # Should always start and end with quotes
            self.assertTrue(result.startswith("'"))
            self.assertTrue(result.endswith("'"))
            # Should not contain unescaped single quotes (except outer ones)
            inner = result[1:-1]
            # Count single quotes - they should come in pairs (escaped as '')
            quote_count = inner.count("'")
            self.assertEqual(quote_count % 2, 0, f"Unescaped quotes in: {path}")

    def test_error_message_escaping(self):
        """Test escaping various error messages."""
        errors = [
            "Lexer error: Unterminated string literal",
            "Parse error at: 'SELECT * FROM'",
            "Error: Unexpected character ':'",
            "Failed: C:\\path\\error.txt",
        ]
        for error in errors:
            result = sql_escape(error)
            # Should always start and end with quotes
            self.assertTrue(result.startswith("'"))
            self.assertTrue(result.endswith("'"))
            # Should not break SQL parsing
            self.assertNotIn("\n", result)
            self.assertNotIn("\r", result)
            self.assertNotIn("\x00", result)


def run_tests():
    """Run all tests and return exit code."""
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # Add all test classes
    suite.addTests(loader.loadTestsFromTestCase(TestSqlEscape))
    suite.addTests(loader.loadTestsFromTestCase(TestSqlEscapeIntegration))

    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    # Return 0 if all tests passed, 1 otherwise
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    sys.exit(run_tests())
