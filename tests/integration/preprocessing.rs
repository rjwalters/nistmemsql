//! Unit tests for MySQL dialect preprocessing logic.
//!
//! These tests verify the `preprocess_for_mysql` function that filters
//! SQLLogicTest directives (onlyif/skipif) for MySQL dialect compatibility.
//!
//! NOTE: These tests were moved from sqllogictest_suite.rs when that file
//! switched to `harness = false` to avoid Rust's 60-second test timeout.

/// Preprocess test file content to filter MySQL-specific directives
fn preprocess_for_mysql(content: &str) -> String {
    let mut output_lines = Vec::new();
    let mut skip_next_record = false;

    for line in content.lines() {
        // Check for dialect directives
        if line.starts_with("onlyif ") {
            let dialect =
                line.trim_start_matches("onlyif ").split_whitespace().next().unwrap_or("");
            skip_next_record = dialect != "mysql";
            continue; // Don't include the directive line
        } else if line.starts_with("skipif ") {
            let dialect =
                line.trim_start_matches("skipif ").split_whitespace().next().unwrap_or("");
            skip_next_record = dialect == "mysql";
            continue; // Don't include the directive line
        }

        // If we're not skipping, include the line
        // The skip applies to the entire test record (until next blank line or new test)
        if skip_next_record {
            // Skip this line, but check if we've reached the end of the record
            if line.trim().is_empty() {
                skip_next_record = false;
                output_lines.push(line); // Include blank lines
            }
            // Continue skipping until blank line or new test starts (implicitly via next directive)
        } else {
            output_lines.push(line);
        }
    }

    output_lines.join("\n")
}

#[test]
fn test_preprocess_onlyif_mysql() {
    let input = "statement ok\nCREATE TABLE t1 (x INT)\n\nonlyif mysql\nstatement ok\nINSERT INTO t1 VALUES (1)\n\nonlyif postgresql\nstatement ok\nINSERT INTO t1 VALUES (2)\n";
    let output = preprocess_for_mysql(input);

    // Should include MySQL-specific statement
    assert!(output.contains("INSERT INTO t1 VALUES (1)"));
    // Should exclude PostgreSQL-specific statement
    assert!(!output.contains("INSERT INTO t1 VALUES (2)"));
    // Should not include directive lines
    assert!(!output.contains("onlyif"));
}

#[test]
fn test_preprocess_skipif_mysql() {
    let input = "statement ok\nCREATE TABLE t1 (x INT)\n\nskipif mysql\nstatement ok\nINSERT INTO t1 VALUES (1)\n\nskipif postgresql\nstatement ok\nINSERT INTO t1 VALUES (2)\n";
    let output = preprocess_for_mysql(input);

    // Should exclude MySQL-skipped statement
    assert!(!output.contains("INSERT INTO t1 VALUES (1)"));
    // Should include statement not skipped for MySQL
    assert!(output.contains("INSERT INTO t1 VALUES (2)"));
    // Should not include directive lines
    assert!(!output.contains("skipif"));
}

#[test]
fn test_preprocess_directive_with_comment() {
    let input = "onlyif mysql # aggregate syntax:\nstatement ok\nSELECT SUM(x) FROM t1\n\nskipif mysql # unsupported feature\nstatement ok\nINSERT INTO t1 VALUES (99)\n";
    let output = preprocess_for_mysql(input);

    // MySQL directive with comment should include statement
    assert!(
        output.contains("SELECT SUM(x) FROM t1"),
        "onlyif mysql with comment should include MySQL statement"
    );
    // MySQL skipif with comment should exclude statement
    assert!(
        !output.contains("INSERT INTO t1 VALUES (99)"),
        "skipif mysql with comment should exclude MySQL statement"
    );
    // Directives should be removed
    assert!(!output.contains("onlyif"));
    assert!(!output.contains("skipif"));
}

#[test]
fn test_preprocess_mixed_directives() {
    let input = r#"statement ok
CREATE TABLE t1 (x INT)

onlyif mysql
statement ok
INSERT INTO t1 VALUES (1)

skipif mysql
query I
SELECT * FROM t1 WHERE x > 10
----

onlyif postgresql
statement ok
INSERT INTO t1 VALUES (2)

statement ok
INSERT INTO t1 VALUES (3)
"#;
    let output = preprocess_for_mysql(input);

    // MySQL-only statement should be included
    assert!(output.contains("INSERT INTO t1 VALUES (1)"));
    // MySQL-skipped query should be excluded
    assert!(!output.contains("SELECT * FROM t1 WHERE x > 10"));
    // PostgreSQL-only statement should be excluded
    assert!(!output.contains("INSERT INTO t1 VALUES (2)"));
    // Universal statement should be included
    assert!(output.contains("INSERT INTO t1 VALUES (3)"));
    // No directives should remain
    assert!(!output.contains("onlyif"));
    assert!(!output.contains("skipif"));
}
