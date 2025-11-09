use executor::SelectExecutor;
use parser::Parser;
use storage::Database;

#[test]
fn test_failing_case_query() {
    let mut db = Database::new();

    // Set up the table and data from select1.test
    let setup_sql = r#"
CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER);
INSERT INTO t1(e,c,b,d,a) VALUES(103,102,100,101,104);
INSERT INTO t1(a,c,d,e,b) VALUES(107,106,108,109,105);
INSERT INTO t1(e,d,b,a,c) VALUES(110,114,112,111,113);
INSERT INTO t1(d,c,e,a,b) VALUES(116,119,117,115,118);
INSERT INTO t1(c,d,b,e,a) VALUES(123,122,124,120,121);
INSERT INTO t1(a,d,b,e,c) VALUES(127,128,129,126,125);
INSERT INTO t1(e,c,a,d,b) VALUES(132,134,131,133,130);
INSERT INTO t1(a,d,b,e,c) VALUES(138,136,139,135,137);
INSERT INTO t1(e,c,d,a,b) VALUES(144,141,140,142,143);
INSERT INTO t1(b,a,e,d,c) VALUES(145,149,146,148,147);
INSERT INTO t1(b,c,a,d,e) VALUES(151,150,153,154,152);
INSERT INTO t1(c,e,a,d,b) VALUES(155,157,159,156,158);
INSERT INTO t1(c,b,a,d,e) VALUES(161,160,163,164,162);
INSERT INTO t1(b,d,a,e,c) VALUES(167,169,168,165,166);
INSERT INTO t1(d,b,c,e,a) VALUES(171,170,172,173,174);
INSERT INTO t1(e,c,a,d,b) VALUES(177,176,179,178,175);
INSERT INTO t1(b,e,a,d,c) VALUES(181,180,182,183,184);
INSERT INTO t1(c,a,b,e,d) VALUES(187,188,186,189,185);
INSERT INTO t1(d,b,c,e,a) VALUES(190,194,193,192,191);
INSERT INTO t1(a,e,b,d,c) VALUES(199,197,198,196,195);
INSERT INTO t1(b,c,d,a,e) VALUES(200,202,203,201,204);
INSERT INTO t1(c,e,a,b,d) VALUES(208,209,205,206,207);
INSERT INTO t1(c,e,a,d,b) VALUES(214,210,213,212,211);
INSERT INTO t1(b,c,a,d,e) VALUES(218,215,216,217,219);
INSERT INTO t1(b,e,d,a,c) VALUES(223,221,222,220,224);
INSERT INTO t1(d,e,b,a,c) VALUES(226,227,228,229,225);
INSERT INTO t1(a,c,b,e,d) VALUES(234,231,232,230,233);
INSERT INTO t1(e,b,a,c,d) VALUES(237,236,239,235,238);
INSERT INTO t1(e,c,b,a,d) VALUES(242,244,240,243,241);
INSERT INTO t1(e,d,c,b,a) VALUES(246,248,247,249,245);
"#;

    for sql in setup_sql.trim().split(';').filter(|s| !s.trim().is_empty()) {
        let stmt = Parser::parse_sql(sql.trim()).unwrap();
        match stmt {
            ast::Statement::CreateTable(create_stmt) => {
                executor::CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();
            }
            ast::Statement::Insert(insert_stmt) => {
                executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();
            }
            _ => panic!("Unexpected statement"),
        }
    }

    // First, check what avg(c) returns
    let avg_sql = "SELECT avg(c) FROM t1";
    let avg_stmt = Parser::parse_sql(avg_sql).unwrap();
    match avg_stmt {
        ast::Statement::Select(select_stmt) => {
            let executor = SelectExecutor::new(&db);
            let rows = executor.execute(&select_stmt).unwrap();
            println!(
                "avg(c) result: {:?} (type: {})",
                rows[0].values[0],
                match rows[0].values[0] {
                    types::SqlValue::Integer(_) => "Integer",
                    types::SqlValue::Numeric(_) => "Numeric",
                    types::SqlValue::Float(_) => "Float",
                    _ => "Other",
                }
            );
        }
        _ => panic!("Expected SELECT"),
    }

    // Test a direct comparison
    let cmp_sql = "SELECT c > 174.36666666666667 FROM t1 LIMIT 5";
    let cmp_stmt = Parser::parse_sql(cmp_sql).unwrap();
    match cmp_stmt {
        ast::Statement::Select(select_stmt) => {
            let executor = SelectExecutor::new(&db);
            let rows = executor.execute(&select_stmt).unwrap();
            println!("Direct comparison results:");
            for row in &rows {
                println!("  {:?}", row.values);
            }
        }
        _ => panic!("Expected SELECT"),
    }

    // Now run the failing query
    let query_sql =
        "SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END FROM t1 ORDER BY 1";
    let stmt = Parser::parse_sql(query_sql).unwrap();
    match stmt {
        ast::Statement::Select(select_stmt) => {
            let executor = SelectExecutor::new(&db);
            let rows = executor.execute(&select_stmt).unwrap();

            println!("Query results ({} rows):", rows.len());
            for (i, row) in rows.iter().enumerate() {
                if i < 10 {
                    // Print first 10 results
                    println!("  Row {}: {:?}", i, row.values);
                } else if i == 10 {
                    println!("  ... ({} more rows)", rows.len() - 10);
                    break;
                }
            }

            // Check if all results are multiples of 10 (indicating ELSE branch always taken)
            let all_multiples_of_10 = rows
                .iter()
                .all(|row| matches!(row.values[0], types::SqlValue::Integer(n) if n % 10 == 0));
            println!("All results are multiples of 10: {}", all_multiples_of_10);
        }
        _ => panic!("Expected SELECT"),
    }
}
