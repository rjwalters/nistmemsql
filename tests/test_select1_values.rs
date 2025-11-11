use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::Database;

#[test]
fn test_select1_division_query() {
    let mut db = Database::new();
    db.set_sql_mode(vibesql_types::SqlMode::MySQL);
    
    // Setup
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
            vibesql_ast::Statement::CreateTable(create_stmt) => {
                vibesql_executor::CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();
            }
            vibesql_ast::Statement::Insert(insert_stmt) => {
                vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();
            }
            _ => panic!("Unexpected statement"),
        }
    }
    
    // The query that's failing:
    // SELECT a+b*2+c*3+d*4+e*5,
    //        (a+b+c+d+e)/5
    //   FROM t1
    //  ORDER BY 1,2
    
    let query_sql = "SELECT a+b*2+c*3+d*4+e*5, (a+b+c+d+e)/5 FROM t1 ORDER BY 1,2";
    
    let stmt = Parser::parse_sql(query_sql).unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select_stmt) => {
            let executor = SelectExecutor::new(&db);
            let rows = executor.execute(&select_stmt).unwrap();
            
            println!("Query: {}", query_sql);
            println!("Result count: {}", rows.len());
            println!("\nResults (col1, col2):");
            
            for (i, row) in rows.iter().enumerate() {
                let col1_str = match &row.values[0] {
                    vibesql_types::SqlValue::Integer(n) => n.to_string(),
                    vibesql_types::SqlValue::Numeric(n) => n.to_string(),
                    v => format!("{:?}", v),
                };
                
                let col2_str = match &row.values[1] {
                    vibesql_types::SqlValue::Integer(n) => n.to_string(),
                    vibesql_types::SqlValue::Float(f) => {
                        if f.fract() == 0.0 {
                            format!("{:.1}", f)
                        } else {
                            f.to_string()
                        }
                    }
                    vibesql_types::SqlValue::Numeric(n) => n.to_string(),
                    v => format!("{:?}", v),
                };
                
                println!("{:2}: ({}, {})", i, col1_str, col2_str);
                
                if i >= 4 {
                    println!("    ... (showing first 5)");
                    break;
                }
            }
            
            // Try hashing the results like sqllogictest does
            println!("\nValue types (first 5 rows):");
            for (i, row) in rows.iter().take(5).enumerate() {
                println!("  Row {}: Col1={:?}, Col2={:?}", i, row.values[0], row.values[1]);
            }
        }
        _ => panic!("Expected SELECT"),
    }
}
