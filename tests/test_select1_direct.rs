use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::Database;

#[tokio::test]
async fn test_select1_division_direct() {
    let mut db = Database::new();
    
    // Set up t1 table
    let create_sql = r#"
        CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER)
    "#;
    let stmt = Parser::parse_sql(create_sql).expect("Parse create table");
    let create_stmt = match stmt {
        vibesql_ast::Statement::CreateTable(s) => s,
        _ => panic!("Expected CreateTable"),
    };
    vibesql_executor::CreateTableExecutor::execute(&create_stmt, &mut db).expect("Execute create table");
    
    // First few inserts
    let inserts = vec![
        "INSERT INTO t1(e,c,b,d,a) VALUES(103,102,100,101,104)",
        "INSERT INTO t1(a,c,d,e,b) VALUES(107,106,108,109,105)",
    ];
    
    for insert_sql in inserts {
        let stmt = Parser::parse_sql(insert_sql).expect("Parse insert");
        let insert_stmt = match stmt {
            vibesql_ast::Statement::Insert(s) => s,
            _ => panic!("Expected Insert"),
        };
        vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).expect("Execute insert");
    }
    
    // Test the division query
    let query = r#"
        SELECT a+b*2+c*3+d*4+e*5 AS col1,
               (a+b+c+d+e)/5 AS col2
          FROM t1
         ORDER BY 1,2
    "#;
    
    let stmt = Parser::parse_sql(query).expect("Parse query");
    let select_stmt = match stmt {
        vibesql_ast::Statement::Select(s) => s,
        _ => panic!("Expected Select"),
    };
    
    let executor = SelectExecutor::new(&db);
    let rows = executor.execute(&select_stmt).expect("Execute query");
    
    println!("Result rows: {}", rows.len());
    for (i, row) in rows.iter().enumerate() {
        let col1 = &row.values[0];
        let col2 = &row.values[1];
        println!("Row {}: col1={:?}, col2={:?}", i, col1, col2);
    }
}
