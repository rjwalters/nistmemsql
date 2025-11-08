use parser::Parser;

#[test]
fn test_parse_is_null_with_subtraction() {
    // Test how the expression is parsed
    let sql = "SELECT col0 FROM tab0 WHERE col0 - col1 IS NULL";
    let stmt = Parser::parse_sql(sql).expect("Parse should succeed");
    
    println!("Parsed statement: {:#?}", stmt);
}

#[test]
fn test_parse_arithmetic_is_null() {
    let sql = "SELECT * FROM tab0 WHERE 15 * - 99 IS NULL";
    let stmt = Parser::parse_sql(sql).expect("Parse should succeed");
    
    println!("Parsed statement: {:#?}", stmt);
}
