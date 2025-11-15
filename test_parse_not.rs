// Quick test to check how NOT col IS NULL is being parsed
use vibesql_parser::Parser;

fn main() {
    let sql = "SELECT * FROM t WHERE NOT col0 IS NULL";
    let stmt = Parser::parse_sql(sql).unwrap();

    println!("Parsed AST:");
    println!("{:#?}", stmt);

    // Also try the correct form
    let sql2 = "SELECT * FROM t WHERE col0 IS NOT NULL";
    let stmt2 = Parser::parse_sql(sql2).unwrap();

    println!("\n\nCorrect form AST:");
    println!("{:#?}", stmt2);
}
