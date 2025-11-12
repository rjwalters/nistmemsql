use vibesql_storage::Database;
use vibesql_parser::Parser;
use vibesql_executor::advanced_objects::execute_create_view;

fn main() {
    let mut db = Database::new();
    
    // Parse and execute CREATE TABLE
    let mut parser = Parser::new("CREATE TABLE t1(x INTEGER);");
    let create_table = parser.parse().unwrap();
    // ... execute create table
    
    // Parse and execute CREATE VIEW with column alias
    let mut parser = Parser::new("CREATE VIEW v1(col_a) AS SELECT x FROM t1;");
    let stmt = parser.parse().unwrap();
    
    // Print what we get
    println!("Statement: {:?}", stmt);
}
