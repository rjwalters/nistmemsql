use parser::Parser;

fn main() {
    let result = Parser::parse_sql("SELECT CURRENT_DATE");
    match result {
        Ok(stmt) => println!("Parsed successfully: {:?}", stmt),
        Err(e) => println!("Parse error: {}", e.message),
    }
}
