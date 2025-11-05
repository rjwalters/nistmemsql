use parser::Parser;
use ast::Expression;

fn main() {
    let sql = "SELECT - 14 + 42 * 46 * + 6 + - - 1 - - 22 AS col1";
    println!("Parsing: {}", sql);

    match Parser::parse_sql(sql) {
        Ok(stmt) => {
            println!("Parsed successfully");
            match stmt {
                ast::Statement::Select(select) => {
                    if let Some(expr) = select.projection.first() {
                        if let ast::SelectItem::Expression { expr, alias: _ } = expr {
                            println!("Expression AST: {:?}", expr);
                            // Try to evaluate manually
                            if let Expression::BinaryOp { op, left, right } = expr {
                                println!("Root op: {:?}", op);
                            }
                        }
                    }
                }
                _ => println!("Not a select statement"),
            }
        }
        Err(e) => println!("Parse error: {:?}", e),
    }
}
