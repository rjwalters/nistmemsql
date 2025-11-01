use parser::Parser;
use ast;

fn main() {
    let sql = "CREATE TABLE `t1710a` (`c1` MULTIPOLYGON COMMENT 'text155459', `c2` MULTIPOLYGON COMMENT 'text155461');";
    
    match Parser::parse_sql(sql) {
        Ok(stmt) => {
            println!("✓ Successfully parsed MULTIPOLYGON with COMMENT");
            match stmt {
                ast::Statement::CreateTable(create) => {
                    println!("Table: {}", create.table_name);
                    println!("Columns: {}", create.columns.len());
                    for col in &create.columns {
                        println!("  Column: {} ({:?}) comment: {:?}", col.name, col.data_type, col.comment);
                    }
                }
                _ => println!("Unexpected statement type"),
            }
        }
        Err(e) => {
            println!("✗ Parse error: {}", e);
            std::process::exit(1);
        }
    }
}
