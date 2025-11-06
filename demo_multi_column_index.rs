use storage::Database;
use ast::{CreateIndexStmt, IndexColumn, OrderDirection, CreateTableStmt, ColumnDef};
use executor::{IndexExecutor, CreateTableExecutor};
use types::DataType;

fn main() {
    println!("🔍 Testing multi-column index creation...");

    let mut db = Database::new();
    
    // First create the table
    let create_table_stmt = CreateTableStmt {
        table_name: "t1".to_string(),
        columns: vec![
            ColumnDef {
                name: "a1".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
            ColumnDef {
                name: "b1".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
            ColumnDef {
                name: "c1".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
            ColumnDef {
                name: "d1".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
            ColumnDef {
                name: "e1".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
            ColumnDef {
                name: "x1".to_string(),
                data_type: DataType::Varchar { max_length: Some(30) },
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
    };
    
    match CreateTableExecutor::execute(&create_table_stmt, &mut db) {
        Ok(_) => println!("✅ Table 't1' created successfully"),
        Err(e) => {
            println!("❌ Failed to create table: {:?}", e);
            return;
        }
    }
    
    // Now create the multi-column index (this was previously failing)
    let create_index_stmt = CreateIndexStmt {
        index_name: "t1i0".to_string(),
        table_name: "t1".to_string(),
        unique: false,
        columns: vec![
            IndexColumn { column_name: "a1".to_string(), direction: OrderDirection::Asc },
            IndexColumn { column_name: "b1".to_string(), direction: OrderDirection::Asc },
            IndexColumn { column_name: "c1".to_string(), direction: OrderDirection::Asc },
            IndexColumn { column_name: "d1".to_string(), direction: OrderDirection::Asc },
            IndexColumn { column_name: "e1".to_string(), direction: OrderDirection::Asc },
            IndexColumn { column_name: "x1".to_string(), direction: OrderDirection::Asc },
        ],
    };
    
    // Before our fix, this would fail with:
    // "Not implemented: Multi-column indexes not yet supported"
    
    match IndexExecutor::execute(&create_index_stmt, &mut db) {
        Ok(msg) => println!("✅ SUCCESS: {}", msg),
        Err(e) => {
            println!("❌ FAILED: {:?}", e);
            return;
        }
    }
    
    // Verify the index exists
    if db.index_exists("t1i0") {
        println!("✅ Index 't1i0' exists in database");
        if let Some(metadata) = db.get_index("t1i0") {
            println!("✅ Index metadata: {} columns on table {}", 
                metadata.columns.len(), metadata.table_name);
        }
    } else {
        println!("❌ Index 't1i0' not found");
    }
    
    println!("\n🎉 Multi-column index implementation verified!");
    println!("   This CREATE INDEX statement would have failed before our fix.");
}
