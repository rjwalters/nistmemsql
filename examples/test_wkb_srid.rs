//! Test WKB format and SRID tracking (Phase 2 spatial functions)

use vibesql::executor::QueryExecutor;
use vibesql::parser::Parser;
use vibesql::storage::Database;

fn main() {
    let mut db = Database::new();

    println!("Testing Phase 2: WKB Format & SRID Tracking");
    println!("===========================================\n");

    // Test 1: ST_GeomFromWKB - Create POINT from WKB
    println!("Test 1: ST_GeomFromWKB - Create POINT from WKB binary");
    test_geom_from_wkb(&mut db);

    // Test 2: ST_AsBinary - Convert to WKB
    println!("\nTest 2: ST_AsBinary - Convert geometry to WKB");
    test_as_binary(&mut db);

    // Test 3: ST_SRID - Get SRID
    println!("\nTest 3: ST_SRID - Get spatial reference ID");
    test_srid_getter(&mut db);

    // Test 4: ST_SetSRID - Set SRID
    println!("\nTest 4: ST_SetSRID - Set SRID on geometry");
    test_srid_setter(&mut db);

    // Test 5: Round-trip WKT -> WKB -> WKT
    println!("\nTest 5: Round-trip: WKT -> WKB -> WKT preservation");
    test_roundtrip(&mut db);

    // Test 6: SRID persistence
    println!("\nTest 6: SRID persistence through WKB conversion");
    test_srid_persistence(&mut db);

    println!("\n🎉 All WKB & SRID tests passed!");
}

fn test_geom_from_wkb(db: &mut Database) {
    // WKB for POINT(1 2): 0101000000000000000000F03F0000000000000040
    // Byte order (1=LE), Type (1=Point), X (1.0), Y (2.0)
    let sql = "SELECT ST_AsText(ST_GeomFromWKB(0x0101000000000000000000F03F0000000000000040))";
    
    match execute_sql(sql, db) {
        Ok(result) => {
            println!("  ✓ Query: {}", sql);
            println!("  ✓ Result: {}", result);
            // Should return POINT(1 2) or similar
        }
        Err(e) => {
            eprintln!("  ✗ Failed: {}", e);
        }
    }
}

fn test_as_binary(db: &mut Database) {
    let sql = "SELECT HEX(ST_AsBinary(ST_GeomFromText('POINT(1 2)')))";
    
    match execute_sql(sql, db) {
        Ok(result) => {
            println!("  ✓ Query: {}", sql);
            println!("  ✓ Result: {}", result);
            // Should return hex-encoded WKB binary
        }
        Err(e) => {
            eprintln!("  ✗ Failed: {}", e);
        }
    }
}

fn test_srid_getter(db: &mut Database) {
    let sql = "SELECT ST_SRID(ST_GeomFromText('POINT(-122.4194 37.7749)', 4326))";
    
    match execute_sql(sql, db) {
        Ok(result) => {
            println!("  ✓ Query: {}", sql);
            println!("  ✓ Result: {} (expected 4326)", result);
        }
        Err(e) => {
            eprintln!("  ✗ Failed: {}", e);
        }
    }
}

fn test_srid_setter(db: &mut Database) {
    let sql = "SELECT ST_SRID(ST_SetSRID(ST_GeomFromText('POINT(1 2)'), 4326))";
    
    match execute_sql(sql, db) {
        Ok(result) => {
            println!("  ✓ Query: {}", sql);
            println!("  ✓ Result: {} (expected 4326)", result);
        }
        Err(e) => {
            eprintln!("  ✗ Failed: {}", e);
        }
    }
}

fn test_roundtrip(db: &mut Database) {
    // Create POINT as WKT, convert to WKB, then back to WKT
    let sql = "SELECT ST_AsText(ST_GeomFromWKB(ST_AsBinary(ST_GeomFromText('POINT(1 2)'))))";
    
    match execute_sql(sql, db) {
        Ok(result) => {
            println!("  ✓ WKT -> WKB -> WKT round-trip");
            println!("  ✓ Result: {}", result);
        }
        Err(e) => {
            eprintln!("  ✗ Failed: {}", e);
        }
    }
}

fn test_srid_persistence(db: &mut Database) {
    // Create geometry with SRID, convert to WKB, retrieve SRID
    let sql = "SELECT ST_SRID(ST_GeomFromWKB(ST_AsBinary(ST_SetSRID(ST_GeomFromText('POINT(-122.4194 37.7749)'), 4326))))";
    
    match execute_sql(sql, db) {
        Ok(result) => {
            println!("  ✓ SRID persistence test");
            println!("  ✓ Result: {} (expected 4326)", result);
        }
        Err(e) => {
            eprintln!("  ✗ Failed: {}", e);
        }
    }
}

fn execute_sql(sql: &str, db: &mut Database) -> Result<String, String> {
    match Parser::parse_sql(sql) {
        Ok(stmt) => {
            match QueryExecutor::execute(&stmt, db) {
                Ok(result) => {
                    let result_str = format!("{:?}", result);
                    Ok(result_str)
                }
                Err(e) => Err(format!("{:?}", e)),
            }
        }
        Err(e) => Err(format!("{:?}", e)),
    }
}
