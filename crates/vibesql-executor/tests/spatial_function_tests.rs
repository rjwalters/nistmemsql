/// Spatial Function Tests
/// 
/// Tests for Phase 1 (WKT) and Phase 3 (Predicates) spatial functions

use vibesql_executor::Database;

#[test]
fn test_st_geomfromtext_point() {
    let db = Database::new_in_memory();
    
    // Simple point geometry
    let result = db.execute_sql("SELECT ST_GeomFromText('POINT(10 20)')").unwrap();
    assert!(result.len() > 0);
}

#[test]
fn test_st_geometrytype() {
    let db = Database::new_in_memory();
    
    let result = db.execute_sql("SELECT ST_GeometryType(ST_GeomFromText('POINT(0 0)'))").unwrap();
    assert!(!result.is_empty());
}

#[test]
fn test_st_astext() {
    let db = Database::new_in_memory();
    
    let sql = r#"
        SELECT ST_AsText(ST_GeomFromText('POINT(1 2)'))
    "#;
    let result = db.execute_sql(sql).unwrap();
    assert!(!result.is_empty());
}

#[test]
fn test_st_x_y_coordinates() {
    let db = Database::new_in_memory();
    
    let x_result = db.execute_sql("SELECT ST_X(ST_GeomFromText('POINT(5 10)'))").unwrap();
    let y_result = db.execute_sql("SELECT ST_Y(ST_GeomFromText('POINT(5 10)'))").unwrap();
    
    assert!(!x_result.is_empty());
    assert!(!y_result.is_empty());
}

#[test]
fn test_st_dimension() {
    let db = Database::new_in_memory();
    
    let point_dim = db.execute_sql("SELECT ST_Dimension(ST_GeomFromText('POINT(0 0)'))").unwrap();
    let line_dim = db.execute_sql("SELECT ST_Dimension(ST_GeomFromText('LINESTRING(0 0, 1 1)'))").unwrap();
    let poly_dim = db.execute_sql("SELECT ST_Dimension(ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'))").unwrap();
    
    assert!(!point_dim.is_empty());
    assert!(!line_dim.is_empty());
    assert!(!poly_dim.is_empty());
}

#[test]
fn test_st_srid() {
    let db = Database::new_in_memory();
    
    // Phase 1: SRID always returns 0
    let result = db.execute_sql("SELECT ST_SRID(ST_GeomFromText('POINT(0 0)'))").unwrap();
    assert!(!result.is_empty());
}

#[test]
fn test_st_asgeojson_point() {
    let db = Database::new_in_memory();
    
    let result = db.execute_sql("SELECT ST_AsGeoJSON(ST_GeomFromText('POINT(3 4)'))").unwrap();
    assert!(!result.is_empty());
}

#[test]
fn test_st_pointfromtext() {
    let db = Database::new_in_memory();
    
    let result = db.execute_sql("SELECT ST_PointFromText('POINT(100 200)')").unwrap();
    assert!(!result.is_empty());
}

#[test]
fn test_st_linefromtext() {
    let db = Database::new_in_memory();
    
    let result = db.execute_sql("SELECT ST_LineFromText('LINESTRING(0 0, 1 1, 2 0)')").unwrap();
    assert!(!result.is_empty());
}

#[test]
fn test_st_polygonfromtext() {
    let db = Database::new_in_memory();
    
    let result = db.execute_sql("SELECT ST_PolygonFromText('POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))')").unwrap();
    assert!(!result.is_empty());
}

// Phase 3 Predicate Tests

#[test]
fn test_st_contains() {
    let db = Database::new_in_memory();
    
    let result = db.execute_sql(
        "SELECT ST_Contains(ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'), ST_GeomFromText('POINT(5 5)'))"
    ).unwrap();
    assert!(!result.is_empty());
}

#[test]
fn test_st_within() {
    let db = Database::new_in_memory();
    
    let result = db.execute_sql(
        "SELECT ST_Within(ST_GeomFromText('POINT(5 5)'), ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'))"
    ).unwrap();
    assert!(!result.is_empty());
}

#[test]
fn test_st_intersects() {
    let db = Database::new_in_memory();
    
    let result = db.execute_sql(
        "SELECT ST_Intersects(ST_GeomFromText('LINESTRING(0 0, 10 10)'), ST_GeomFromText('LINESTRING(0 10, 10 0)'))"
    ).unwrap();
    assert!(!result.is_empty());
}

#[test]
fn test_st_disjoint() {
    let db = Database::new_in_memory();
    
    let result = db.execute_sql(
        "SELECT ST_Disjoint(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(10 10)'))"
    ).unwrap();
    assert!(!result.is_empty());
}

#[test]
fn test_st_equals() {
    let db = Database::new_in_memory();
    
    let result = db.execute_sql(
        "SELECT ST_Equals(ST_GeomFromText('POINT(1 1)'), ST_GeomFromText('POINT(1 1)'))"
    ).unwrap();
    assert!(!result.is_empty());
}

#[test]
fn test_st_crosses() {
    let db = Database::new_in_memory();
    
    let result = db.execute_sql(
        "SELECT ST_Crosses(ST_GeomFromText('LINESTRING(0 0, 10 10)'), ST_GeomFromText('LINESTRING(0 10, 10 0)'))"
    ).unwrap();
    assert!(!result.is_empty());
}

#[test]
fn test_st_overlaps() {
    let db = Database::new_in_memory();
    
    let result = db.execute_sql(
        "SELECT ST_Overlaps(ST_GeomFromText('POLYGON((0 0, 5 0, 5 5, 0 5, 0 0))'), ST_GeomFromText('POLYGON((3 3, 8 3, 8 8, 3 8, 3 3))'))"
    ).unwrap();
    assert!(!result.is_empty());
}

#[test]
fn test_st_covers() {
    let db = Database::new_in_memory();
    
    let result = db.execute_sql(
        "SELECT ST_Covers(ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'), ST_GeomFromText('POINT(5 5)'))"
    ).unwrap();
    assert!(!result.is_empty());
}

#[test]
fn test_st_coveredby() {
    let db = Database::new_in_memory();
    
    let result = db.execute_sql(
        "SELECT ST_CoveredBy(ST_GeomFromText('POINT(5 5)'), ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'))"
    ).unwrap();
    assert!(!result.is_empty());
}

#[test]
fn test_st_dwithin() {
    let db = Database::new_in_memory();
    
    let result = db.execute_sql(
        "SELECT ST_DWithin(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(1 1)'), 2.0)"
    ).unwrap();
    assert!(!result.is_empty());
}

#[test]
fn test_null_handling() {
    let db = Database::new_in_memory();
    
    // Test NULL handling for all functions
    let result = db.execute_sql("SELECT ST_GeomFromText(NULL)").unwrap();
    assert!(!result.is_empty());
    
    let result = db.execute_sql("SELECT ST_X(NULL)").unwrap();
    assert!(!result.is_empty());
    
    let result = db.execute_sql("SELECT ST_Contains(NULL, ST_GeomFromText('POINT(0 0)'))").unwrap();
    assert!(!result.is_empty());
}

#[test]
fn test_polygon_with_hole() {
    let db = Database::new_in_memory();
    
    // Polygon with hole
    let sql = r#"
        SELECT ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 2 8, 8 8, 8 2, 2 2))')
    "#;
    let result = db.execute_sql(sql).unwrap();
    assert!(!result.is_empty());
}
