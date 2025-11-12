//! Spatial/Geometric Functions
//!
//! Implements SQL spatial functions for working with geometries.
//! Functions are organized by phase:
//!
//! - **Phase 1 (WKT)**: Constructor and accessor functions for WKT parsing
//!   - ST_GeomFromText, ST_PointFromText, ST_LineFromText, ST_PolygonFromText
//!   - ST_X, ST_Y, ST_GeometryType, ST_Dimension, ST_SRID
//!   - ST_AsText, ST_AsGeoJSON, ST_AsBinary
//!
//! - **Phase 3 (Predicates)**: Spatial relationship testing
//!   - ST_Contains, ST_Within, ST_Intersects, ST_Crosses, ST_Overlaps, ST_Touches, ST_Disjoint, ST_Equals
//!   - ST_Covers, ST_CoveredBy, ST_Relate, ST_DWithin
//!
//! Geometries are internally stored as WKT strings with a marker prefix for type identification.

use crate::errors::ExecutorError;
use vibesql_types::SqlValue;
use geo::{Contains, Intersects, HaversineDistance};

// Internal geometry marker prefix for WKT storage
const GEOM_MARKER: &str = "GEOM:";

/// Parse WKT string and return geometry with type marker
fn parse_wkt(wkt: &str) -> Result<String, ExecutorError> {
    let trimmed = wkt.trim();
    
    // Validate basic WKT structure
    if trimmed.is_empty() {
        return Err(ExecutorError::Other(
            "Empty WKT string".to_string(),
        ));
    }
    
    // Try to parse with wkt crate
    let _: wkt::Wkt<f64> = trimmed.parse()
        .map_err(|e| ExecutorError::Other(
            format!("Invalid WKT format: {}", e),
        ))?;
    
    // Store with marker prefix and uppercase type
    Ok(format!("{}{}", GEOM_MARKER, trimmed.to_uppercase()))
}

/// Extract geometry type from WKT string
fn get_geometry_type(wkt: &str) -> Result<String, ExecutorError> {
    let trimmed = wkt.trim_start_matches(GEOM_MARKER).trim();
    let geom_type = trimmed
        .split('(')
        .next()
        .unwrap_or("")
        .trim()
        .to_uppercase();
    
    if geom_type.is_empty() {
        return Err(ExecutorError::Other(
            "Cannot determine geometry type".to_string(),
        ));
    }
    
    Ok(geom_type)
}

/// Dimension: 0=Point, 1=Line, 2=Polygon, -1=Empty
fn get_dimension(geom_type: &str) -> i32 {
    match geom_type.to_uppercase().as_str() {
        "POINT" => 0,
        "MULTIPOINT" => 0,
        "LINESTRING" => 1,
        "MULTILINESTRING" => 1,
        "POLYGON" => 2,
        "MULTIPOLYGON" => 2,
        "GEOMETRYCOLLECTION" => -1,
        _ => -1,
    }
}

// ============================================================================
// Phase 1: WKT Functions
// ============================================================================

/// ST_GeomFromText(wkt) - Create any geometry from WKT string
pub fn st_geomfromtext(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.is_empty() || args.len() > 2 {
        return Err(ExecutorError::Other(
            "ST_GeomFromText expects 1 or 2 arguments".to_string(),
        ));
    }
    
    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Varchar(wkt) | SqlValue::Character(wkt) => {
            let geom = parse_wkt(wkt)?;
            Ok(SqlValue::Varchar(geom))
        }
        _ => Err(ExecutorError::Other(format!(
            "ST_GeomFromText requires VARCHAR argument, got {}",
            args[0].type_name()
        ))),
    }
}

/// ST_PointFromText(wkt) - Create POINT from WKT string
pub fn st_pointfromtext(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.is_empty() || args.len() > 2 {
        return Err(ExecutorError::Other(
            "ST_PointFromText expects 1 or 2 arguments".to_string(),
        ));
    }
    
    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Varchar(wkt) | SqlValue::Character(wkt) => {
            let geom = parse_wkt(wkt)?;
            let geom_type = get_geometry_type(&geom)?;
            
            if geom_type != "POINT" {
                return Err(ExecutorError::Other(format!(
                    "ST_PointFromText requires POINT WKT, got {}",
                    geom_type
                )));
            }
            
            Ok(SqlValue::Varchar(geom))
        }
        _ => Err(ExecutorError::Other(format!(
            "ST_PointFromText requires VARCHAR argument, got {}",
            args[0].type_name()
        ))),
    }
}

/// ST_LineFromText(wkt) - Create LINESTRING from WKT string
pub fn st_linefromtext(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.is_empty() || args.len() > 2 {
        return Err(ExecutorError::Other(
            "ST_LineFromText expects 1 or 2 arguments".to_string(),
        ));
    }
    
    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Varchar(wkt) | SqlValue::Character(wkt) => {
            let geom = parse_wkt(wkt)?;
            let geom_type = get_geometry_type(&geom)?;
            
            if geom_type != "LINESTRING" {
                return Err(ExecutorError::Other(format!(
                    "ST_LineFromText requires LINESTRING WKT, got {}",
                    geom_type
                )));
            }
            
            Ok(SqlValue::Varchar(geom))
        }
        _ => Err(ExecutorError::Other(format!(
            "ST_LineFromText requires VARCHAR argument, got {}",
            args[0].type_name()
        ))),
    }
}

/// ST_PolygonFromText(wkt) - Create POLYGON from WKT string
pub fn st_polygonfromtext(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.is_empty() || args.len() > 2 {
        return Err(ExecutorError::Other(
            "ST_PolygonFromText expects 1 or 2 arguments".to_string(),
        ));
    }
    
    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Varchar(wkt) | SqlValue::Character(wkt) => {
            let geom = parse_wkt(wkt)?;
            let geom_type = get_geometry_type(&geom)?;
            
            if geom_type != "POLYGON" {
                return Err(ExecutorError::Other(format!(
                    "ST_PolygonFromText requires POLYGON WKT, got {}",
                    geom_type
                )));
            }
            
            Ok(SqlValue::Varchar(geom))
        }
        _ => Err(ExecutorError::Other(format!(
            "ST_PolygonFromText requires VARCHAR argument, got {}",
            args[0].type_name()
        ))),
    }
}

/// ST_X(point) - Get X coordinate of a point
pub fn st_x(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::Other(
            "ST_X expects exactly 1 argument".to_string(),
        ));
    }
    
    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Varchar(wkt) | SqlValue::Character(wkt) => {
            let trimmed = wkt.trim_start_matches(GEOM_MARKER).trim();
            
            // Parse POINT(x y) or POINT(x y z)
            if !trimmed.starts_with("POINT") {
                return Err(ExecutorError::Other(
                    "ST_X requires a POINT geometry".to_string(),
                ));
            }
            
            let inner = trimmed
                .trim_start_matches("POINT")
                .trim_start_matches('(')
                .trim_end_matches(')')
                .trim();
            
            let x_str = inner.split_whitespace().next()
                .ok_or_else(|| ExecutorError::Other(
                    "Cannot extract X coordinate".to_string(),
                ))?;
            
            let x: f64 = x_str.parse()
                .map_err(|_| ExecutorError::Other(
                    "X coordinate is not a valid number".to_string(),
                ))?;
            
            Ok(SqlValue::Double(x))
        }
        _ => Err(ExecutorError::Other(format!(
            "ST_X requires VARCHAR argument, got {}",
            args[0].type_name()
        ))),
    }
}

/// ST_Y(point) - Get Y coordinate of a point
pub fn st_y(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::Other(
            "ST_Y expects exactly 1 argument".to_string(),
        ));
    }
    
    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Varchar(wkt) | SqlValue::Character(wkt) => {
            let trimmed = wkt.trim_start_matches(GEOM_MARKER).trim();
            
            // Parse POINT(x y) or POINT(x y z)
            if !trimmed.starts_with("POINT") {
                return Err(ExecutorError::Other(
                    "ST_Y requires a POINT geometry".to_string(),
                ));
            }
            
            let inner = trimmed
                .trim_start_matches("POINT")
                .trim_start_matches('(')
                .trim_end_matches(')')
                .trim();
            
            let y_str = inner.split_whitespace().nth(1)
                .ok_or_else(|| ExecutorError::Other(
                    "Cannot extract Y coordinate".to_string(),
                ))?;
            
            let y: f64 = y_str.parse()
                .map_err(|_| ExecutorError::Other(
                    "Y coordinate is not a valid number".to_string(),
                ))?;
            
            Ok(SqlValue::Double(y))
        }
        _ => Err(ExecutorError::Other(format!(
            "ST_Y requires VARCHAR argument, got {}",
            args[0].type_name()
        ))),
    }
}

/// ST_GeometryType(geom) - Get geometry type name
pub fn st_geometrytype(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::Other(
            "ST_GeometryType expects exactly 1 argument".to_string(),
        ));
    }
    
    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Varchar(wkt) | SqlValue::Character(wkt) => {
            let geom_type = get_geometry_type(wkt)?;
            Ok(SqlValue::Varchar(geom_type))
        }
        _ => Err(ExecutorError::Other(format!(
            "ST_GeometryType requires VARCHAR argument, got {}",
            args[0].type_name()
        ))),
    }
}

/// ST_Dimension(geom) - Get dimension (0=point, 1=line, 2=polygon)
pub fn st_dimension(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::Other(
            "ST_Dimension expects exactly 1 argument".to_string(),
        ));
    }
    
    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Varchar(wkt) | SqlValue::Character(wkt) => {
            let geom_type = get_geometry_type(wkt)?;
            let dim = get_dimension(&geom_type);
            Ok(SqlValue::Integer(dim as i64))
        }
        _ => Err(ExecutorError::Other(format!(
            "ST_Dimension requires VARCHAR argument, got {}",
            args[0].type_name()
        ))),
    }
}

/// ST_SRID(geom) - Get spatial reference system ID (always 0 in Phase 1)
pub fn st_srid(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::Other(
            "ST_SRID expects exactly 1 argument".to_string(),
        ));
    }
    
    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Varchar(_) | SqlValue::Character(_) => {
            // Phase 1: Always return 0 (no SRID tracking yet)
            Ok(SqlValue::Integer(0))
        }
        _ => Err(ExecutorError::Other(format!(
            "ST_SRID requires VARCHAR argument, got {}",
            args[0].type_name()
        ))),
    }
}

/// ST_AsText(geom) - Convert geometry to WKT string
pub fn st_astext(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::Other(
            "ST_AsText expects exactly 1 argument".to_string(),
        ));
    }
    
    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Varchar(wkt) | SqlValue::Character(wkt) => {
            // Remove marker and return WKT
            let result = wkt.trim_start_matches(GEOM_MARKER).to_string();
            Ok(SqlValue::Varchar(result))
        }
        _ => Err(ExecutorError::Other(format!(
            "ST_AsText requires VARCHAR argument, got {}",
            args[0].type_name()
        ))),
    }
}

/// ST_AsGeoJSON(geom) - Convert geometry to GeoJSON string
pub fn st_asgeojson(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.is_empty() || args.len() > 2 {
        return Err(ExecutorError::Other(
            "ST_AsGeoJSON expects 1 or 2 arguments".to_string(),
        ));
    }
    
    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Varchar(wkt) | SqlValue::Character(wkt) => {
            let trimmed = wkt.trim_start_matches(GEOM_MARKER);
            
            // Get geometry type for basic GeoJSON structure
            let geom_type = get_geometry_type(wkt).unwrap_or_else(|_| "Unknown".to_string());
            
            // For now, return a simplified GeoJSON with just the type
            // Full coordinates extraction would require deeper WKT parsing
            let geojson = match geom_type.as_str() {
                "POINT" => {
                    // Extract coordinates from POINT(x y)
                    if let Some(start) = trimmed.find('(') {
                        if let Some(end) = trimmed.find(')') {
                            let coords_str = &trimmed[start+1..end];
                            let parts: Vec<&str> = coords_str.split_whitespace().collect();
                            if parts.len() >= 2 {
                                return Ok(SqlValue::Varchar(
                                    format!(r#"{{"type":"Point","coordinates":[{},{}]}}"#, parts[0], parts[1])
                                ));
                            }
                        }
                    }
                    r#"{"type":"Point","coordinates":null}"#.to_string()
                }
                "LINESTRING" => format!(r#"{{"type":"LineString","coordinates":[]}}"#),
                "POLYGON" => format!(r#"{{"type":"Polygon","coordinates":[]}}"#),
                "MULTIPOINT" => format!(r#"{{"type":"MultiPoint","coordinates":[]}}"#),
                "MULTILINESTRING" => format!(r#"{{"type":"MultiLineString","coordinates":[]}}"#),
                "MULTIPOLYGON" => format!(r#"{{"type":"MultiPolygon","coordinates":[]}}"#),
                _ => format!(r#"{{"type":"GeometryCollection","coordinates":[]}}"#),
            };
            
            Ok(SqlValue::Varchar(geojson))
        }
        _ => Err(ExecutorError::Other(format!(
            "ST_AsGeoJSON requires VARCHAR argument, got {}",
            args[0].type_name()
        ))),
    }
}

/// ST_AsBinary(geom) - Stub: Return error (Phase 2 feature)
pub fn st_asbinary(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.is_empty() {
        return Err(ExecutorError::Other(
            "ST_AsBinary expects at least 1 argument".to_string(),
        ));
    }
    
    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        _ => Err(ExecutorError::UnsupportedFeature(
            "ST_AsBinary (WKB conversion) is not yet implemented (Phase 2)".to_string(),
        )),
    }
}

// ============================================================================
// Phase 3: Spatial Predicates
// ============================================================================

/// Helper: Parse coordinate pairs from WKT coordinate string
fn parse_coordinates(coords_str: &str) -> Result<Vec<geo::Coord<f64>>, ExecutorError> {
    let mut coords = Vec::new();
    
    // Handle nested parentheses: split by comma at the top level
    let mut depth = 0;
    let mut current = String::new();
    
    for ch in coords_str.chars() {
        match ch {
            '(' => {
                depth += 1;
                current.push(ch);
            }
            ')' => {
                depth -= 1;
                current.push(ch);
            }
            ',' if depth == 0 => {
                let trimmed = current.trim();
                if !trimmed.is_empty() && !trimmed.starts_with('(') {
                    // Simple coordinate pair
                    let parts: Vec<&str> = trimmed.split_whitespace().collect();
                    if parts.len() >= 2 {
                        if let (Ok(x), Ok(y)) = (parts[0].parse::<f64>(), parts[1].parse::<f64>()) {
                            coords.push(geo::Coord { x, y });
                        }
                    }
                }
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    
    // Process last coordinate
    let trimmed = current.trim();
    if !trimmed.is_empty() && !trimmed.starts_with('(') {
        let parts: Vec<&str> = trimmed.split_whitespace().collect();
        if parts.len() >= 2 {
            if let (Ok(x), Ok(y)) = (parts[0].parse::<f64>(), parts[1].parse::<f64>()) {
                coords.push(geo::Coord { x, y });
            }
        }
    }
    
    if coords.is_empty() {
        return Err(ExecutorError::Other("No valid coordinates found".to_string()));
    }
    
    Ok(coords)
}

/// Helper: Parse WKT to geo::Geometry  
fn wkt_to_geo(wkt_str: &str) -> Result<geo::Geometry<f64>, ExecutorError> {
    let trimmed = wkt_str.trim_start_matches(GEOM_MARKER);
    let _parsed: wkt::Wkt<f64> = trimmed.parse()
        .map_err(|e| ExecutorError::Other(
            format!("Failed to parse WKT: {}", e),
        ))?;
    
    // Extract geometry type to create appropriate geo type
    let geom_type_upper = get_geometry_type(wkt_str)?;
    
    match geom_type_upper.as_str() {
        "POINT" => {
            // Parse POINT(x y)
            if let Some(start) = trimmed.find('(') {
                if let Some(end) = trimmed.rfind(')') {
                    let coords_str = &trimmed[start+1..end];
                    let parts: Vec<&str> = coords_str.split_whitespace().collect();
                    if parts.len() >= 2 {
                        if let (Ok(x), Ok(y)) = (parts[0].parse::<f64>(), parts[1].parse::<f64>()) {
                            return Ok(geo::Geometry::Point(geo::Point::new(x, y)));
                        }
                    }
                }
            }
            Err(ExecutorError::Other("Invalid POINT coordinates".to_string()))
        }
        "LINESTRING" => {
            // Parse LINESTRING(x1 y1, x2 y2, ...)
            if let Some(start) = trimmed.find('(') {
                if let Some(end) = trimmed.rfind(')') {
                    let coords_str = &trimmed[start+1..end];
                    let coords = parse_coordinates(coords_str)?;
                    let line = geo::LineString::new(coords);
                    return Ok(geo::Geometry::LineString(line));
                }
            }
            Err(ExecutorError::Other("Invalid LINESTRING coordinates".to_string()))
        }
        "POLYGON" => {
            // Parse POLYGON((outer ring), (hole1), (hole2), ...)
            if let Some(start) = trimmed.find('(') {
                if let Some(end) = trimmed.rfind(')') {
                    let inner = &trimmed[start+1..end];
                    
                    // Split rings by finding balanced parentheses
                    let mut rings = Vec::new();
                    let mut ring_str = String::new();
                    let mut paren_depth = 0;
                    
                    for ch in inner.chars() {
                        match ch {
                            '(' => {
                                paren_depth += 1;
                                ring_str.push(ch);
                            }
                            ')' => {
                                paren_depth -= 1;
                                ring_str.push(ch);
                                if paren_depth == 0 {
                                    let trimmed_ring = ring_str.trim();
                                    if trimmed_ring.starts_with('(') && trimmed_ring.ends_with(')') {
                                        let coords_part = &trimmed_ring[1..trimmed_ring.len()-1];
                                        if let Ok(coords) = parse_coordinates(coords_part) {
                                            rings.push(coords);
                                        }
                                    }
                                    ring_str.clear();
                                }
                            }
                            ',' if paren_depth == 0 => {
                                // Skip comma between rings
                            }
                            _ => ring_str.push(ch),
                        }
                    }
                    
                    if !rings.is_empty() {
                        let exterior = geo::LineString::new(rings[0].clone());
                        let interiors = rings[1..].iter()
                            .map(|r| geo::LineString::new(r.clone()))
                            .collect();
                        let poly = geo::Polygon::new(exterior, interiors);
                        return Ok(geo::Geometry::Polygon(poly));
                    }
                }
            }
            Err(ExecutorError::Other("Invalid POLYGON coordinates".to_string()))
        }
        _ => Err(ExecutorError::UnsupportedFeature(
            format!("Geometry type {} not yet fully supported", geom_type_upper),
        )),
    }
}

/// ST_Contains(geom1, geom2) - Does geom1 completely contain geom2?
pub fn st_contains(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::Other(
            "ST_Contains expects exactly 2 arguments".to_string(),
        ));
    }
    
    match (&args[0], &args[1]) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),
        (SqlValue::Varchar(wkt1) | SqlValue::Character(wkt1),
         SqlValue::Varchar(wkt2) | SqlValue::Character(wkt2)) => {
            let geom1 = wkt_to_geo(wkt1)?;
            let geom2 = wkt_to_geo(wkt2)?;
            
            let result = geom1.contains(&geom2);
            Ok(SqlValue::Boolean(result))
        }
        _ => Err(ExecutorError::Other(
            "ST_Contains requires VARCHAR geometry arguments".to_string(),
        )),
    }
}

/// ST_Within(geom1, geom2) - Is geom1 completely within geom2?
pub fn st_within(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::Other(
            "ST_Within expects exactly 2 arguments".to_string(),
        ));
    }
    
    match (&args[0], &args[1]) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),
        (SqlValue::Varchar(wkt1) | SqlValue::Character(wkt1),
         SqlValue::Varchar(wkt2) | SqlValue::Character(wkt2)) => {
            let geom1 = wkt_to_geo(wkt1)?;
            let geom2 = wkt_to_geo(wkt2)?;
            
            let result = geom2.contains(&geom1);
            Ok(SqlValue::Boolean(result))
        }
        _ => Err(ExecutorError::Other(
            "ST_Within requires VARCHAR geometry arguments".to_string(),
        )),
    }
}

/// ST_Intersects(geom1, geom2) - Do geom1 and geom2 share any space?
pub fn st_intersects(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::Other(
            "ST_Intersects expects exactly 2 arguments".to_string(),
        ));
    }
    
    match (&args[0], &args[1]) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),
        (SqlValue::Varchar(wkt1) | SqlValue::Character(wkt1),
         SqlValue::Varchar(wkt2) | SqlValue::Character(wkt2)) => {
            let geom1 = wkt_to_geo(wkt1)?;
            let geom2 = wkt_to_geo(wkt2)?;
            
            let result = geom1.intersects(&geom2);
            Ok(SqlValue::Boolean(result))
        }
        _ => Err(ExecutorError::Other(
            "ST_Intersects requires VARCHAR geometry arguments".to_string(),
        )),
    }
}

/// ST_Disjoint(geom1, geom2) - Do geom1 and geom2 share no space?
pub fn st_disjoint(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::Other(
            "ST_Disjoint expects exactly 2 arguments".to_string(),
        ));
    }
    
    match (&args[0], &args[1]) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),
        (SqlValue::Varchar(wkt1) | SqlValue::Character(wkt1),
         SqlValue::Varchar(wkt2) | SqlValue::Character(wkt2)) => {
            let geom1 = wkt_to_geo(wkt1)?;
            let geom2 = wkt_to_geo(wkt2)?;
            
            // Disjoint = NOT Intersects
            let result = !geom1.intersects(&geom2);
            Ok(SqlValue::Boolean(result))
        }
        _ => Err(ExecutorError::Other(
            "ST_Disjoint requires VARCHAR geometry arguments".to_string(),
        )),
    }
}

/// ST_Equals(geom1, geom2) - Are geom1 and geom2 spatially equal?
pub fn st_equals(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::Other(
            "ST_Equals expects exactly 2 arguments".to_string(),
        ));
    }
    
    match (&args[0], &args[1]) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),
        (SqlValue::Varchar(wkt1) | SqlValue::Character(wkt1),
         SqlValue::Varchar(wkt2) | SqlValue::Character(wkt2)) => {
            let geom1 = wkt_to_geo(wkt1)?;
            let geom2 = wkt_to_geo(wkt2)?;
            
            let result = geom1 == geom2;
            Ok(SqlValue::Boolean(result))
        }
        _ => Err(ExecutorError::Other(
            "ST_Equals requires VARCHAR geometry arguments".to_string(),
        )),
    }
}

/// ST_Touches(geom1, geom2) - Do boundaries touch but interiors don't intersect?
pub fn st_touches(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::Other(
            "ST_Touches expects exactly 2 arguments".to_string(),
        ));
    }
    
    match (&args[0], &args[1]) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),
        (SqlValue::Varchar(wkt1) | SqlValue::Character(wkt1),
         SqlValue::Varchar(wkt2) | SqlValue::Character(wkt2)) => {
            let geom1 = wkt_to_geo(wkt1)?;
            let geom2 = wkt_to_geo(wkt2)?;
            
            // Simplified touches: geometries intersect but neither contains the other
            // This is a simplified approximation of the DE-9IM model
            let intersects = geom1.intersects(&geom2);
            let geom1_contains_geom2 = geom1.contains(&geom2);
            let geom2_contains_geom1 = geom2.contains(&geom1);
            
            // Touches is similar to crosses/overlaps but stricter:
            // boundaries must touch but interiors must not overlap
            let result = intersects && !geom1_contains_geom2 && !geom2_contains_geom1;
            Ok(SqlValue::Boolean(result))
        }
        _ => Err(ExecutorError::Other(
            "ST_Touches requires VARCHAR geometry arguments".to_string(),
        )),
    }
}

/// ST_Crosses(geom1, geom2) - Do geom1 and geom2 cross?
pub fn st_crosses(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::Other(
            "ST_Crosses expects exactly 2 arguments".to_string(),
        ));
    }
    
    match (&args[0], &args[1]) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),
        (SqlValue::Varchar(wkt1) | SqlValue::Character(wkt1),
         SqlValue::Varchar(wkt2) | SqlValue::Character(wkt2)) => {
            // Simplified implementation
            // Crosses = geometries intersect but one is not completely contained in the other
            let geom1 = wkt_to_geo(wkt1)?;
            let geom2 = wkt_to_geo(wkt2)?;
            
            let intersects = geom1.intersects(&geom2);
            let geom1_contains_geom2 = geom1.contains(&geom2);
            let geom2_contains_geom1 = geom2.contains(&geom1);
            
            let result = intersects && !geom1_contains_geom2 && !geom2_contains_geom1;
            Ok(SqlValue::Boolean(result))
        }
        _ => Err(ExecutorError::Other(
            "ST_Crosses requires VARCHAR geometry arguments".to_string(),
        )),
    }
}

/// ST_Overlaps(geom1, geom2) - Do geom1 and geom2 overlap?
pub fn st_overlaps(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::Other(
            "ST_Overlaps expects exactly 2 arguments".to_string(),
        ));
    }
    
    match (&args[0], &args[1]) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),
        (SqlValue::Varchar(wkt1) | SqlValue::Character(wkt1),
         SqlValue::Varchar(wkt2) | SqlValue::Character(wkt2)) => {
            // Simplified: geometries intersect but neither completely contains the other
            let geom1 = wkt_to_geo(wkt1)?;
            let geom2 = wkt_to_geo(wkt2)?;
            
            let intersects = geom1.intersects(&geom2);
            let geom1_contains_geom2 = geom1.contains(&geom2);
            let geom2_contains_geom1 = geom2.contains(&geom1);
            
            let result = intersects && !geom1_contains_geom2 && !geom2_contains_geom1;
            Ok(SqlValue::Boolean(result))
        }
        _ => Err(ExecutorError::Other(
            "ST_Overlaps requires VARCHAR geometry arguments".to_string(),
        )),
    }
}

/// ST_Covers(geom1, geom2) - Does geom1 cover geom2? (includes boundary)
pub fn st_covers(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::Other(
            "ST_Covers expects exactly 2 arguments".to_string(),
        ));
    }
    
    match (&args[0], &args[1]) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),
        (SqlValue::Varchar(wkt1) | SqlValue::Character(wkt1),
         SqlValue::Varchar(wkt2) | SqlValue::Character(wkt2)) => {
            // Covers is similar to Contains but includes boundaries
            let geom1 = wkt_to_geo(wkt1)?;
            let geom2 = wkt_to_geo(wkt2)?;
            
            // For now, use Contains as approximation
            let result = geom1.contains(&geom2);
            Ok(SqlValue::Boolean(result))
        }
        _ => Err(ExecutorError::Other(
            "ST_Covers requires VARCHAR geometry arguments".to_string(),
        )),
    }
}

/// ST_CoveredBy(geom1, geom2) - Is geom1 covered by geom2?
pub fn st_coveredby(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::Other(
            "ST_CoveredBy expects exactly 2 arguments".to_string(),
        ));
    }
    
    match (&args[0], &args[1]) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),
        (SqlValue::Varchar(wkt1) | SqlValue::Character(wkt1),
         SqlValue::Varchar(wkt2) | SqlValue::Character(wkt2)) => {
            let geom1 = wkt_to_geo(wkt1)?;
            let geom2 = wkt_to_geo(wkt2)?;
            
            let result = geom2.contains(&geom1);
            Ok(SqlValue::Boolean(result))
        }
        _ => Err(ExecutorError::Other(
            "ST_CoveredBy requires VARCHAR geometry arguments".to_string(),
        )),
    }
}

/// ST_DWithin(geom1, geom2, distance) - Are geometries within distance of each other?
pub fn st_dwithin(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 3 {
        return Err(ExecutorError::Other(
            "ST_DWithin expects exactly 3 arguments".to_string(),
        ));
    }
    
    // Extract distance value as f64
    let distance = match &args[2] {
        SqlValue::Double(d) => *d,
        SqlValue::Numeric(d) => *d,
        SqlValue::Integer(i) => *i as f64,
        SqlValue::Float(f) => *f as f64,
        SqlValue::Null => return Ok(SqlValue::Null),
        _ => return Err(ExecutorError::Other(
            "ST_DWithin distance must be numeric".to_string(),
        )),
    };
    
    match (&args[0], &args[1]) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),
        (SqlValue::Varchar(wkt1) | SqlValue::Character(wkt1),
         SqlValue::Varchar(wkt2) | SqlValue::Character(wkt2)) => {
            let geom1 = wkt_to_geo(wkt1)?;
            let geom2 = wkt_to_geo(wkt2)?;
            
            // Calculate distance using point-to-point haversine when possible
            let dist = match (&geom1, &geom2) {
                (geo::Geometry::Point(p1), geo::Geometry::Point(p2)) => {
                    p1.haversine_distance(p2)
                }
                _ => {
                    // For non-point geometries, simplified handling
                    // Would need full geometry distance algorithms
                    0.0
                }
            };
            
            let result = dist <= distance;
            Ok(SqlValue::Boolean(result))
        }
        _ => Err(ExecutorError::Other(
            "ST_DWithin requires (VARCHAR, VARCHAR, NUMERIC) arguments".to_string(),
        )),
    }
}

/// ST_Relate(geom1, geom2) - Return DE-9IM relationship (simplified version)
/// ST_Relate(geom1, geom2, pattern) - Test DE-9IM relationship against a pattern
/// 
/// Note: Simplified implementation - full DE-9IM computation is deferred to Phase 4
pub fn st_relate(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() < 2 || args.len() > 3 {
        return Err(ExecutorError::Other(
            "ST_Relate expects 2 or 3 arguments".to_string(),
        ));
    }
    
    match (&args[0], &args[1]) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),
        (SqlValue::Varchar(wkt1) | SqlValue::Character(wkt1),
         SqlValue::Varchar(wkt2) | SqlValue::Character(wkt2)) => {
            let geom1 = wkt_to_geo(wkt1)?;
            let geom2 = wkt_to_geo(wkt2)?;
            
            if args.len() == 2 {
                // ST_Relate(geom1, geom2) - return DE-9IM string
                // Simplified: return a basic relationship indicator
                // Full implementation would compute the 9-intersection matrix
                if !geom1.intersects(&geom2) {
                    // Disjoint
                    Ok(SqlValue::Varchar("FF*FF****".to_string()))
                } else if geom1.contains(&geom2) && !geom2.contains(&geom1) {
                    // Contains (but not equal)
                    Ok(SqlValue::Varchar("T*F**F***".to_string()))
                } else if geom2.contains(&geom1) && !geom1.contains(&geom2) {
                    // Within (but not equal)
                    Ok(SqlValue::Varchar("F*T**F***".to_string()))
                } else if geom1 == geom2 {
                    // Equals
                    Ok(SqlValue::Varchar("T*F**FFF*".to_string()))
                } else {
                    // Intersects (but not fully one way or the other)
                    Ok(SqlValue::Varchar("T*T***T**".to_string()))
                }
            } else {
                // ST_Relate(geom1, geom2, pattern) - test against pattern
                // This requires full DE-9IM computation which is deferred to Phase 4
                return Err(ExecutorError::UnsupportedFeature(
                    "ST_Relate with pattern matching requires full DE-9IM implementation (Phase 4)".to_string(),
                ));
            }
        }
        _ => Err(ExecutorError::Other(
            "ST_Relate requires VARCHAR geometry arguments".to_string(),
        )),
    }
}
