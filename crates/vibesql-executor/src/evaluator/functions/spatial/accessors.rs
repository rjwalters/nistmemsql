//! Spatial accessor functions
//!
//! Extract coordinates and metadata from geometries.

use super::{Geometry, sql_value_to_geometry};
use vibesql_types::SqlValue;
use crate::errors::ExecutorError;

/// ST_X(point) - Get X coordinate from POINT
pub fn st_x(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "ST_X requires exactly 1 argument, got {}",
            args.len()
        )));
    }
    
    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        _ => {
            let geom = sql_value_to_geometry(&args[0])?;
            match geom {
                Geometry::Point { x, .. } => Ok(SqlValue::Double(x)),
                _ => Err(ExecutorError::UnsupportedFeature(
                    "ST_X requires a POINT geometry".to_string(),
                )),
            }
        }
    }
}

/// ST_Y(point) - Get Y coordinate from POINT
pub fn st_y(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "ST_Y requires exactly 1 argument, got {}",
            args.len()
        )));
    }
    
    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        _ => {
            let geom = sql_value_to_geometry(&args[0])?;
            match geom {
                Geometry::Point { y, .. } => Ok(SqlValue::Double(y)),
                _ => Err(ExecutorError::UnsupportedFeature(
                    "ST_Y requires a POINT geometry".to_string(),
                )),
            }
        }
    }
}

/// ST_GeometryType(geom) - Get geometry type name
pub fn st_geometry_type(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "ST_GeometryType requires exactly 1 argument, got {}",
            args.len()
        )));
    }
    
    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        _ => {
            let geom = sql_value_to_geometry(&args[0])?;
            Ok(SqlValue::Varchar(geom.geometry_type().to_string()))
        }
    }
}

/// ST_Dimension(geom) - Get dimension (0=point, 1=line, 2=polygon)
pub fn st_dimension(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "ST_Dimension requires exactly 1 argument, got {}",
            args.len()
        )));
    }
    
    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        _ => {
            let geom = sql_value_to_geometry(&args[0])?;
            Ok(SqlValue::Integer(geom.dimension() as i64))
        }
    }
}

/// ST_SRID(geom) - Get Spatial Reference System ID
/// Note: Currently always returns 0 as we don't track SRID
pub fn st_srid(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "ST_SRID requires exactly 1 argument, got {}",
            args.len()
        )));
    }
    
    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        _ => {
            let _geom = sql_value_to_geometry(&args[0])?;
            // For now, always return 0 as we don't track SRID
            Ok(SqlValue::Integer(0))
        }
    }
}

/// ST_AsText(geom) - Convert geometry to WKT (Well-Known Text)
pub fn st_as_text(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "ST_AsText requires exactly 1 argument, got {}",
            args.len()
        )));
    }
    
    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        _ => {
            let geom = sql_value_to_geometry(&args[0])?;
            Ok(SqlValue::Varchar(geom.to_wkt()))
        }
    }
}

/// ST_AsBinary(geom) - Convert geometry to WKB (Well-Known Binary)
/// Note: Currently returns a placeholder as WKB is complex
pub fn st_as_binary(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "ST_AsBinary requires exactly 1 argument, got {}",
            args.len()
        )));
    }
    
    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        _ => {
            let _geom = sql_value_to_geometry(&args[0])?;
            // For Phase 1, return a placeholder since WKB encoding is complex
            // This would be implemented in Phase 2 with proper WKB encoding
            Err(ExecutorError::UnsupportedFeature(
                "ST_AsBinary is not yet fully implemented".to_string(),
            ))
        }
    }
}

/// ST_AsGeoJSON(geom) - Convert geometry to GeoJSON
pub fn st_as_geojson(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "ST_AsGeoJSON requires exactly 1 argument, got {}",
            args.len()
        )));
    }
    
    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        _ => {
            let geom = sql_value_to_geometry(&args[0])?;
            let geojson = geometry_to_geojson(&geom);
            Ok(SqlValue::Varchar(geojson))
        }
    }
}

/// Convert geometry to simple GeoJSON representation
fn geometry_to_geojson(geom: &Geometry) -> String {
    match geom {
        Geometry::Point { x, y } => {
            format!(
                r#"{{"type":"Point","coordinates":[{},{}]}}"#,
                x, y
            )
        }
        Geometry::LineString { points } => {
            let coords = points
                .iter()
                .map(|(x, y)| format!("[{},{}]", x, y))
                .collect::<Vec<_>>()
                .join(",");
            format!(r#"{{"type":"LineString","coordinates":[{}]}}"#, coords)
        }
        Geometry::Polygon { rings } => {
            let rings_str = rings
                .iter()
                .map(|ring| {
                    let coords = ring
                        .iter()
                        .map(|(x, y)| format!("[{},{}]", x, y))
                        .collect::<Vec<_>>()
                        .join(",");
                    format!("[{}]", coords)
                })
                .collect::<Vec<_>>()
                .join(",");
            format!(r#"{{"type":"Polygon","coordinates":[{}]}}"#, rings_str)
        }
        Geometry::MultiPoint { points } => {
            let coords = points
                .iter()
                .map(|(x, y)| format!("[{},{}]", x, y))
                .collect::<Vec<_>>()
                .join(",");
            format!(r#"{{"type":"MultiPoint","coordinates":[{}]}}"#, coords)
        }
        Geometry::MultiLineString { lines } => {
            let lines_str = lines
                .iter()
                .map(|line| {
                    let coords = line
                        .iter()
                        .map(|(x, y)| format!("[{},{}]", x, y))
                        .collect::<Vec<_>>()
                        .join(",");
                    format!("[{}]", coords)
                })
                .collect::<Vec<_>>()
                .join(",");
            format!(r#"{{"type":"MultiLineString","coordinates":[{}]}}"#, lines_str)
        }
        Geometry::MultiPolygon { polygons } => {
            let polys_str = polygons
                .iter()
                .map(|poly| {
                    let rings_str = poly
                        .iter()
                        .map(|ring| {
                            let coords = ring
                                .iter()
                                .map(|(x, y)| format!("[{},{}]", x, y))
                                .collect::<Vec<_>>()
                                .join(",");
                            format!("[{}]", coords)
                        })
                        .collect::<Vec<_>>()
                        .join(",");
                    format!("[{}]", rings_str)
                })
                .collect::<Vec<_>>()
                .join(",");
            format!(r#"{{"type":"MultiPolygon","coordinates":[{}]}}"#, polys_str)
        }
        Geometry::GeometryCollection { geometries } => {
            let geoms = geometries
                .iter()
                .map(|g| {
                    // Extract just the geometry object without the full GeoJSON wrapper
                    let json = geometry_to_geojson(g);
                    json
                })
                .collect::<Vec<_>>()
                .join(",");
            format!(r#"{{"type":"GeometryCollection","geometries":[{}]}}"#, geoms)
        }
    }
}
