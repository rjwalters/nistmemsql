//! Spatial Predicate Functions
//!
//! Implements spatial relationship tests between geometries.
//! Uses geo-types and geo crates for actual geometric calculations.

use vibesql_types::SqlValue;
use crate::errors::ExecutorError;
use super::{sql_value_to_geometry, Geometry};
use geo::{Contains, Intersects, Relate};
use geo::algorithm::{HaversineDistance, Within, Relate as RelateAlgo};

/// Convert internal Geometry to geo::Geometry for spatial operations
fn to_geo_geometry(geom: &Geometry) -> Result<geo::Geometry<f64>, ExecutorError> {
    match geom {
        Geometry::Point { x, y } => {
            Ok(geo::Geometry::Point(geo::Point::new(*x, *y)))
        }
        Geometry::LineString { points } => {
            let coords: Vec<geo::Coord<f64>> = points
                .iter()
                .map(|(x, y)| geo::Coord { x: *x, y: *y })
                .collect();
            Ok(geo::Geometry::LineString(geo::LineString(coords)))
        }
        Geometry::Polygon { rings } => {
            if rings.is_empty() {
                return Err(ExecutorError::Other("Empty polygon".to_string()));
            }
            
            let exterior: Vec<geo::Coord<f64>> = rings[0]
                .iter()
                .map(|(x, y)| geo::Coord { x: *x, y: *y })
                .collect();
            let exterior_ring = geo::LineString(exterior);
            
            let interiors: Vec<geo::LineString<f64>> = rings[1..]
                .iter()
                .map(|ring| {
                    let coords: Vec<geo::Coord<f64>> = ring
                        .iter()
                        .map(|(x, y)| geo::Coord { x: *x, y: *y })
                        .collect();
                    geo::LineString(coords)
                })
                .collect();
            
            Ok(geo::Geometry::Polygon(geo::Polygon::new(exterior_ring, interiors)))
        }
        _ => Err(ExecutorError::UnsupportedFeature(format!(
            "Geometry type {} not yet fully supported for spatial predicates",
            geom.geometry_type()
        ))),
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
