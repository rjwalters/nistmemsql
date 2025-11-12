//! Spatial/Geometric Query Functions
//!
//! Implements ST_* operations for working with spatial data types.
//! Currently supports WKT (Well-Known Text) parsing and basic geometry operations.
//!
//! Functions are organized as:
//! - Constructor functions: Create geometry from text/binary
//! - Accessor functions: Extract coordinates and metadata
//! - Relationship functions: Spatial predicates
//! - Measurement functions: Distance and area calculations
//! - Output functions: Convert to text/binary formats

pub mod constructors;
pub mod accessors;

use vibesql_types::SqlValue;
use crate::errors::ExecutorError;

/// A simple geometry representation for Phase 1
/// Stores geometries as WKT strings internally
#[derive(Debug, Clone, PartialEq)]
pub enum Geometry {
    Point { x: f64, y: f64 },
    LineString { points: Vec<(f64, f64)> },
    Polygon { rings: Vec<Vec<(f64, f64)>> },
    MultiPoint { points: Vec<(f64, f64)> },
    MultiLineString { lines: Vec<Vec<(f64, f64)>> },
    MultiPolygon { polygons: Vec<Vec<Vec<(f64, f64)>>> },
    GeometryCollection { geometries: Vec<Geometry> },
}

impl Geometry {
    /// Get the type name
    pub fn geometry_type(&self) -> &'static str {
        match self {
            Geometry::Point { .. } => "POINT",
            Geometry::LineString { .. } => "LINESTRING",
            Geometry::Polygon { .. } => "POLYGON",
            Geometry::MultiPoint { .. } => "MULTIPOINT",
            Geometry::MultiLineString { .. } => "MULTILINESTRING",
            Geometry::MultiPolygon { .. } => "MULTIPOLYGON",
            Geometry::GeometryCollection { .. } => "GEOMETRYCOLLECTION",
        }
    }

    /// Get the dimension (0 for point, 1 for line, 2 for polygon)
    pub fn dimension(&self) -> i32 {
        match self {
            Geometry::Point { .. } | Geometry::MultiPoint { .. } => 0,
            Geometry::LineString { .. } | Geometry::MultiLineString { .. } => 1,
            Geometry::Polygon { .. } | Geometry::MultiPolygon { .. } => 2,
            Geometry::GeometryCollection { geometries } => {
                geometries.iter().map(|g| g.dimension()).max().unwrap_or(-1)
            }
        }
    }

    /// Convert to WKT string
    pub fn to_wkt(&self) -> String {
        match self {
            Geometry::Point { x, y } => {
                format!("POINT({} {})", x, y)
            }
            Geometry::LineString { points } => {
                let coords = points
                    .iter()
                    .map(|(x, y)| format!("{} {}", x, y))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("LINESTRING({})", coords)
            }
            Geometry::Polygon { rings } => {
                let ring_strs = rings
                    .iter()
                    .map(|ring| {
                        let coords = ring
                            .iter()
                            .map(|(x, y)| format!("{} {}", x, y))
                            .collect::<Vec<_>>()
                            .join(", ");
                        format!("({})", coords)
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("POLYGON({})", ring_strs)
            }
            Geometry::MultiPoint { points } => {
                let coords = points
                    .iter()
                    .map(|(x, y)| format!("({} {})", x, y))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("MULTIPOINT({})", coords)
            }
            Geometry::MultiLineString { lines } => {
                let line_strs = lines
                    .iter()
                    .map(|line| {
                        let coords = line
                            .iter()
                            .map(|(x, y)| format!("{} {}", x, y))
                            .collect::<Vec<_>>()
                            .join(", ");
                        format!("({})", coords)
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("MULTILINESTRING({})", line_strs)
            }
            Geometry::MultiPolygon { polygons } => {
                let poly_strs = polygons
                    .iter()
                    .map(|poly| {
                        let ring_strs = poly
                            .iter()
                            .map(|ring| {
                                let coords = ring
                                    .iter()
                                    .map(|(x, y)| format!("{} {}", x, y))
                                    .collect::<Vec<_>>()
                                    .join(", ");
                                format!("({})", coords)
                            })
                            .collect::<Vec<_>>()
                            .join(", ");
                        format!("({})", ring_strs)
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("MULTIPOLYGON({})", poly_strs)
            }
            Geometry::GeometryCollection { geometries } => {
                let geo_strs = geometries
                    .iter()
                    .map(|g| g.to_wkt())
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("GEOMETRYCOLLECTION({})", geo_strs)
            }
        }
    }
}

/// Store geometry as a serialized WKT string in SqlValue
pub const GEOMETRY_TYPE_MARKER: &str = "__GEOMETRY__";

pub fn geometry_to_sql_value(geom: Geometry) -> SqlValue {
    let wkt = geom.to_wkt();
    SqlValue::Varchar(format!("{}{}", GEOMETRY_TYPE_MARKER, wkt))
}

pub fn sql_value_to_geometry(value: &SqlValue) -> Result<Geometry, ExecutorError> {
    match value {
        SqlValue::Varchar(s) | SqlValue::Character(s) => {
            if let Some(wkt) = s.strip_prefix(GEOMETRY_TYPE_MARKER) {
                constructors::parse_wkt(wkt)
            } else {
                // Try to parse as WKT directly if not marked
                constructors::parse_wkt(s)
            }
        }
        SqlValue::Null => Err(ExecutorError::UnsupportedFeature(
            "Cannot convert NULL to geometry".to_string(),
        )),
        _ => Err(ExecutorError::UnsupportedFeature(format!(
            "Cannot convert {} to geometry",
            value.type_name()
        ))),
    }
}
