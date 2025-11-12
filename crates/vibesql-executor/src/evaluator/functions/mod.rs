//! SQL Function Implementations
//!
//! This module contains all scalar SQL function implementations, organized by category:
//!
//! - `null_handling`: NULL operations (COALESCE, NULLIF)
//! - `string`: String manipulation (UPPER, SUBSTR, CONCAT, etc.)
//! - `numeric`: Mathematical operations (ABS, ROUND, POWER, SIN, etc.)
//! - `datetime`: Date/time operations (CURRENT_DATE, YEAR, DATE_ADD, etc.)
//! - `control`: Control flow (IF)
//! - `spatial`: Spatial/geometric functions (ST_GeomFromText, ST_Contains, ST_Intersects, etc.)
//!
//! ## Usage
//!
//! The main entry point is `eval_scalar_function()`, which dispatches to the
//! appropriate module based on the function name.

use crate::errors::ExecutorError;

// Module declarations
mod control;
mod conversion;
mod datetime;
mod null_handling;
mod numeric;
pub(crate) mod string;
mod system;
mod spatial;

/// Evaluate a scalar function on given argument values
///
/// This handles SQL scalar functions that don't depend on table schemas
/// (unlike aggregates like COUNT, SUM which are handled elsewhere).
pub(super) fn eval_scalar_function(
    name: &str,
    args: &[vibesql_types::SqlValue],
    character_unit: &Option<vibesql_ast::CharacterUnit>,
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    match name.to_uppercase().as_str() {
        // NULL handling functions
        "COALESCE" => null_handling::coalesce(args),
        "NULLIF" => null_handling::nullif(args),

        // String functions
        "UPPER" => string::upper(args),
        "LOWER" => string::lower(args),
        "SUBSTRING" => string::substring(args),
        "SUBSTR" => string::substring(args), // Alias for SUBSTRING
        // Note: TRIM is handled as a special expression in the parser (like POSITION)
        "CHAR_LENGTH" | "CHARACTER_LENGTH" => string::char_length(args, name, character_unit),
        "OCTET_LENGTH" => string::octet_length(args),
        "CONCAT" => string::concat(args),
        "LENGTH" => string::length(args),
        "POSITION" => string::position(args),
        "REPLACE" => string::replace(args),
        "REVERSE" => string::reverse(args),
        "LEFT" => string::left(args),
        "RIGHT" => string::right(args),
        "INSTR" => string::instr(args),
        "LOCATE" => string::locate(args),

        // Numeric functions
        "ABS" => numeric::abs(args),
        "ROUND" => numeric::round(args),
        "TRUNCATE" => numeric::truncate(args),
        "FLOOR" => numeric::floor(args),
        "CEIL" | "CEILING" => numeric::ceil(args),
        "MOD" => numeric::mod_func(args),
        "POWER" | "POW" => numeric::power(args),
        "SQRT" => numeric::sqrt(args),
        "EXP" => numeric::exp(args),
        "LN" | "LOG" => numeric::ln(args),
        "LOG10" => numeric::log10(args),
        "SIGN" => numeric::sign(args),
        "PI" => numeric::pi(args),
        "SIN" => numeric::sin(args),
        "COS" => numeric::cos(args),
        "TAN" => numeric::tan(args),
        "ASIN" => numeric::asin(args),
        "ACOS" => numeric::acos(args),
        "ATAN" => numeric::atan(args),
        "ATAN2" => numeric::atan2(args),
        "RADIANS" => numeric::radians(args),
        "DEGREES" => numeric::degrees(args),
        "GREATEST" => numeric::greatest(args),
        "LEAST" => numeric::least(args),
        "FORMAT" => numeric::format(args),

        // Date/time functions
        "CURRENT_DATE" | "CURDATE" => datetime::current_date(args),
        "CURRENT_TIME" | "CURTIME" => datetime::current_time(args),
        "CURRENT_TIMESTAMP" | "NOW" => datetime::current_timestamp(args),
        "YEAR" => datetime::year(args),
        "MONTH" => datetime::month(args),
        "DAY" => datetime::day(args),
        "HOUR" => datetime::hour(args),
        "MINUTE" => datetime::minute(args),
        "SECOND" => datetime::second(args),
        "DATEDIFF" => datetime::datediff(args),
        "DATE_ADD" | "ADDDATE" => datetime::date_add(args),
        "DATE_SUB" | "SUBDATE" => datetime::date_sub(args),
        "EXTRACT" => datetime::extract(args),
        "AGE" => datetime::age(args),

        // Control flow functions
        "IF" => control::if_func(args),

        // Type conversion functions
        "TO_NUMBER" => conversion::to_number(args),
        "TO_DATE" => conversion::to_date(args),
        "TO_TIMESTAMP" => conversion::to_timestamp(args),
        "TO_CHAR" => conversion::to_char(args),
        "CAST" => conversion::cast(args),

        // System information functions
        "VERSION" => system::version(args),
        "DATABASE" | "SCHEMA" => system::database(args, name),
        "USER" | "CURRENT_USER" => system::user(args, name),

        // Spatial/Geometric functions
        // Constructor functions - WKT (Phase 1)
        "ST_GEOMFROMTEXT" | "ST_GEOM_FROM_TEXT" => spatial::constructors::st_geom_from_text(args),
        "ST_POINTFROMTEXT" | "ST_POINT_FROM_TEXT" => spatial::constructors::st_point_from_text(args),
        "ST_LINEFROMTEXT" | "ST_LINE_FROM_TEXT" => spatial::constructors::st_line_from_text(args),
        "ST_POLYGONFROMTEXT" | "ST_POLYGON_FROM_TEXT" => spatial::constructors::st_polygon_from_text(args),

        // Constructor functions - WKB (Phase 2)
        "ST_GEOMFROMWKB" | "ST_GEOM_FROM_WKB" => spatial::constructors::st_geom_from_wkb(args),
        "ST_POINTFROMWKB" | "ST_POINT_FROM_WKB" => spatial::constructors::st_point_from_wkb(args),
        "ST_LINEFROMWKB" | "ST_LINE_FROM_WKB" => spatial::constructors::st_line_from_wkb(args),
        "ST_POLYGONFROMWKB" | "ST_POLYGON_FROM_WKB" => spatial::constructors::st_polygon_from_wkb(args),

        // Accessor functions
        "ST_X" => spatial::accessors::st_x(args),
        "ST_Y" => spatial::accessors::st_y(args),
        "ST_GEOMETRYTYPE" | "ST_GEOMETRY_TYPE" => spatial::accessors::st_geometry_type(args),
        "ST_DIMENSION" => spatial::accessors::st_dimension(args),
        "ST_ASTEXT" | "ST_AS_TEXT" => spatial::accessors::st_as_text(args),
        "ST_ASBINARY" | "ST_AS_BINARY" => spatial::accessors::st_as_binary(args),
        "ST_ASGEOJSON" | "ST_AS_GEOJSON" => spatial::accessors::st_as_geojson(args),

        // SRID functions (Phase 2)
        "ST_SETSRID" | "ST_SET_SRID" => spatial::srid::st_set_srid(args),
        "ST_SRID" => spatial::srid::st_srid(args),

        // Spatial predicates
        "ST_CONTAINS" => spatial::predicates::st_contains(args),
        "ST_WITHIN" => spatial::predicates::st_within(args),
        "ST_INTERSECTS" => spatial::predicates::st_intersects(args),
        "ST_DISJOINT" => spatial::predicates::st_disjoint(args),
        "ST_EQUALS" => spatial::predicates::st_equals(args),
        "ST_TOUCHES" => spatial::predicates::st_touches(args),
        "ST_CROSSES" => spatial::predicates::st_crosses(args),
        "ST_OVERLAPS" => spatial::predicates::st_overlaps(args),
        "ST_COVERS" => spatial::predicates::st_covers(args),
        "ST_COVEREDBY" | "ST_COVERED_BY" => spatial::predicates::st_coveredby(args),
        "ST_DWITHIN" | "ST_D_WITHIN" => spatial::predicates::st_dwithin(args),
        "ST_RELATE" => spatial::predicates::st_relate(args),

        // Unknown function
        _ => Err(ExecutorError::UnsupportedFeature(format!("Unknown function: {}", name))),
    }
}
