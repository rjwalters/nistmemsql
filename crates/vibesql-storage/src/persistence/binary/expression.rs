// ============================================================================
// Expression Serialization
// ============================================================================
//
// Handles serialization and deserialization of SQL expression ASTs.
//
// Uses a tag-based approach where each Expression variant gets a unique u8
// discriminant, followed by the variant-specific data.

use std::io::{Read, Write};

use vibesql_ast::{
    BinaryOperator, CaseWhen, CharacterUnit, Expression, FrameBound, FrameUnit, FulltextMode,
    IntervalUnit, PseudoTable, TrimPosition, UnaryOperator, WindowFrame, WindowFunctionSpec,
    WindowSpec,
};

use super::{io::*, value::*};
use crate::StorageError;

/// Expression variant tags for serialization
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExprTag {
    Literal = 0x00,
    ColumnRef = 0x01,
    BinaryOp = 0x02,
    UnaryOp = 0x03,
    Function = 0x04,
    AggregateFunction = 0x05,
    IsNull = 0x06,
    Wildcard = 0x07,
    Case = 0x08,
    ScalarSubquery = 0x09,          // Not fully supported - returns error
    In = 0x0A,                       // Not fully supported - returns error
    InList = 0x0B,
    Between = 0x0C,
    Cast = 0x0D,
    Position = 0x0E,
    Trim = 0x0F,
    Like = 0x10,
    Exists = 0x11,                   // Not fully supported - returns error
    QuantifiedComparison = 0x12,     // Not fully supported - returns error
    CurrentDate = 0x13,
    CurrentTime = 0x14,
    CurrentTimestamp = 0x15,
    Interval = 0x16,
    Default = 0x17,
    DuplicateKeyValue = 0x18,
    WindowFunction = 0x19,
    NextValue = 0x1A,
    MatchAgainst = 0x1B,
    PseudoVariable = 0x1C,
    SessionVariable = 0x1D,
}

impl ExprTag {
    fn from_u8(tag: u8) -> Result<Self, StorageError> {
        match tag {
            0x00 => Ok(ExprTag::Literal),
            0x01 => Ok(ExprTag::ColumnRef),
            0x02 => Ok(ExprTag::BinaryOp),
            0x03 => Ok(ExprTag::UnaryOp),
            0x04 => Ok(ExprTag::Function),
            0x05 => Ok(ExprTag::AggregateFunction),
            0x06 => Ok(ExprTag::IsNull),
            0x07 => Ok(ExprTag::Wildcard),
            0x08 => Ok(ExprTag::Case),
            0x09 => Ok(ExprTag::ScalarSubquery),
            0x0A => Ok(ExprTag::In),
            0x0B => Ok(ExprTag::InList),
            0x0C => Ok(ExprTag::Between),
            0x0D => Ok(ExprTag::Cast),
            0x0E => Ok(ExprTag::Position),
            0x0F => Ok(ExprTag::Trim),
            0x10 => Ok(ExprTag::Like),
            0x11 => Ok(ExprTag::Exists),
            0x12 => Ok(ExprTag::QuantifiedComparison),
            0x13 => Ok(ExprTag::CurrentDate),
            0x14 => Ok(ExprTag::CurrentTime),
            0x15 => Ok(ExprTag::CurrentTimestamp),
            0x16 => Ok(ExprTag::Interval),
            0x17 => Ok(ExprTag::Default),
            0x18 => Ok(ExprTag::DuplicateKeyValue),
            0x19 => Ok(ExprTag::WindowFunction),
            0x1A => Ok(ExprTag::NextValue),
            0x1B => Ok(ExprTag::MatchAgainst),
            0x1C => Ok(ExprTag::PseudoVariable),
            0x1D => Ok(ExprTag::SessionVariable),
            _ => Err(StorageError::NotImplemented(format!(
                "Unknown expression tag: 0x{:02X}",
                tag
            ))),
        }
    }
}

pub fn write_expression<W: Write>(writer: &mut W, expr: &Expression) -> Result<(), StorageError> {
    match expr {
        Expression::Literal(value) => {
            writer
                .write_all(&[ExprTag::Literal as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_sql_value(writer, value)?;
        }
        Expression::ColumnRef { table, column } => {
            writer
                .write_all(&[ExprTag::ColumnRef as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            // Write optional table name
            write_bool(writer, table.is_some())?;
            if let Some(t) = table {
                write_string(writer, t)?;
            }
            write_string(writer, column)?;
        }
        Expression::BinaryOp { op, left, right } => {
            writer
                .write_all(&[ExprTag::BinaryOp as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_binary_operator(writer, *op)?;
            write_expression(writer, left)?;
            write_expression(writer, right)?;
        }
        Expression::UnaryOp { op, expr } => {
            writer
                .write_all(&[ExprTag::UnaryOp as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_unary_operator(writer, *op)?;
            write_expression(writer, expr)?;
        }
        Expression::Function {
            name,
            args,
            character_unit,
        } => {
            writer
                .write_all(&[ExprTag::Function as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_string(writer, name)?;
            write_u32(writer, args.len() as u32)?;
            for arg in args {
                write_expression(writer, arg)?;
            }
            write_bool(writer, character_unit.is_some())?;
            if let Some(unit) = character_unit {
                write_character_unit(writer, unit)?;
            }
        }
        Expression::AggregateFunction {
            name,
            distinct,
            args,
        } => {
            writer
                .write_all(&[ExprTag::AggregateFunction as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_string(writer, name)?;
            write_bool(writer, *distinct)?;
            write_u32(writer, args.len() as u32)?;
            for arg in args {
                write_expression(writer, arg)?;
            }
        }
        Expression::IsNull { expr, negated } => {
            writer
                .write_all(&[ExprTag::IsNull as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_expression(writer, expr)?;
            write_bool(writer, *negated)?;
        }
        Expression::Wildcard => {
            writer
                .write_all(&[ExprTag::Wildcard as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
        }
        Expression::Case {
            operand,
            when_clauses,
            else_result,
        } => {
            writer
                .write_all(&[ExprTag::Case as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            // Write optional operand
            write_bool(writer, operand.is_some())?;
            if let Some(op) = operand {
                write_expression(writer, op)?;
            }
            // Write when clauses
            write_u32(writer, when_clauses.len() as u32)?;
            for when_clause in when_clauses {
                write_case_when(writer, when_clause)?;
            }
            // Write optional else result
            write_bool(writer, else_result.is_some())?;
            if let Some(else_expr) = else_result {
                write_expression(writer, else_expr)?;
            }
        }
        Expression::ScalarSubquery(_) => {
            return Err(StorageError::NotImplemented(
                "ScalarSubquery serialization not yet implemented".to_string(),
            ));
        }
        Expression::In { .. } => {
            return Err(StorageError::NotImplemented(
                "IN with subquery serialization not yet implemented".to_string(),
            ));
        }
        Expression::InList {
            expr,
            values,
            negated,
        } => {
            writer
                .write_all(&[ExprTag::InList as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_expression(writer, expr)?;
            write_u32(writer, values.len() as u32)?;
            for value in values {
                write_expression(writer, value)?;
            }
            write_bool(writer, *negated)?;
        }
        Expression::Between {
            expr,
            low,
            high,
            negated,
            symmetric,
        } => {
            writer
                .write_all(&[ExprTag::Between as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_expression(writer, expr)?;
            write_expression(writer, low)?;
            write_expression(writer, high)?;
            write_bool(writer, *negated)?;
            write_bool(writer, *symmetric)?;
        }
        Expression::Cast { expr, data_type } => {
            writer
                .write_all(&[ExprTag::Cast as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_expression(writer, expr)?;
            // Serialize DataType as string (reuse existing format logic)
            let type_str = crate::persistence::save::format_data_type(data_type);
            write_string(writer, &type_str)?;
        }
        Expression::Position {
            substring,
            string,
            character_unit,
        } => {
            writer
                .write_all(&[ExprTag::Position as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_expression(writer, substring)?;
            write_expression(writer, string)?;
            write_bool(writer, character_unit.is_some())?;
            if let Some(unit) = character_unit {
                write_character_unit(writer, unit)?;
            }
        }
        Expression::Trim {
            position,
            removal_char,
            string,
        } => {
            writer
                .write_all(&[ExprTag::Trim as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_bool(writer, position.is_some())?;
            if let Some(pos) = position {
                write_trim_position(writer, pos)?;
            }
            write_bool(writer, removal_char.is_some())?;
            if let Some(char_expr) = removal_char {
                write_expression(writer, char_expr)?;
            }
            write_expression(writer, string)?;
        }
        Expression::Like {
            expr,
            pattern,
            negated,
        } => {
            writer
                .write_all(&[ExprTag::Like as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_expression(writer, expr)?;
            write_expression(writer, pattern)?;
            write_bool(writer, *negated)?;
        }
        Expression::Exists { .. } => {
            return Err(StorageError::NotImplemented(
                "EXISTS serialization not yet implemented".to_string(),
            ));
        }
        Expression::QuantifiedComparison { .. } => {
            return Err(StorageError::NotImplemented(
                "Quantified comparison serialization not yet implemented".to_string(),
            ));
        }
        Expression::CurrentDate => {
            writer
                .write_all(&[ExprTag::CurrentDate as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
        }
        Expression::CurrentTime { precision } => {
            writer
                .write_all(&[ExprTag::CurrentTime as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_bool(writer, precision.is_some())?;
            if let Some(p) = precision {
                write_u32(writer, *p)?;
            }
        }
        Expression::CurrentTimestamp { precision } => {
            writer
                .write_all(&[ExprTag::CurrentTimestamp as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_bool(writer, precision.is_some())?;
            if let Some(p) = precision {
                write_u32(writer, *p)?;
            }
        }
        Expression::Interval {
            value,
            unit,
            leading_precision,
            fractional_precision,
        } => {
            writer
                .write_all(&[ExprTag::Interval as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_expression(writer, value)?;
            write_interval_unit(writer, unit)?;
            write_bool(writer, leading_precision.is_some())?;
            if let Some(p) = leading_precision {
                write_u32(writer, *p)?;
            }
            write_bool(writer, fractional_precision.is_some())?;
            if let Some(p) = fractional_precision {
                write_u32(writer, *p)?;
            }
        }
        Expression::Default => {
            writer
                .write_all(&[ExprTag::Default as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
        }
        Expression::DuplicateKeyValue { column } => {
            writer
                .write_all(&[ExprTag::DuplicateKeyValue as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_string(writer, column)?;
        }
        Expression::WindowFunction { function, over } => {
            writer
                .write_all(&[ExprTag::WindowFunction as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_window_function_spec(writer, function)?;
            write_window_spec(writer, over)?;
        }
        Expression::NextValue { sequence_name } => {
            writer
                .write_all(&[ExprTag::NextValue as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_string(writer, sequence_name)?;
        }
        Expression::MatchAgainst {
            columns,
            search_modifier,
            mode,
        } => {
            writer
                .write_all(&[ExprTag::MatchAgainst as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_u32(writer, columns.len() as u32)?;
            for col in columns {
                write_string(writer, col)?;
            }
            write_expression(writer, search_modifier)?;
            write_fulltext_mode(writer, mode)?;
        }
        Expression::PseudoVariable {
            pseudo_table,
            column,
        } => {
            writer
                .write_all(&[ExprTag::PseudoVariable as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_pseudo_table(writer, *pseudo_table)?;
            write_string(writer, column)?;
        }
        Expression::SessionVariable { name } => {
            writer
                .write_all(&[ExprTag::SessionVariable as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_string(writer, name)?;
        }
    }
    Ok(())
}

pub fn read_expression<R: Read>(reader: &mut R) -> Result<Expression, StorageError> {
    let tag = read_u8(reader)?;
    let expr_tag = ExprTag::from_u8(tag)?;

    match expr_tag {
        ExprTag::Literal => {
            let value = read_sql_value(reader)?;
            Ok(Expression::Literal(value))
        }
        ExprTag::ColumnRef => {
            let has_table = read_bool(reader)?;
            let table = if has_table {
                Some(read_string(reader)?)
            } else {
                None
            };
            let column = read_string(reader)?;
            Ok(Expression::ColumnRef { table, column })
        }
        ExprTag::BinaryOp => {
            let op = read_binary_operator(reader)?;
            let left = Box::new(read_expression(reader)?);
            let right = Box::new(read_expression(reader)?);
            Ok(Expression::BinaryOp { op, left, right })
        }
        ExprTag::UnaryOp => {
            let op = read_unary_operator(reader)?;
            let expr = Box::new(read_expression(reader)?);
            Ok(Expression::UnaryOp { op, expr })
        }
        ExprTag::Function => {
            let name = read_string(reader)?;
            let arg_count = read_u32(reader)?;
            let mut args = Vec::new();
            for _ in 0..arg_count {
                args.push(read_expression(reader)?);
            }
            let has_unit = read_bool(reader)?;
            let character_unit = if has_unit {
                Some(read_character_unit(reader)?)
            } else {
                None
            };
            Ok(Expression::Function {
                name,
                args,
                character_unit,
            })
        }
        ExprTag::AggregateFunction => {
            let name = read_string(reader)?;
            let distinct = read_bool(reader)?;
            let arg_count = read_u32(reader)?;
            let mut args = Vec::new();
            for _ in 0..arg_count {
                args.push(read_expression(reader)?);
            }
            Ok(Expression::AggregateFunction {
                name,
                distinct,
                args,
            })
        }
        ExprTag::IsNull => {
            let expr = Box::new(read_expression(reader)?);
            let negated = read_bool(reader)?;
            Ok(Expression::IsNull { expr, negated })
        }
        ExprTag::Wildcard => Ok(Expression::Wildcard),
        ExprTag::Case => {
            let has_operand = read_bool(reader)?;
            let operand = if has_operand {
                Some(Box::new(read_expression(reader)?))
            } else {
                None
            };
            let when_count = read_u32(reader)?;
            let mut when_clauses = Vec::new();
            for _ in 0..when_count {
                when_clauses.push(read_case_when(reader)?);
            }
            let has_else = read_bool(reader)?;
            let else_result = if has_else {
                Some(Box::new(read_expression(reader)?))
            } else {
                None
            };
            Ok(Expression::Case {
                operand,
                when_clauses,
                else_result,
            })
        }
        ExprTag::ScalarSubquery => Err(StorageError::NotImplemented(
            "ScalarSubquery deserialization not yet implemented".to_string(),
        )),
        ExprTag::In => Err(StorageError::NotImplemented(
            "IN with subquery deserialization not yet implemented".to_string(),
        )),
        ExprTag::InList => {
            let expr = Box::new(read_expression(reader)?);
            let value_count = read_u32(reader)?;
            let mut values = Vec::new();
            for _ in 0..value_count {
                values.push(read_expression(reader)?);
            }
            let negated = read_bool(reader)?;
            Ok(Expression::InList {
                expr,
                values,
                negated,
            })
        }
        ExprTag::Between => {
            let expr = Box::new(read_expression(reader)?);
            let low = Box::new(read_expression(reader)?);
            let high = Box::new(read_expression(reader)?);
            let negated = read_bool(reader)?;
            let symmetric = read_bool(reader)?;
            Ok(Expression::Between {
                expr,
                low,
                high,
                negated,
                symmetric,
            })
        }
        ExprTag::Cast => {
            let expr = Box::new(read_expression(reader)?);
            let type_str = read_string(reader)?;
            // Parse DataType from string (reuse existing logic from catalog)
            let data_type = super::catalog::parse_data_type(&type_str)?;
            Ok(Expression::Cast { expr, data_type })
        }
        ExprTag::Position => {
            let substring = Box::new(read_expression(reader)?);
            let string = Box::new(read_expression(reader)?);
            let has_unit = read_bool(reader)?;
            let character_unit = if has_unit {
                Some(read_character_unit(reader)?)
            } else {
                None
            };
            Ok(Expression::Position {
                substring,
                string,
                character_unit,
            })
        }
        ExprTag::Trim => {
            let has_position = read_bool(reader)?;
            let position = if has_position {
                Some(read_trim_position(reader)?)
            } else {
                None
            };
            let has_removal_char = read_bool(reader)?;
            let removal_char = if has_removal_char {
                Some(Box::new(read_expression(reader)?))
            } else {
                None
            };
            let string = Box::new(read_expression(reader)?);
            Ok(Expression::Trim {
                position,
                removal_char,
                string,
            })
        }
        ExprTag::Like => {
            let expr = Box::new(read_expression(reader)?);
            let pattern = Box::new(read_expression(reader)?);
            let negated = read_bool(reader)?;
            Ok(Expression::Like {
                expr,
                pattern,
                negated,
            })
        }
        ExprTag::Exists => Err(StorageError::NotImplemented(
            "EXISTS deserialization not yet implemented".to_string(),
        )),
        ExprTag::QuantifiedComparison => Err(StorageError::NotImplemented(
            "Quantified comparison deserialization not yet implemented".to_string(),
        )),
        ExprTag::CurrentDate => Ok(Expression::CurrentDate),
        ExprTag::CurrentTime => {
            let has_precision = read_bool(reader)?;
            let precision = if has_precision {
                Some(read_u32(reader)?)
            } else {
                None
            };
            Ok(Expression::CurrentTime { precision })
        }
        ExprTag::CurrentTimestamp => {
            let has_precision = read_bool(reader)?;
            let precision = if has_precision {
                Some(read_u32(reader)?)
            } else {
                None
            };
            Ok(Expression::CurrentTimestamp { precision })
        }
        ExprTag::Interval => {
            let value = Box::new(read_expression(reader)?);
            let unit = read_interval_unit(reader)?;
            let has_leading_precision = read_bool(reader)?;
            let leading_precision = if has_leading_precision {
                Some(read_u32(reader)?)
            } else {
                None
            };
            let has_fractional_precision = read_bool(reader)?;
            let fractional_precision = if has_fractional_precision {
                Some(read_u32(reader)?)
            } else {
                None
            };
            Ok(Expression::Interval {
                value,
                unit,
                leading_precision,
                fractional_precision,
            })
        }
        ExprTag::Default => Ok(Expression::Default),
        ExprTag::DuplicateKeyValue => {
            let column = read_string(reader)?;
            Ok(Expression::DuplicateKeyValue { column })
        }
        ExprTag::WindowFunction => {
            let function = read_window_function_spec(reader)?;
            let over = read_window_spec(reader)?;
            Ok(Expression::WindowFunction { function, over })
        }
        ExprTag::NextValue => {
            let sequence_name = read_string(reader)?;
            Ok(Expression::NextValue { sequence_name })
        }
        ExprTag::MatchAgainst => {
            let col_count = read_u32(reader)?;
            let mut columns = Vec::new();
            for _ in 0..col_count {
                columns.push(read_string(reader)?);
            }
            let search_modifier = Box::new(read_expression(reader)?);
            let mode = read_fulltext_mode(reader)?;
            Ok(Expression::MatchAgainst {
                columns,
                search_modifier,
                mode,
            })
        }
        ExprTag::PseudoVariable => {
            let pseudo_table = read_pseudo_table(reader)?;
            let column = read_string(reader)?;
            Ok(Expression::PseudoVariable {
                pseudo_table,
                column,
            })
        }
        ExprTag::SessionVariable => {
            let name = read_string(reader)?;
            Ok(Expression::SessionVariable { name })
        }
    }
}

// Helper serialization functions for operator and other types

fn write_binary_operator<W: Write>(writer: &mut W, op: BinaryOperator) -> Result<(), StorageError> {
    let tag = match op {
        BinaryOperator::Plus => 0u8,
        BinaryOperator::Minus => 1,
        BinaryOperator::Multiply => 2,
        BinaryOperator::Divide => 3,
        BinaryOperator::IntegerDivide => 4,
        BinaryOperator::Modulo => 5,
        BinaryOperator::Equal => 10,
        BinaryOperator::NotEqual => 11,
        BinaryOperator::LessThan => 12,
        BinaryOperator::LessThanOrEqual => 13,
        BinaryOperator::GreaterThan => 14,
        BinaryOperator::GreaterThanOrEqual => 15,
        BinaryOperator::And => 20,
        BinaryOperator::Or => 21,
        BinaryOperator::Concat => 30,
    };
    writer
        .write_all(&[tag])
        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))
}

fn read_binary_operator<R: Read>(reader: &mut R) -> Result<BinaryOperator, StorageError> {
    let tag = read_u8(reader)?;
    match tag {
        0 => Ok(BinaryOperator::Plus),
        1 => Ok(BinaryOperator::Minus),
        2 => Ok(BinaryOperator::Multiply),
        3 => Ok(BinaryOperator::Divide),
        4 => Ok(BinaryOperator::IntegerDivide),
        5 => Ok(BinaryOperator::Modulo),
        10 => Ok(BinaryOperator::Equal),
        11 => Ok(BinaryOperator::NotEqual),
        12 => Ok(BinaryOperator::LessThan),
        13 => Ok(BinaryOperator::LessThanOrEqual),
        14 => Ok(BinaryOperator::GreaterThan),
        15 => Ok(BinaryOperator::GreaterThanOrEqual),
        20 => Ok(BinaryOperator::And),
        21 => Ok(BinaryOperator::Or),
        30 => Ok(BinaryOperator::Concat),
        _ => Err(StorageError::NotImplemented(format!(
            "Unknown binary operator tag: {}",
            tag
        ))),
    }
}

fn write_unary_operator<W: Write>(writer: &mut W, op: UnaryOperator) -> Result<(), StorageError> {
    let tag = match op {
        UnaryOperator::Not => 0u8,
        UnaryOperator::Minus => 1,
        UnaryOperator::Plus => 2,
        UnaryOperator::IsNull => 3,
        UnaryOperator::IsNotNull => 4,
    };
    writer
        .write_all(&[tag])
        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))
}

fn read_unary_operator<R: Read>(reader: &mut R) -> Result<UnaryOperator, StorageError> {
    let tag = read_u8(reader)?;
    match tag {
        0 => Ok(UnaryOperator::Not),
        1 => Ok(UnaryOperator::Minus),
        2 => Ok(UnaryOperator::Plus),
        3 => Ok(UnaryOperator::IsNull),
        4 => Ok(UnaryOperator::IsNotNull),
        _ => Err(StorageError::NotImplemented(format!(
            "Unknown unary operator tag: {}",
            tag
        ))),
    }
}

fn write_case_when<W: Write>(writer: &mut W, when: &CaseWhen) -> Result<(), StorageError> {
    write_u32(writer, when.conditions.len() as u32)?;
    for condition in &when.conditions {
        write_expression(writer, condition)?;
    }
    write_expression(writer, &when.result)?;
    Ok(())
}

fn read_case_when<R: Read>(reader: &mut R) -> Result<CaseWhen, StorageError> {
    let condition_count = read_u32(reader)?;
    let mut conditions = Vec::new();
    for _ in 0..condition_count {
        conditions.push(read_expression(reader)?);
    }
    let result = read_expression(reader)?;
    Ok(CaseWhen { conditions, result })
}

fn write_character_unit<W: Write>(
    writer: &mut W,
    unit: &CharacterUnit,
) -> Result<(), StorageError> {
    let tag = match unit {
        CharacterUnit::Characters => 0u8,
        CharacterUnit::Octets => 1,
    };
    writer
        .write_all(&[tag])
        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))
}

fn read_character_unit<R: Read>(reader: &mut R) -> Result<CharacterUnit, StorageError> {
    let tag = read_u8(reader)?;
    match tag {
        0 => Ok(CharacterUnit::Characters),
        1 => Ok(CharacterUnit::Octets),
        _ => Err(StorageError::NotImplemented(format!(
            "Unknown character unit tag: {}",
            tag
        ))),
    }
}

fn write_trim_position<W: Write>(writer: &mut W, pos: &TrimPosition) -> Result<(), StorageError> {
    let tag = match pos {
        TrimPosition::Both => 0u8,
        TrimPosition::Leading => 1,
        TrimPosition::Trailing => 2,
    };
    writer
        .write_all(&[tag])
        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))
}

fn read_trim_position<R: Read>(reader: &mut R) -> Result<TrimPosition, StorageError> {
    let tag = read_u8(reader)?;
    match tag {
        0 => Ok(TrimPosition::Both),
        1 => Ok(TrimPosition::Leading),
        2 => Ok(TrimPosition::Trailing),
        _ => Err(StorageError::NotImplemented(format!(
            "Unknown trim position tag: {}",
            tag
        ))),
    }
}

fn write_interval_unit<W: Write>(writer: &mut W, unit: &IntervalUnit) -> Result<(), StorageError> {
    let tag = match unit {
        IntervalUnit::Microsecond => 0u8,
        IntervalUnit::Second => 1,
        IntervalUnit::Minute => 2,
        IntervalUnit::Hour => 3,
        IntervalUnit::Day => 4,
        IntervalUnit::Week => 5,
        IntervalUnit::Month => 6,
        IntervalUnit::Quarter => 7,
        IntervalUnit::Year => 8,
        IntervalUnit::SecondMicrosecond => 9,
        IntervalUnit::MinuteMicrosecond => 10,
        IntervalUnit::MinuteSecond => 11,
        IntervalUnit::HourMicrosecond => 12,
        IntervalUnit::HourSecond => 13,
        IntervalUnit::HourMinute => 14,
        IntervalUnit::DayMicrosecond => 15,
        IntervalUnit::DaySecond => 16,
        IntervalUnit::DayMinute => 17,
        IntervalUnit::DayHour => 18,
        IntervalUnit::YearMonth => 19,
    };
    writer
        .write_all(&[tag])
        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))
}

fn read_interval_unit<R: Read>(reader: &mut R) -> Result<IntervalUnit, StorageError> {
    let tag = read_u8(reader)?;
    match tag {
        0 => Ok(IntervalUnit::Microsecond),
        1 => Ok(IntervalUnit::Second),
        2 => Ok(IntervalUnit::Minute),
        3 => Ok(IntervalUnit::Hour),
        4 => Ok(IntervalUnit::Day),
        5 => Ok(IntervalUnit::Week),
        6 => Ok(IntervalUnit::Month),
        7 => Ok(IntervalUnit::Quarter),
        8 => Ok(IntervalUnit::Year),
        9 => Ok(IntervalUnit::SecondMicrosecond),
        10 => Ok(IntervalUnit::MinuteMicrosecond),
        11 => Ok(IntervalUnit::MinuteSecond),
        12 => Ok(IntervalUnit::HourMicrosecond),
        13 => Ok(IntervalUnit::HourSecond),
        14 => Ok(IntervalUnit::HourMinute),
        15 => Ok(IntervalUnit::DayMicrosecond),
        16 => Ok(IntervalUnit::DaySecond),
        17 => Ok(IntervalUnit::DayMinute),
        18 => Ok(IntervalUnit::DayHour),
        19 => Ok(IntervalUnit::YearMonth),
        _ => Err(StorageError::NotImplemented(format!(
            "Unknown interval unit tag: {}",
            tag
        ))),
    }
}

fn write_fulltext_mode<W: Write>(writer: &mut W, mode: &FulltextMode) -> Result<(), StorageError> {
    let tag = match mode {
        FulltextMode::NaturalLanguage => 0u8,
        FulltextMode::Boolean => 1,
        FulltextMode::QueryExpansion => 2,
    };
    writer
        .write_all(&[tag])
        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))
}

fn read_fulltext_mode<R: Read>(reader: &mut R) -> Result<FulltextMode, StorageError> {
    let tag = read_u8(reader)?;
    match tag {
        0 => Ok(FulltextMode::NaturalLanguage),
        1 => Ok(FulltextMode::Boolean),
        2 => Ok(FulltextMode::QueryExpansion),
        _ => Err(StorageError::NotImplemented(format!(
            "Unknown fulltext mode tag: {}",
            tag
        ))),
    }
}

fn write_pseudo_table<W: Write>(writer: &mut W, table: PseudoTable) -> Result<(), StorageError> {
    let tag = match table {
        PseudoTable::Old => 0u8,
        PseudoTable::New => 1,
    };
    writer
        .write_all(&[tag])
        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))
}

fn read_pseudo_table<R: Read>(reader: &mut R) -> Result<PseudoTable, StorageError> {
    let tag = read_u8(reader)?;
    match tag {
        0 => Ok(PseudoTable::Old),
        1 => Ok(PseudoTable::New),
        _ => Err(StorageError::NotImplemented(format!(
            "Unknown pseudo table tag: {}",
            tag
        ))),
    }
}

fn write_window_function_spec<W: Write>(
    writer: &mut W,
    spec: &WindowFunctionSpec,
) -> Result<(), StorageError> {
    match spec {
        WindowFunctionSpec::Aggregate { name, args } => {
            writer
                .write_all(&[0u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_string(writer, name)?;
            write_u32(writer, args.len() as u32)?;
            for arg in args {
                write_expression(writer, arg)?;
            }
        }
        WindowFunctionSpec::Ranking { name, args } => {
            writer
                .write_all(&[1u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_string(writer, name)?;
            write_u32(writer, args.len() as u32)?;
            for arg in args {
                write_expression(writer, arg)?;
            }
        }
        WindowFunctionSpec::Value { name, args } => {
            writer
                .write_all(&[2u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_string(writer, name)?;
            write_u32(writer, args.len() as u32)?;
            for arg in args {
                write_expression(writer, arg)?;
            }
        }
    }
    Ok(())
}

fn read_window_function_spec<R: Read>(reader: &mut R) -> Result<WindowFunctionSpec, StorageError> {
    let tag = read_u8(reader)?;
    match tag {
        0 => {
            let name = read_string(reader)?;
            let arg_count = read_u32(reader)?;
            let mut args = Vec::new();
            for _ in 0..arg_count {
                args.push(read_expression(reader)?);
            }
            Ok(WindowFunctionSpec::Aggregate { name, args })
        }
        1 => {
            let name = read_string(reader)?;
            let arg_count = read_u32(reader)?;
            let mut args = Vec::new();
            for _ in 0..arg_count {
                args.push(read_expression(reader)?);
            }
            Ok(WindowFunctionSpec::Ranking { name, args })
        }
        2 => {
            let name = read_string(reader)?;
            let arg_count = read_u32(reader)?;
            let mut args = Vec::new();
            for _ in 0..arg_count {
                args.push(read_expression(reader)?);
            }
            Ok(WindowFunctionSpec::Value { name, args })
        }
        _ => Err(StorageError::NotImplemented(format!(
            "Unknown window function spec tag: {}",
            tag
        ))),
    }
}

fn write_window_spec<W: Write>(writer: &mut W, spec: &WindowSpec) -> Result<(), StorageError> {
    // Write partition_by
    write_bool(writer, spec.partition_by.is_some())?;
    if let Some(partition_exprs) = &spec.partition_by {
        write_u32(writer, partition_exprs.len() as u32)?;
        for expr in partition_exprs {
            write_expression(writer, expr)?;
        }
    }

    // Write order_by - note: OrderByItem serialization not implemented yet
    // For now, we'll serialize as empty (this is a limitation)
    write_bool(writer, false)?;

    // Write frame
    write_bool(writer, spec.frame.is_some())?;
    if let Some(frame) = &spec.frame {
        write_window_frame(writer, frame)?;
    }

    Ok(())
}

fn read_window_spec<R: Read>(reader: &mut R) -> Result<WindowSpec, StorageError> {
    // Read partition_by
    let has_partition_by = read_bool(reader)?;
    let partition_by = if has_partition_by {
        let count = read_u32(reader)?;
        let mut exprs = Vec::new();
        for _ in 0..count {
            exprs.push(read_expression(reader)?);
        }
        Some(exprs)
    } else {
        None
    };

    // Read order_by
    let has_order_by = read_bool(reader)?;
    if has_order_by {
        return Err(StorageError::NotImplemented(
            "OrderBy in window spec not yet supported".to_string(),
        ));
    }

    // Read frame
    let has_frame = read_bool(reader)?;
    let frame = if has_frame {
        Some(read_window_frame(reader)?)
    } else {
        None
    };

    Ok(WindowSpec {
        partition_by,
        order_by: None,
        frame,
    })
}

fn write_window_frame<W: Write>(writer: &mut W, frame: &WindowFrame) -> Result<(), StorageError> {
    // Write unit
    let unit_tag = match frame.unit {
        FrameUnit::Rows => 0u8,
        FrameUnit::Range => 1,
    };
    writer
        .write_all(&[unit_tag])
        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

    // Write start bound
    write_frame_bound(writer, &frame.start)?;

    // Write end bound
    write_bool(writer, frame.end.is_some())?;
    if let Some(end) = &frame.end {
        write_frame_bound(writer, end)?;
    }

    Ok(())
}

fn read_window_frame<R: Read>(reader: &mut R) -> Result<WindowFrame, StorageError> {
    // Read unit
    let unit_tag = read_u8(reader)?;
    let unit = match unit_tag {
        0 => FrameUnit::Rows,
        1 => FrameUnit::Range,
        _ => {
            return Err(StorageError::NotImplemented(format!(
                "Unknown frame unit tag: {}",
                unit_tag
            )))
        }
    };

    // Read start bound
    let start = read_frame_bound(reader)?;

    // Read end bound
    let has_end = read_bool(reader)?;
    let end = if has_end {
        Some(read_frame_bound(reader)?)
    } else {
        None
    };

    Ok(WindowFrame { unit, start, end })
}

fn write_frame_bound<W: Write>(writer: &mut W, bound: &FrameBound) -> Result<(), StorageError> {
    match bound {
        FrameBound::UnboundedPreceding => {
            writer
                .write_all(&[0u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
        }
        FrameBound::Preceding(expr) => {
            writer
                .write_all(&[1u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_expression(writer, expr)?;
        }
        FrameBound::CurrentRow => {
            writer
                .write_all(&[2u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
        }
        FrameBound::Following(expr) => {
            writer
                .write_all(&[3u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_expression(writer, expr)?;
        }
        FrameBound::UnboundedFollowing => {
            writer
                .write_all(&[4u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
        }
    }
    Ok(())
}

fn read_frame_bound<R: Read>(reader: &mut R) -> Result<FrameBound, StorageError> {
    let tag = read_u8(reader)?;
    match tag {
        0 => Ok(FrameBound::UnboundedPreceding),
        1 => {
            let expr = Box::new(read_expression(reader)?);
            Ok(FrameBound::Preceding(expr))
        }
        2 => Ok(FrameBound::CurrentRow),
        3 => {
            let expr = Box::new(read_expression(reader)?);
            Ok(FrameBound::Following(expr))
        }
        4 => Ok(FrameBound::UnboundedFollowing),
        _ => Err(StorageError::NotImplemented(format!(
            "Unknown frame bound tag: {}",
            tag
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_types::SqlValue;

    #[test]
    fn test_literal_roundtrip() {
        let expr = Expression::Literal(SqlValue::Integer(42));
        let mut buf = Vec::new();
        write_expression(&mut buf, &expr).unwrap();

        let mut reader = &buf[..];
        let result = read_expression(&mut reader).unwrap();
        assert_eq!(result, expr);
    }

    #[test]
    fn test_column_ref_roundtrip() {
        let expr = Expression::ColumnRef {
            table: Some("users".to_string()),
            column: "id".to_string(),
        };
        let mut buf = Vec::new();
        write_expression(&mut buf, &expr).unwrap();

        let mut reader = &buf[..];
        let result = read_expression(&mut reader).unwrap();
        assert_eq!(result, expr);
    }

    #[test]
    fn test_binary_op_roundtrip() {
        let expr = Expression::BinaryOp {
            op: BinaryOperator::Plus,
            left: Box::new(Expression::Literal(SqlValue::Integer(1))),
            right: Box::new(Expression::Literal(SqlValue::Integer(2))),
        };
        let mut buf = Vec::new();
        write_expression(&mut buf, &expr).unwrap();

        let mut reader = &buf[..];
        let result = read_expression(&mut reader).unwrap();
        assert_eq!(result, expr);
    }

    #[test]
    fn test_function_roundtrip() {
        let expr = Expression::Function {
            name: "UPPER".to_string(),
            args: vec![Expression::Literal(SqlValue::Varchar("test".to_string()))],
            character_unit: None,
        };
        let mut buf = Vec::new();
        write_expression(&mut buf, &expr).unwrap();

        let mut reader = &buf[..];
        let result = read_expression(&mut reader).unwrap();
        assert_eq!(result, expr);
    }

    #[test]
    fn test_case_roundtrip() {
        let expr = Expression::Case {
            operand: None,
            when_clauses: vec![CaseWhen {
                conditions: vec![Expression::Literal(SqlValue::Boolean(true))],
                result: Expression::Literal(SqlValue::Integer(1)),
            }],
            else_result: Some(Box::new(Expression::Literal(SqlValue::Integer(0)))),
        };
        let mut buf = Vec::new();
        write_expression(&mut buf, &expr).unwrap();

        let mut reader = &buf[..];
        let result = read_expression(&mut reader).unwrap();
        assert_eq!(result, expr);
    }
}
