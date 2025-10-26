/// Project columns from a row based on SELECT list (combined schema version)
pub(super) fn project_row_combined(
    row: &storage::Row,
    columns: &[ast::SelectItem],
    evaluator: &crate::evaluator::CombinedExpressionEvaluator,
) -> Result<storage::Row, crate::errors::ExecutorError> {
    let mut values = Vec::new();

    for item in columns {
        match item {
            ast::SelectItem::Wildcard => {
                // SELECT * - include all columns
                values.extend(row.values.iter().cloned());
            }
            ast::SelectItem::Expression { expr, alias: _ } => {
                // Evaluate the expression
                let value = evaluator.eval(expr, row)?;
                values.push(value);
            }
        }
    }

    Ok(storage::Row::new(values))
}
