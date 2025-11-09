use crate::executor::QueryResult;
use prettytable::{Table, Row, Cell};

#[derive(Debug, Clone, Copy)]
pub enum OutputFormat {
    Table,
    Json,
    Csv,
}

pub struct ResultFormatter {
    format: OutputFormat,
}

impl ResultFormatter {
    pub fn new() -> Self {
        ResultFormatter {
            format: OutputFormat::Table,
        }
    }

    pub fn set_format(&mut self, format: OutputFormat) {
        self.format = format;
    }

    pub fn print_result(&self, result: &QueryResult) {
        match self.format {
            OutputFormat::Table => self.print_table(result),
            OutputFormat::Json => self.print_json(result),
            OutputFormat::Csv => self.print_csv(result),
        }

        // Print timing if available
        if let Some(time_ms) = result.execution_time_ms {
            println!("{} rows in set ({:.3}s)", result.row_count, time_ms / 1000.0);
        } else {
            println!("{} rows", result.row_count);
        }
    }

    fn print_table(&self, result: &QueryResult) {
        if result.columns.is_empty() {
            return;
        }

        let mut table = Table::new();

        // Add header
        let header_cells: Vec<Cell> = result
            .columns
            .iter()
            .map(|col| Cell::new(col))
            .collect();
        table.add_row(Row::new(header_cells));

        // Add rows
        for row in &result.rows {
            let cells: Vec<Cell> = row.iter().map(|val| Cell::new(val)).collect();
            table.add_row(Row::new(cells));
        }

        table.printstd();
    }

    fn print_json(&self, result: &QueryResult) {
        let mut json_rows = Vec::new();
        for row in &result.rows {
            let mut json_obj = serde_json::Map::new();
            for (i, col) in result.columns.iter().enumerate() {
                if i < row.len() {
                    json_obj.insert(
                        col.clone(),
                        serde_json::Value::String(row[i].clone()),
                    );
                }
            }
            json_rows.push(serde_json::Value::Object(json_obj));
        }
        
        let output = serde_json::to_string_pretty(&json_rows)
            .unwrap_or_else(|_| "[]".to_string());
        println!("{}", output);
    }

    fn print_csv(&self, result: &QueryResult) {
        // Print header
        println!("{}", result.columns.join(","));

        // Print rows
        for row in &result.rows {
            println!("{}", row.join(","));
        }
    }
}
