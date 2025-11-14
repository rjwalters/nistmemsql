//! View management methods and dependency tracking.

use crate::{errors::CatalogError, view::ViewDefinition};

impl super::super::Catalog {
    // ============================================================================
    // View Management Methods
    // ============================================================================

    /// Create a VIEW
    pub fn create_view(&mut self, view: ViewDefinition) -> Result<(), CatalogError> {
        let name = view.name.clone();
        if self.views.contains_key(&name) {
            return Err(CatalogError::ViewAlreadyExists(name));
        }
        self.views.insert(name, view);
        Ok(())
    }

    /// Get a VIEW definition by name with optional case-insensitive lookup
    pub fn get_view(&self, name: &str) -> Option<&ViewDefinition> {
        if self.case_sensitive_identifiers {
            self.views.get(name)
        } else {
            // Case-insensitive lookup
            let name_upper = name.to_uppercase();
            self.views
                .values()
                .find(|view| view.name.to_uppercase() == name_upper)
        }
    }

    /// Drop a VIEW
    pub fn drop_view(&mut self, name: &str, cascade: bool) -> Result<(), CatalogError> {
        // Find the actual view name (handle case-insensitivity)
        let actual_name = if self.case_sensitive_identifiers {
            if !self.views.contains_key(name) {
                return Err(CatalogError::ViewNotFound(name.to_string()));
            }
            name.to_string()
        } else {
            // Case-insensitive lookup
            let name_upper = name.to_uppercase();
            self.views
                .iter()
                .find(|(_, view)| view.name.to_uppercase() == name_upper)
                .map(|(key, _)| key.clone())
                .ok_or_else(|| CatalogError::ViewNotFound(name.to_string()))?
        };

        // Find all views that depend on this view or table
        let dependent_views = self.find_dependent_views(&actual_name);

        // If RESTRICT and there are dependent views, return error
        if !cascade && !dependent_views.is_empty() {
            return Err(CatalogError::ViewInUse {
                view_name: actual_name,
                dependent_views,
            });
        }

        // If CASCADE, drop all dependent views recursively
        if cascade {
            let views_to_drop = dependent_views.clone();
            for dependent_view in views_to_drop {
                // Recursively drop dependent views (they might have their own dependents)
                self.drop_view(&dependent_view, true)?;
            }
        }

        // Finally, drop the view itself
        self.views.remove(&actual_name);
        Ok(())
    }

    /// Find all views that depend on a given view or table
    fn find_dependent_views(&self, target_name: &str) -> Vec<String> {
        let mut dependent_views = Vec::new();

        for (view_name, view_def) in &self.views {
            if view_name == target_name {
                // Skip the view itself
                continue;
            }

            // Check if this view's query references the target
            if self.select_references_table(&view_def.query, target_name) {
                dependent_views.push(view_name.clone());
            }
        }

        dependent_views
    }

    /// Check if a SELECT statement references a specific table or view
    fn select_references_table(&self, select: &vibesql_ast::SelectStmt, table_name: &str) -> bool {
        // Check the FROM clause
        if let Some(ref from) = select.from {
            if self.does_from_clause_reference_table(from, table_name) {
                return true;
            }
        }

        // Check CTEs (WITH clause)
        if let Some(ref ctes) = select.with_clause {
            for cte in ctes {
                if self.select_references_table(&cte.query, table_name) {
                    return true;
                }
            }
        }

        // Check set operations (UNION, INTERSECT, EXCEPT)
        if let Some(ref set_op) = select.set_operation {
            if self.select_references_table(&set_op.right, table_name) {
                return true;
            }
        }

        false
    }

    /// Check if a FROM clause references a specific table or view
    fn does_from_clause_reference_table(&self, from: &vibesql_ast::FromClause, table_name: &str) -> bool {
        use vibesql_ast::FromClause;
        match from {
            FromClause::Table { name, .. } => {
                // Respect case sensitivity setting when comparing table names
                if self.case_sensitive_identifiers {
                    name == table_name
                } else {
                    name.to_uppercase() == table_name.to_uppercase()
                }
            }
            FromClause::Join { left, right, .. } => {
                self.does_from_clause_reference_table(left, table_name)
                    || self.does_from_clause_reference_table(right, table_name)
            }
            FromClause::Subquery { query, .. } => self.select_references_table(query, table_name),
        }
    }
}
