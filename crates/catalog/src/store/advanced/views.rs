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

    /// Get a VIEW definition by name (supports qualified names like "schema.view")
    pub fn get_view(&self, name: &str) -> Option<&ViewDefinition> {
        self.views.get(name)
    }

    /// Drop a VIEW
    pub fn drop_view(&mut self, name: &str, cascade: bool) -> Result<(), CatalogError> {
        // Check if view exists
        if !self.views.contains_key(name) {
            return Err(CatalogError::ViewNotFound(name.to_string()));
        }

        // Find all views that depend on this view or table
        let dependent_views = self.find_dependent_views(name);

        // If RESTRICT and there are dependent views, return error
        if !cascade && !dependent_views.is_empty() {
            return Err(CatalogError::ViewInUse {
                view_name: name.to_string(),
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
        self.views.remove(name);
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
    fn select_references_table(&self, select: &ast::SelectStmt, table_name: &str) -> bool {
        // Check the FROM clause
        if let Some(ref from) = select.from {
            if self.from_clause_references_table(from, table_name) {
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
    fn from_clause_references_table(&self, from: &ast::FromClause, table_name: &str) -> bool {
        use ast::FromClause;
        match from {
            FromClause::Table { name, .. } => name == table_name,
            FromClause::Join { left, right, .. } => {
                self.from_clause_references_table(left, table_name)
                    || self.from_clause_references_table(right, table_name)
            }
            FromClause::Subquery { query, .. } => self.select_references_table(query, table_name),
        }
    }
}
