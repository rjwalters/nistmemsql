//! Advanced SQL:1999 object management.
//!
//! This module handles user-defined types, domains, sequences, collations,
//! character sets, translations, views, and triggers.

use crate::{
    advanced_objects::{Assertion, CharacterSet, Collation, Sequence, Translation},
    domain::DomainDefinition,
    errors::CatalogError,
    trigger::TriggerDefinition,
    type_definition::TypeDefinition,
    view::ViewDefinition,
};

impl super::Catalog {
    // ============================================================================
    // Type Definition Management Methods
    // ============================================================================

    /// Create a new user-defined type.
    pub fn create_type(&mut self, type_def: TypeDefinition) -> Result<(), CatalogError> {
        let type_name = type_def.name.clone();
        if self.type_definitions.contains_key(&type_name) {
            return Err(CatalogError::TypeAlreadyExists(type_name));
        }
        self.type_definitions.insert(type_name, type_def);
        Ok(())
    }

    /// Drop a user-defined type.
    pub fn drop_type(&mut self, name: &str, cascade: bool) -> Result<(), CatalogError> {
        if !self.type_definitions.contains_key(name) {
            return Err(CatalogError::TypeNotFound(name.to_string()));
        }

        // Check for dependencies if not CASCADE
        if !cascade {
            // Check if any tables use this type
            for schema in self.schemas.values() {
                for table_name in schema.list_tables() {
                    if let Some(table) = schema.get_table(&table_name) {
                        for column in &table.columns {
                            if let types::DataType::UserDefined { type_name } = &column.data_type {
                                if type_name == name {
                                    return Err(CatalogError::TypeInUse(name.to_string()));
                                }
                            }
                        }
                    }
                }
            }
        }

        self.type_definitions.remove(name);

        // If CASCADE, also drop dependent objects (tables with columns of this type)
        if cascade {
            let mut tables_to_drop = Vec::new();
            for (schema_name, schema) in &self.schemas {
                for table_name in schema.list_tables() {
                    if let Some(table) = schema.get_table(&table_name) {
                        for column in &table.columns {
                            if let types::DataType::UserDefined { type_name } = &column.data_type {
                                if type_name == name {
                                    tables_to_drop.push(format!("{}.{}", schema_name, table_name));
                                    break;
                                }
                            }
                        }
                    }
                }
            }

            // Drop the dependent tables
            for qualified_table_name in tables_to_drop {
                let _ = self.drop_table(&qualified_table_name);
            }
        }

        Ok(())
    }

    /// Get a type definition by name.
    pub fn get_type(&self, name: &str) -> Option<&TypeDefinition> {
        self.type_definitions.get(name)
    }

    /// Check if a type exists.
    pub fn type_exists(&self, name: &str) -> bool {
        self.type_definitions.contains_key(name)
    }

    /// List all user-defined type names.
    pub fn list_types(&self) -> Vec<String> {
        self.type_definitions.keys().cloned().collect()
    }

    // ============================================================================
    // Domain Management Methods
    // ============================================================================

    /// Create a new domain.
    pub fn create_domain(&mut self, domain: DomainDefinition) -> Result<(), CatalogError> {
        let name = domain.name.clone();
        if self.domains.contains_key(&name) {
            return Err(CatalogError::DomainAlreadyExists(name));
        }
        self.domains.insert(name, domain);
        Ok(())
    }

    /// Get a domain definition by name.
    pub fn get_domain(&self, name: &str) -> Option<&DomainDefinition> {
        self.domains.get(name)
    }

    /// Drop a domain.
    pub fn drop_domain(&mut self, name: &str, cascade: bool) -> Result<(), CatalogError> {
        if !self.domains.contains_key(name) {
            return Err(CatalogError::DomainNotFound(name.to_string()));
        }

        // Check if any columns use this domain
        // Note: Domain support in column definitions is not fully implemented yet
        // This code provides the framework for when it is implemented
        let mut columns_using_domain = Vec::new();

        for schema in self.schemas.values() {
            for table_name in schema.list_tables() {
                if let Some(table) = schema.get_table(&table_name) {
                    for column in &table.columns {
                        // Check if column type is a UserDefined type that matches this domain
                        // In the future, when domains are properly integrated, we might have
                        // a DataType::Domain variant or track domain usage separately
                        if let types::DataType::UserDefined { type_name } = &column.data_type {
                            // Check if this user-defined type is actually a domain
                            if self.domains.contains_key(type_name) && type_name == name {
                                columns_using_domain.push((table_name.clone(), column.name.clone()));
                            }
                        }
                    }
                }
            }
        }

        // If RESTRICT and domain is in use, return error
        if !cascade && !columns_using_domain.is_empty() {
            return Err(CatalogError::DomainInUse {
                domain_name: name.to_string(),
                dependent_columns: columns_using_domain,
            });
        }

        // If CASCADE, we would need to either:
        // 1. Convert columns to the domain's base type
        // 2. Or drop the columns/tables using the domain
        // For now, since domain support isn't fully implemented, we'll just proceed
        if cascade && !columns_using_domain.is_empty() {
            // Get the domain to find its base type
            let domain = self.domains.get(name).cloned();

            if let Some(domain_def) = domain {
                // Convert all columns using this domain to the domain's base type
                for (table_name, column_name) in columns_using_domain {
                    for schema in self.schemas.values_mut() {
                        if let Some(table) = schema.get_table(&table_name) {
                            let mut modified_table = table.clone();
                            for col in &mut modified_table.columns {
                                if col.name == column_name {
                                    // Replace domain type with base type
                                    col.data_type = domain_def.data_type.clone();
                                }
                            }
                            // Drop and recreate table with modified columns
                            schema.drop_table(&table_name)?;
                            schema.create_table(modified_table)?;
                            break;
                        }
                    }
                }
            }
        }

        self.domains.remove(name);
        Ok(())
    }

    /// Check if a domain exists.
    pub fn domain_exists(&self, name: &str) -> bool {
        self.domains.contains_key(&name.to_uppercase())
    }

    /// List all domain names.
    pub fn list_domains(&self) -> Vec<String> {
        self.domains.keys().cloned().collect()
    }

    // ============================================================================
    // Sequence Management Methods
    // ============================================================================

    /// Create a SEQUENCE
    pub fn create_sequence(
        &mut self,
        name: String,
        start_with: Option<i64>,
        increment_by: i64,
        min_value: Option<i64>,
        max_value: Option<i64>,
        cycle: bool,
    ) -> Result<(), CatalogError> {
        if self.sequences.contains_key(&name) {
            return Err(CatalogError::SequenceAlreadyExists(name));
        }
        self.sequences.insert(
            name.clone(),
            Sequence::new(name, start_with, increment_by, min_value, max_value, cycle),
        );
        Ok(())
    }

    /// Drop a SEQUENCE
    pub fn drop_sequence(&mut self, name: &str, cascade: bool) -> Result<(), CatalogError> {
        // Check if sequence exists
        if !self.sequences.contains_key(name) {
            return Err(CatalogError::SequenceNotFound(name.to_string()));
        }

        // Check if any columns use this sequence in their default values
        let mut columns_using_sequence = Vec::new();
        for schema in self.schemas.values() {
            for table_name in schema.list_tables() {
                if let Some(table) = schema.get_table(&table_name) {
                    for column in &table.columns {
                        if let Some(default_expr) = &column.default_value {
                            if self.expression_uses_sequence(default_expr, name) {
                                columns_using_sequence.push((
                                    table_name.clone(),
                                    column.name.clone(),
                                ));
                            }
                        }
                    }
                }
            }
        }

        // If RESTRICT and sequence is in use, return error
        if !cascade && !columns_using_sequence.is_empty() {
            return Err(CatalogError::SequenceInUse {
                sequence_name: name.to_string(),
                dependent_columns: columns_using_sequence,
            });
        }

        // If CASCADE, remove sequence dependencies from columns
        if cascade && !columns_using_sequence.is_empty() {
            for (table_name, column_name) in columns_using_sequence {
                // Get mutable reference to the schema containing the table
                for schema in self.schemas.values_mut() {
                    if let Some(table) = schema.get_table(&table_name) {
                        // Find the column and remove its default value
                        if table.columns.iter().any(|c| c.name == column_name) {
                            // We need to reconstruct the table to modify it
                            // This is a limitation of the current architecture
                            // For now, we'll handle this by removing and reinserting the table
                            let mut modified_table = table.clone();
                            for col in &mut modified_table.columns {
                                if col.name == column_name {
                                    col.default_value = None;
                                }
                            }
                            // Drop and recreate table with modified columns
                            schema.drop_table(&table_name)?;
                            schema.create_table(modified_table)?;
                            break;
                        }
                    }
                }
            }
        }

        // Finally, drop the sequence
        self.sequences.remove(name);
        Ok(())
    }

    /// Check if an expression uses a specific sequence
    fn expression_uses_sequence(&self, expr: &ast::Expression, sequence_name: &str) -> bool {
        use ast::Expression;
        match expr {
            Expression::NextValue { sequence_name: seq_name } => seq_name == sequence_name,
            Expression::BinaryOp { left, right, .. } => {
                self.expression_uses_sequence(left, sequence_name)
                    || self.expression_uses_sequence(right, sequence_name)
            }
            Expression::UnaryOp { expr, .. } => self.expression_uses_sequence(expr, sequence_name),
            Expression::Function { args, .. } | Expression::AggregateFunction { args, .. } => {
                args.iter().any(|arg| self.expression_uses_sequence(arg, sequence_name))
            }
            Expression::IsNull { expr, .. } => self.expression_uses_sequence(expr, sequence_name),
            Expression::Case { operand, when_clauses, else_result } => {
                operand.as_ref().map_or(false, |e| self.expression_uses_sequence(e, sequence_name))
                    || when_clauses.iter().any(|when| {
                        when.conditions.iter().any(|c| self.expression_uses_sequence(c, sequence_name))
                            || self.expression_uses_sequence(&when.result, sequence_name)
                    })
                    || else_result
                        .as_ref()
                        .map_or(false, |e| self.expression_uses_sequence(e, sequence_name))
            }
            Expression::ScalarSubquery(_)
            | Expression::In { .. }
            | Expression::InList { .. }
            | Expression::Between { .. }
            | Expression::Cast { .. }
            | Expression::Position { .. }
            | Expression::Trim { .. }
            | Expression::Like { .. }
            | Expression::Exists { .. }
            | Expression::QuantifiedComparison { .. }
            | Expression::WindowFunction { .. } => {
                // These could theoretically contain sequence references in subexpressions
                // For now, we'll do a simple check
                false
            }
            _ => false,
        }
    }

    /// Alter a SEQUENCE
    pub fn alter_sequence(
        &mut self,
        name: &str,
        restart_with: Option<i64>,
        increment_by: Option<i64>,
        min_value: Option<Option<i64>>,
        max_value: Option<Option<i64>>,
        cycle: Option<bool>,
    ) -> Result<(), CatalogError> {
        let seq = self
            .sequences
            .get_mut(name)
            .ok_or_else(|| CatalogError::SequenceNotFound(name.to_string()))?;

        if let Some(restart) = restart_with {
            seq.restart(Some(restart));
        }
        if let Some(incr) = increment_by {
            seq.increment_by = incr;
        }
        if let Some(min) = min_value {
            seq.min_value = min;
        }
        if let Some(max) = max_value {
            seq.max_value = max;
        }
        if let Some(cyc) = cycle {
            seq.cycle = cyc;
        }

        Ok(())
    }

    /// Get a mutable reference to a SEQUENCE for NEXT VALUE FOR
    pub fn get_sequence_mut(&mut self, name: &str) -> Result<&mut Sequence, CatalogError> {
        self.sequences.get_mut(name).ok_or_else(|| CatalogError::SequenceNotFound(name.to_string()))
    }

    // ============================================================================
    // Collation Management Methods
    // ============================================================================

    /// Create a COLLATION
    pub fn create_collation(
        &mut self,
        name: String,
        character_set: Option<String>,
        source_collation: Option<String>,
        pad_space: Option<bool>,
    ) -> Result<(), CatalogError> {
        if self.collations.contains_key(&name) {
            return Err(CatalogError::CollationAlreadyExists(name));
        }
        self.collations
            .insert(name.clone(), Collation::new(name, character_set, source_collation, pad_space));
        Ok(())
    }

    /// Drop a COLLATION
    pub fn drop_collation(&mut self, name: &str) -> Result<(), CatalogError> {
        self.collations
            .remove(name)
            .map(|_| ())
            .ok_or_else(|| CatalogError::CollationNotFound(name.to_string()))
    }

    // ============================================================================
    // Character Set Management Methods
    // ============================================================================

    /// Create a CHARACTER SET
    pub fn create_character_set(
        &mut self,
        name: String,
        source: Option<String>,
        collation: Option<String>,
    ) -> Result<(), CatalogError> {
        if self.character_sets.contains_key(&name) {
            return Err(CatalogError::CharacterSetAlreadyExists(name));
        }
        self.character_sets.insert(name.clone(), CharacterSet::new(name, source, collation));
        Ok(())
    }

    /// Drop a CHARACTER SET
    pub fn drop_character_set(&mut self, name: &str) -> Result<(), CatalogError> {
        self.character_sets
            .remove(name)
            .map(|_| ())
            .ok_or_else(|| CatalogError::CharacterSetNotFound(name.to_string()))
    }

    // ============================================================================
    // Translation Management Methods
    // ============================================================================

    /// Create a TRANSLATION
    pub fn create_translation(
        &mut self,
        name: String,
        source_charset: Option<String>,
        target_charset: Option<String>,
        translation_source: Option<String>,
    ) -> Result<(), CatalogError> {
        if self.translations.contains_key(&name) {
            return Err(CatalogError::TranslationAlreadyExists(name));
        }
        self.translations.insert(
            name.clone(),
            Translation::new(name, source_charset, target_charset, translation_source),
        );
        Ok(())
    }

    /// Drop a TRANSLATION
    pub fn drop_translation(&mut self, name: &str) -> Result<(), CatalogError> {
        self.translations
            .remove(name)
            .map(|_| ())
            .ok_or_else(|| CatalogError::TranslationNotFound(name.to_string()))
    }

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

    // ============================================================================
    // Trigger Management Methods
    // ============================================================================

    /// Create a TRIGGER
    pub fn create_trigger(&mut self, trigger: TriggerDefinition) -> Result<(), CatalogError> {
        let name = trigger.name.clone();
        if self.triggers.contains_key(&name) {
            return Err(CatalogError::TriggerAlreadyExists(name));
        }
        self.triggers.insert(name, trigger);
        Ok(())
    }

    /// Get a TRIGGER definition by name
    pub fn get_trigger(&self, name: &str) -> Option<&TriggerDefinition> {
        self.triggers.get(name)
    }

    /// Drop a TRIGGER
    pub fn drop_trigger(&mut self, name: &str) -> Result<(), CatalogError> {
        self.triggers
            .remove(name)
            .map(|_| ())
            .ok_or_else(|| CatalogError::TriggerNotFound(name.to_string()))
    }

    /// Get all triggers for a table with a specific event
    ///
    /// # Arguments
    /// * `table_name` - Name of the table to check for triggers
    /// * `event` - Optional trigger event to filter by (Insert, Update, Delete)
    ///
    /// # Returns
    /// Iterator over trigger definitions matching the criteria
    pub fn get_triggers_for_table<'a>(
        &'a self,
        table_name: &'a str,
        event: Option<ast::TriggerEvent>,
    ) -> impl Iterator<Item = &'a TriggerDefinition> + 'a {
        self.triggers.values().filter(move |trigger| {
            trigger.table_name == table_name && event.as_ref().is_none_or(|e| trigger.event == *e)
        })
    }

    /// Create an ASSERTION (SQL:1999 Feature F671/F672)
    pub fn create_assertion(&mut self, assertion: Assertion) -> Result<(), CatalogError> {
        let name = assertion.name.clone();
        if self.assertions.contains_key(&name) {
            return Err(CatalogError::AssertionAlreadyExists(name));
        }
        self.assertions.insert(name, assertion);
        Ok(())
    }

    /// Get an ASSERTION definition by name
    pub fn get_assertion(&self, name: &str) -> Option<&Assertion> {
        self.assertions.get(name)
    }

    /// Drop an ASSERTION
    pub fn drop_assertion(&mut self, name: &str, cascade: bool) -> Result<(), CatalogError> {
        // For now, assertions don't have dependencies, so cascade is ignored
        // In the future, cascade might be used if assertions can reference other assertions
        let _ = cascade;

        self.assertions
            .remove(name)
            .map(|_| ())
            .ok_or_else(|| CatalogError::AssertionNotFound(name.to_string()))
    }

    /// Check if an assertion exists
    pub fn assertion_exists(&self, name: &str) -> bool {
        self.assertions.contains_key(name)
    }

    /// List all assertion names
    pub fn list_assertions(&self) -> Vec<String> {
        self.assertions.keys().cloned().collect()
    }

    // ============================================================================
    // Function Management Methods (SQL:1999 Feature P001)
    // ============================================================================

    /// Create a function stub for privilege tracking
    pub fn create_function_stub(
        &mut self,
        name: String,
        schema: String,
    ) -> Result<(), CatalogError> {
        use crate::advanced_objects::Function;
        if self.functions.contains_key(&name) {
            return Err(CatalogError::FunctionAlreadyExists(name));
        }
        self.functions.insert(name.clone(), Function::new(name, schema));
        Ok(())
    }

    /// Check if a function exists
    pub fn function_exists(&self, name: &str) -> bool {
        self.functions.contains_key(name)
    }

    /// Get a function definition by name
    pub fn get_function(&self, name: &str) -> Option<&crate::advanced_objects::Function> {
        self.functions.get(name)
    }

    /// List all function names
    pub fn list_functions(&self) -> Vec<String> {
        self.functions.keys().cloned().collect()
    }

    // ============================================================================
    // Procedure Management Methods (SQL:1999 Feature P001)
    // ============================================================================

    /// Create a procedure stub for privilege tracking
    pub fn create_procedure_stub(
        &mut self,
        name: String,
        schema: String,
    ) -> Result<(), CatalogError> {
        use crate::advanced_objects::Procedure;
        if self.procedures.contains_key(&name) {
            return Err(CatalogError::ProcedureAlreadyExists(name));
        }
        self.procedures.insert(name.clone(), Procedure::new(name, schema));
        Ok(())
    }

    /// Check if a procedure exists
    pub fn procedure_exists(&self, name: &str) -> bool {
        self.procedures.contains_key(name)
    }

    /// Get a procedure definition by name
    pub fn get_procedure(&self, name: &str) -> Option<&crate::advanced_objects::Procedure> {
        self.procedures.get(name)
    }

    /// List all procedure names
    pub fn list_procedures(&self) -> Vec<String> {
        self.procedures.keys().cloned().collect()
    }
}
