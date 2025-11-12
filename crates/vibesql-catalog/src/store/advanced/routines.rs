//! Function and procedure management methods (SQL:1999 Feature P001).

use crate::errors::CatalogError;

impl super::super::Catalog {
    // ============================================================================
    // Function Management Methods (SQL:1999 Feature P001)
    // ============================================================================

    /// Create a function
    pub fn create_function(
        &mut self,
        name: String,
        schema: String,
        parameters: Vec<crate::advanced_objects::FunctionParam>,
        return_type: vibesql_types::DataType,
        body: crate::advanced_objects::FunctionBody,
    ) -> Result<(), CatalogError> {
        use crate::advanced_objects::Function;
        if self.functions.contains_key(&name) {
            return Err(CatalogError::FunctionAlreadyExists(name));
        }
        self.functions
            .insert(name.clone(), Function::new(name, schema, parameters, return_type, body));
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

    /// Drop a function
    pub fn drop_function(&mut self, name: &str) -> Result<(), CatalogError> {
        if self.functions.remove(name).is_some() {
            Ok(())
        } else {
            Err(CatalogError::FunctionNotFound(name.to_string()))
        }
    }

    // ============================================================================
    // Procedure Management Methods (SQL:1999 Feature P001)
    // ============================================================================

    /// Create a procedure
    pub fn create_procedure(
        &mut self,
        name: String,
        schema: String,
        parameters: Vec<crate::advanced_objects::ProcedureParam>,
        body: crate::advanced_objects::ProcedureBody,
    ) -> Result<(), CatalogError> {
        use crate::advanced_objects::Procedure;
        if self.procedures.contains_key(&name) {
            return Err(CatalogError::ProcedureAlreadyExists(name));
        }
        self.procedures
            .insert(name.clone(), Procedure::new(name, schema, parameters, body));
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

    /// Drop a procedure
    pub fn drop_procedure(&mut self, name: &str) -> Result<(), CatalogError> {
        if self.procedures.remove(name).is_some() {
            Ok(())
        } else {
            Err(CatalogError::ProcedureNotFound(name.to_string()))
        }
    }
}
