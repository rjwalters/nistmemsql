//! Privilege and grant management operations for the catalog.
//!
//! This module handles all privilege-related operations including grants,
//! revocations, and privilege checks.

use crate::{errors::CatalogError, privilege::PrivilegeGrant};

impl super::Catalog {
    /// Add a privilege grant to the catalog.
    pub fn add_grant(&mut self, grant: PrivilegeGrant) {
        self.privilege_grants.push(grant);
    }

    /// Check if a grantee has a specific privilege on an object.
    pub fn has_privilege(
        &self,
        grantee: &str,
        object: &str,
        priv_type: &vibesql_ast::PrivilegeType,
    ) -> bool {
        self.privilege_grants
            .iter()
            .any(|g| g.grantee == grantee && g.object == object && g.privilege == *priv_type)
    }

    /// Get all grants for a specific grantee.
    pub fn get_grants_for_grantee(&self, grantee: &str) -> Vec<&PrivilegeGrant> {
        self.privilege_grants.iter().filter(|g| g.grantee == grantee).collect()
    }

    /// Get all grants for a specific object.
    pub fn get_grants_for_object(&self, object: &str) -> Vec<&PrivilegeGrant> {
        self.privilege_grants.iter().filter(|g| g.object == object).collect()
    }

    /// Get all privilege grants.
    pub fn get_all_grants(&self) -> &[PrivilegeGrant] {
        &self.privilege_grants
    }

    /// Remove privilege grants matching the given criteria.
    ///
    /// Returns the number of grants removed.
    pub fn remove_grants(
        &mut self,
        object: &str,
        grantee: &str,
        privilege: &vibesql_ast::PrivilegeType,
        grant_option_only: bool,
    ) -> usize {
        let initial_len = self.privilege_grants.len();

        if grant_option_only {
            // Only remove the grant option, not the privilege itself
            for grant in self.privilege_grants.iter_mut() {
                if grant.object == object
                    && grant.grantee == grantee
                    && grant.privilege == *privilege
                {
                    grant.with_grant_option = false;
                }
            }
            // Return 0 since we didn't remove any grants, just modified them
            0
        } else {
            // Remove the entire grant
            self.privilege_grants.retain(|g| {
                !(g.object == object && g.grantee == grantee && g.privilege == *privilege)
            });

            initial_len - self.privilege_grants.len()
        }
    }

    /// Check if there are dependent grants (grants made by the grantee with grant option).
    ///
    /// Used for RESTRICT behavior - errors if dependent grants exist.
    pub fn has_dependent_grants(
        &self,
        object: &str,
        grantee: &str,
        privilege: &vibesql_ast::PrivilegeType,
    ) -> bool {
        // Check if the grantee has granted this privilege to others
        self.privilege_grants
            .iter()
            .any(|g| g.object == object && g.grantor == grantee && g.privilege == *privilege)
    }

    /// Create a new role.
    pub fn create_role(&mut self, name: String) -> Result<(), CatalogError> {
        if self.roles.contains(&name) {
            return Err(CatalogError::RoleAlreadyExists(name));
        }
        self.roles.insert(name);
        Ok(())
    }

    /// Drop a role.
    pub fn drop_role(&mut self, name: &str) -> Result<(), CatalogError> {
        if !self.roles.contains(name) {
            return Err(CatalogError::RoleNotFound(name.to_string()));
        }
        self.roles.remove(name);
        Ok(())
    }

    /// Check if a role exists.
    pub fn role_exists(&self, name: &str) -> bool {
        self.roles.contains(name)
    }

    /// List all roles.
    pub fn list_roles(&self) -> Vec<String> {
        self.roles.iter().cloned().collect()
    }
}
