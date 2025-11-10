// Shared test utilities for constraint testing
#[path = "update_constraint_tests/constraint_test_utils.rs"]
mod constraint_test_utils;

// Organized constraint test modules
#[path = "update_constraint_tests/not_null_constraints.rs"]
mod not_null_constraints;

#[path = "update_constraint_tests/primary_key_constraints.rs"]
mod primary_key_constraints;

#[path = "update_constraint_tests/unique_constraints.rs"]
mod unique_constraints;

#[path = "update_constraint_tests/check_constraints.rs"]
mod check_constraints;
