//! Advanced SQL:1999 object definitions
//! Minimal stub implementations for: DOMAIN, SEQUENCE, TYPE, COLLATION, CHARACTER SET, TRANSLATION

/// Domain - User-defined data type with constraints
#[derive(Debug, Clone)]
pub struct Domain {
    pub name: String,
    // TODO: Add base_type, constraints when implementing full functionality
}

impl Domain {
    pub fn new(name: String) -> Self {
        Domain { name }
    }
}

impl Default for Domain {
    fn default() -> Self {
        Domain { name: String::new() }
    }
}

/// Sequence - Auto-incrementing number generator
#[derive(Debug, Clone)]
pub struct Sequence {
    pub name: String,
    // TODO: Add start_with, increment_by, current_value when implementing full functionality
}

impl Sequence {
    pub fn new(name: String) -> Self {
        Sequence { name }
    }
}

impl Default for Sequence {
    fn default() -> Self {
        Sequence { name: String::new() }
    }
}

/// User-defined Type (UDT)
#[derive(Debug, Clone)]
pub struct UserDefinedType {
    pub name: String,
    // TODO: Add fields when implementing full functionality
}

impl UserDefinedType {
    pub fn new(name: String) -> Self {
        UserDefinedType { name }
    }
}

impl Default for UserDefinedType {
    fn default() -> Self {
        UserDefinedType { name: String::new() }
    }
}

/// Collation - String comparison rules
#[derive(Debug, Clone)]
pub struct Collation {
    pub name: String,
    // TODO: Add locale, pad_attribute when implementing full functionality
}

impl Collation {
    pub fn new(name: String) -> Self {
        Collation { name }
    }
}

impl Default for Collation {
    fn default() -> Self {
        Collation { name: String::new() }
    }
}

/// Character Set - Character encoding
#[derive(Debug, Clone)]
pub struct CharacterSet {
    pub name: String,
    // TODO: Add encoding details when implementing full functionality
}

impl CharacterSet {
    pub fn new(name: String) -> Self {
        CharacterSet { name }
    }
}

impl Default for CharacterSet {
    fn default() -> Self {
        CharacterSet { name: String::new() }
    }
}

/// Translation - Character set translation
#[derive(Debug, Clone)]
pub struct Translation {
    pub name: String,
    // TODO: Add from_charset, to_charset when implementing full functionality
}

impl Translation {
    pub fn new(name: String) -> Self {
        Translation { name }
    }
}

impl Default for Translation {
    fn default() -> Self {
        Translation { name: String::new() }
    }
}
