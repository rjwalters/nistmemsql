/// SQL LIKE pattern matching
/// Supports wildcards:
/// - % matches any sequence of characters (including empty)
/// - _ matches exactly one character
///
/// This is a case-sensitive match following SQL:1999 semantics
pub(crate) fn like_match(text: &str, pattern: &str) -> bool {
    like_match_recursive(text.as_bytes(), pattern.as_bytes(), 0, 0)
}

/// Recursive helper for LIKE pattern matching
fn like_match_recursive(text: &[u8], pattern: &[u8], text_pos: usize, pattern_pos: usize) -> bool {
    // If we've consumed the entire pattern
    if pattern_pos >= pattern.len() {
        // Match succeeds if we've also consumed all of text
        return text_pos >= text.len();
    }

    let pattern_char = pattern[pattern_pos];

    match pattern_char {
        b'%' => {
            // % matches zero or more characters
            // Try matching with % consuming 0 chars, 1 char, 2 chars, etc.
            for skip in 0..=(text.len() - text_pos) {
                if like_match_recursive(text, pattern, text_pos + skip, pattern_pos + 1) {
                    return true;
                }
            }
            false
        }
        b'_' => {
            // _ matches exactly one character
            if text_pos >= text.len() {
                // No character left to match
                return false;
            }
            // Skip one character in text and one in pattern
            like_match_recursive(text, pattern, text_pos + 1, pattern_pos + 1)
        }
        _ => {
            // Regular character must match exactly
            if text_pos >= text.len() {
                // No character left in text
                return false;
            }
            if text[text_pos] != pattern_char {
                // Characters don't match
                return false;
            }
            // Characters match, continue
            like_match_recursive(text, pattern, text_pos + 1, pattern_pos + 1)
        }
    }
}
