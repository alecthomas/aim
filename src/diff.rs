use similar::{ChangeTag, TextDiff};

/// Produce a unified-style text diff between two strings.
///
/// Returns an empty string if `left` and `right` are identical.
/// Shows 2 lines of context around each change.
pub fn text_diff(left: &str, right: &str) -> String {
    if left == right {
        return String::new();
    }

    let diff = TextDiff::from_lines(left, right);
    let mut output = Vec::new();

    for hunk in diff.unified_diff().context_radius(2).iter_hunks() {
        for change in hunk.iter_changes() {
            let prefix = match change.tag() {
                ChangeTag::Equal => "  ",
                ChangeTag::Delete => "- ",
                ChangeTag::Insert => "+ ",
            };
            // change.value() includes trailing newline; strip it.
            output.push(format!("{prefix}{}", change.value().trim_end_matches('\n')));
        }
    }

    output.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_identical() {
        assert!(text_diff("hello\nworld", "hello\nworld").is_empty());
    }

    #[test]
    fn test_addition() {
        let result = text_diff("a\nb", "a\nb\nc");
        assert!(result.contains("+ c"), "got: {result}");
    }

    #[test]
    fn test_removal() {
        let result = text_diff("a\nb\nc", "a\nc");
        assert!(result.contains("- b"), "got: {result}");
    }

    #[test]
    fn test_change() {
        let result = text_diff("a\nb\nc", "a\nB\nc");
        assert!(result.contains("- b"), "got: {result}");
        assert!(result.contains("+ B"), "got: {result}");
    }

    #[test]
    fn test_context_collapse() {
        let left = "1\n2\n3\n4\n5\n6\n7\n8\n9\n10";
        let right = "1\n2\n3\n4\nFIVE\n6\n7\n8\n9\n10";
        let result = text_diff(left, right);
        assert!(result.contains("- 5"), "got: {result}");
        assert!(result.contains("+ FIVE"), "got: {result}");
        // Lines far from the change should not appear.
        assert!(!result.contains("  1"), "should not contain line 1: {result}");
        assert!(!result.contains("  10"), "should not contain line 10: {result}");
    }
}
