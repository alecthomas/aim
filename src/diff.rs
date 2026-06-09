use similar::{ChangeTag, TextDiff};

/// Produce a unified-style text diff between two strings.
///
/// Returns an empty string if `left` and `right` are identical.
/// Shows 2 lines of context around each change, and prefixes each hunk with a
/// `@@ <statement> @@` header naming the enclosing statement (e.g. the
/// `CREATE TABLE ...` line) so changes deep inside a long statement still show
/// which object they belong to.
pub fn text_diff(left: &str, right: &str) -> String {
    if left == right {
        return String::new();
    }

    let left_sections = line_sections(left);
    let right_sections = line_sections(right);
    let diff = TextDiff::from_lines(left, right);
    let mut output = Vec::new();

    for hunk in diff.unified_diff().context_radius(2).iter_hunks() {
        let changes: Vec<_> = hunk.iter_changes().collect();

        // Header from the first actual change (fallback: first line) so the
        // label reflects the object being modified, not trailing context.
        if let Some(header) = changes
            .iter()
            .find(|c| c.tag() != ChangeTag::Equal)
            .or_else(|| changes.first())
            .and_then(|c| section_for(c, &left_sections, &right_sections))
            .filter(|h| !h.is_empty())
        {
            output.push(format!("@@ {header} @@"));
        }

        for change in &changes {
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

/// Resolve the statement header for a change from whichever side it belongs to.
fn section_for(change: &similar::Change<&str>, left_sections: &[String], right_sections: &[String]) -> Option<String> {
    change
        .old_index()
        .and_then(|i| left_sections.get(i))
        .or_else(|| change.new_index().and_then(|i| right_sections.get(i)))
        .cloned()
}

/// For each line in `text`, the header of the statement it belongs to.
///
/// Statements in a normalized schema are separated by blank lines, so the
/// header is the first non-blank line of each block (e.g. `CREATE TABLE foo (`).
fn line_sections(text: &str) -> Vec<String> {
    let mut sections = Vec::new();
    let mut current = String::new();
    let mut at_block_start = true;
    for line in text.lines() {
        if line.trim().is_empty() {
            at_block_start = true;
        } else if at_block_start {
            current = line.trim_end().to_string();
            at_block_start = false;
        }
        sections.push(current.clone());
    }
    sections
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
    fn test_section_header_names_enclosing_statement() {
        // A change deep inside a long CREATE TABLE is out of the 2-line context
        // window, so the header is what shows which table it belongs to.
        let left = "CREATE TABLE foo (\n  a INT,\n  b INT,\n  c INT,\n  d INT,\n  e INT\n)";
        let right = "CREATE TABLE foo (\n  a INT,\n  b INT,\n  c INT,\n  d INT,\n  e INT,\n  f INT\n)";
        let result = text_diff(left, right);
        assert!(
            result.contains("@@ CREATE TABLE foo ("),
            "missing section header: {result}"
        );
        assert!(result.contains("+   f INT"), "got: {result}");
    }

    #[test]
    fn test_section_header_uses_correct_block() {
        // The change is in the first statement, so its header must name that
        // statement and not leak the unrelated table that follows.
        let left = "CREATE TYPE color AS ENUM ('red', 'green')\n\nCREATE TABLE foo (\n  a INT\n)";
        let right = "CREATE TYPE color AS ENUM ('red', 'green', 'blue')\n\nCREATE TABLE foo (\n  a INT\n)";
        let result = text_diff(left, right);
        assert!(
            result.contains("@@ CREATE TYPE color AS ENUM ('red', 'green') @@"),
            "got: {result}"
        );
        assert!(
            !result.contains("@@ CREATE TABLE foo"),
            "unrelated header leaked: {result}"
        );
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
