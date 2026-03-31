/// Produce a unified-style text diff between two strings.
///
/// Returns an empty string if `left` and `right` are identical.
/// Used to give the LLM feedback on schema mismatches during retry.
pub fn text_diff(left: &str, right: &str) -> String {
    let left_lines: Vec<&str> = left.lines().collect();
    let right_lines: Vec<&str> = right.lines().collect();

    if left_lines == right_lines {
        return String::new();
    }

    // Simple line-by-line diff using longest common subsequence.
    let lcs = lcs_table(&left_lines, &right_lines);
    let mut output = Vec::new();
    build_diff(
        &lcs,
        &left_lines,
        &right_lines,
        left_lines.len(),
        right_lines.len(),
        &mut output,
    );

    output.join("\n")
}

/// Build the LCS length table.
fn lcs_table(left: &[&str], right: &[&str]) -> Vec<Vec<usize>> {
    let m = left.len();
    let n = right.len();
    let mut table = vec![vec![0usize; n + 1]; m + 1];

    for i in 1..=m {
        for j in 1..=n {
            table[i][j] = if left[i - 1] == right[j - 1] {
                table[i - 1][j - 1] + 1
            } else {
                table[i - 1][j].max(table[i][j - 1])
            };
        }
    }
    table
}

/// Walk the LCS table to produce diff lines.
fn build_diff(table: &[Vec<usize>], left: &[&str], right: &[&str], i: usize, j: usize, output: &mut Vec<String>) {
    if i > 0 && j > 0 && left[i - 1] == right[j - 1] {
        build_diff(table, left, right, i - 1, j - 1, output);
        output.push(format!("  {}", left[i - 1]));
    } else if j > 0 && (i == 0 || table[i][j - 1] >= table[i - 1][j]) {
        build_diff(table, left, right, i, j - 1, output);
        output.push(format!("+ {}", right[j - 1]));
    } else if i > 0 {
        build_diff(table, left, right, i - 1, j, output);
        output.push(format!("- {}", left[i - 1]));
    }
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
}
