//! Dialect-agnostic SQL schema normalization.
//!
//! Parses DDL with sqlparser, normalizes identifier quoting, sorts columns
//! and constraints, then renders back to a canonical multi-line form.
//! This ensures schema comparisons are insensitive to column order and
//! quoting style differences.

use sqlparser::ast::{ObjectNamePart, Statement};
use sqlparser::dialect::Dialect;
use sqlparser::parser::Parser;

/// Normalize a DDL string for comparison.
///
/// For `CREATE TABLE` statements, sorts columns by name and constraints
/// by their string representation, strips identifier quoting, and renders
/// each column on its own line. Non-table DDL is whitespace-normalized.
pub fn normalize_ddl(dialect: &dyn Dialect, sql: &str) -> String {
    let parsed = match Parser::parse_sql(dialect, sql) {
        Ok(stmts) => stmts,
        Err(_) => return format_statement(sql),
    };

    let mut normalized = Vec::with_capacity(parsed.len());
    for stmt in parsed {
        if let Statement::CreateTable(mut ct) = stmt {
            strip_quotes_from_name(&mut ct.name);
            for col in &mut ct.columns {
                col.name.quote_style = None;
            }
            ct.columns.sort_by_key(|c| c.name.value.clone());
            ct.constraints.sort_by_key(|c| c.to_string());
            normalized.push(render_create_table(&ct));
        } else {
            normalized.push(format_statement(&stmt.to_string()));
        }
    }

    normalized.join(";\n\n")
}

/// Render a CREATE TABLE statement with one column/constraint per line.
fn render_create_table(ct: &sqlparser::ast::CreateTable) -> String {
    let mut lines: Vec<String> = ct.columns.iter().map(|c| format!("  {c}")).collect();
    for constraint in &ct.constraints {
        lines.push(format!("  {constraint}"));
    }
    format!("CREATE TABLE {} (\n{}\n)", ct.name, lines.join(",\n"))
}

/// Strip quote styles from all identifiers in an ObjectName.
fn strip_quotes_from_name(name: &mut sqlparser::ast::ObjectName) {
    for part in &mut name.0 {
        if let ObjectNamePart::Identifier(ident) = part {
            ident.quote_style = None;
        }
    }
}

/// Format a SQL statement with line breaks before major clause keywords.
///
/// First normalizes whitespace to single spaces, then inserts newlines
/// before keywords like SELECT, FROM, JOIN, WHERE, etc. This produces
/// readable multi-line output for views and other complex statements.
///
/// NOTE: This operates on the string output of sqlparser's `Display` impl
/// rather than walking the AST directly, because implementing custom
/// formatting for every Statement variant would be significant work.
/// This is safe for DDL statements (which don't contain string literals
/// with embedded keywords), but could produce incorrect formatting for
/// statements containing string literals like `'SELECT FROM'`. Since we
/// only use this for schema objects from sqlite_master (DDL only), this
/// is acceptable.
fn format_statement(s: &str) -> String {
    let normalized: String = s.split_whitespace().collect::<Vec<_>>().join(" ");

    // Keywords that should start a new line (when not at the start).
    const BREAK_KEYWORDS: &[&str] = &[
        " SELECT ",
        " FROM ",
        " JOIN ",
        " LEFT JOIN ",
        " RIGHT JOIN ",
        " INNER JOIN ",
        " OUTER JOIN ",
        " CROSS JOIN ",
        " NATURAL JOIN ",
        " LEFT OUTER JOIN ",
        " RIGHT OUTER JOIN ",
        " FULL OUTER JOIN ",
        " WHERE ",
        " GROUP BY ",
        " HAVING ",
        " ORDER BY ",
        " LIMIT ",
        " UNION ",
        " UNION ALL ",
        " INTERSECT ",
        " EXCEPT ",
        " ADD COLUMN ",
        " DROP COLUMN ",
        " RENAME COLUMN ",
        " RENAME TO ",
        " SET ",
        " VALUES ",
    ];

    let upper = normalized.to_uppercase();
    let mut result = String::with_capacity(normalized.len() + 32);
    let mut pos = 0;

    while pos < normalized.len() {
        // Find the earliest keyword match from current position.
        let mut earliest: Option<(usize, usize)> = None; // (position, keyword_len)
        for kw in BREAK_KEYWORDS {
            let kw_upper = kw.to_uppercase();
            if let Some(found) = upper[pos..].find(&kw_upper) {
                let abs_pos = pos + found;
                // Only break if this isn't at the very start of the string.
                if abs_pos > 0 && (earliest.is_none() || abs_pos < earliest.expect("checked").0) {
                    earliest = Some((abs_pos, kw_upper.len()));
                }
            }
        }

        match earliest {
            Some((break_pos, kw_len)) => {
                // Append text up to the break point (excluding the space before keyword).
                result.push_str(&normalized[pos..break_pos]);
                result.push('\n');
                // Append keyword and continue (skip the leading space).
                let kw_end = break_pos + kw_len;
                result.push_str(normalized[break_pos + 1..kw_end].trim());
                result.push(' ');
                pos = kw_end;
            }
            None => {
                result.push_str(&normalized[pos..]);
                break;
            }
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::SQLiteDialect;

    fn sqlite() -> SQLiteDialect {
        SQLiteDialect {}
    }

    #[test]
    fn test_sorts_columns() {
        let sql = "CREATE TABLE t (b TEXT, a INT, c REAL)";
        let normalized = normalize_ddl(&sqlite(), sql);
        let a_pos = normalized.find("a INT").expect("a INT");
        let b_pos = normalized.find("b TEXT").expect("b TEXT");
        let c_pos = normalized.find("c REAL").expect("c REAL");
        assert!(a_pos < b_pos, "a before b: {normalized}");
        assert!(b_pos < c_pos, "b before c: {normalized}");
    }

    #[test]
    fn test_strips_quotes() {
        let sql = r#"CREATE TABLE "t" ("a" INT, "b" TEXT)"#;
        let normalized = normalize_ddl(&sqlite(), sql);
        assert!(!normalized.contains('"'), "quotes not stripped: {normalized}");
    }

    #[test]
    fn test_multiline_output() {
        let sql = "CREATE TABLE t (a INT, b TEXT)";
        let normalized = normalize_ddl(&sqlite(), sql);
        assert!(normalized.contains('\n'), "should be multiline: {normalized}");
    }

    #[test]
    fn test_non_table_whitespace_normalized() {
        let sql = "CREATE   INDEX   idx   ON   t  (a)";
        let normalized = normalize_ddl(&sqlite(), sql);
        assert!(!normalized.contains("  "), "double spaces: {normalized}");
    }

    #[test]
    fn test_view_multiline() {
        let sql = "CREATE VIEW group_members AS SELECT g.name AS group_name, u.name AS user_name FROM groups g JOIN groups_users gu ON g.id = gu.group_id JOIN users u ON gu.user_id = u.id";
        let normalized = normalize_ddl(&sqlite(), sql);
        assert!(normalized.contains("\nSELECT "), "SELECT on new line: {normalized}");
        assert!(normalized.contains("\nFROM "), "FROM on new line: {normalized}");
        assert!(normalized.contains("\nJOIN "), "JOIN on new line: {normalized}");
    }

    #[test]
    fn test_unparseable_falls_back() {
        let sql = "NOT VALID SQL {{{}}}";
        let normalized = normalize_ddl(&sqlite(), sql);
        assert_eq!(normalized, "NOT VALID SQL {{{}}}");
    }
}
