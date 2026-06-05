//! Dialect-agnostic SQL schema normalization.
//!
//! Parses DDL with sqlparser, normalizes identifier quoting, sorts columns
//! and constraints, then renders back to a canonical multi-line form.
//! This ensures schema comparisons are insensitive to column order and
//! quoting style differences.

use std::collections::{HashMap, HashSet};

use sqlparser::ast::{DataType, ObjectNamePart, Statement};
use sqlparser::dialect::Dialect;
use sqlparser::parser::Parser;

/// Per-table set of column names that use an array type (e.g. PostgreSQL `TEXT[]`).
///
/// Keyed by table name; the value is the set of array-typed column names.
pub type ArrayColumns = HashMap<String, HashSet<String>>;

/// Normalize a DDL string for comparison.
///
/// For `CREATE TABLE` statements, sorts columns by name and constraints
/// by their string representation, strips identifier quoting, and renders
/// each column on its own line. Non-table DDL is whitespace-normalized.
pub fn normalize_ddl(dialect: &dyn Dialect, sql: &str) -> String {
    let parsed = match Parser::parse_sql(dialect, sql) {
        Ok(stmts) => stmts,
        Err(_) => return format_unparseable(sql),
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

/// Extract table names from a DDL dump.
///
/// Parses each statement (separated by `;` + blank line) and collects
/// the unquoted name of every `CREATE TABLE` found. Returns names in
/// the order they appear.
pub fn table_names(dialect: &dyn Dialect, ddl: &str) -> Vec<String> {
    let statements: Vec<&str> = ddl
        .split(";\n\n")
        .map(|s| s.trim().trim_end_matches(';').trim())
        .filter(|s| !s.is_empty())
        .collect();

    let mut names = Vec::new();
    for sql in statements {
        if let Ok(parsed) = Parser::parse_sql(dialect, sql) {
            for stmt in parsed {
                if let Statement::CreateTable(ct) = stmt {
                    let name = ct
                        .name
                        .0
                        .iter()
                        .filter_map(|part| match part {
                            ObjectNamePart::Identifier(ident) => Some(ident.value.clone()),
                            ObjectNamePart::Function(_) => None,
                        })
                        .next_back();
                    if let Some(n) = name {
                        names.push(n);
                    }
                }
            }
        }
    }
    names
}

/// Collect the set of column names for every table in a DDL dump.
///
/// Keyed by table name. Used to determine which columns a migration drops
/// (present in the previous schema, absent in the desired schema), so the
/// down-migration data check can skip values it cannot possibly restore.
pub fn table_columns(dialect: &dyn Dialect, ddl: &str) -> HashMap<String, HashSet<String>> {
    let statements: Vec<&str> = ddl
        .split(";\n\n")
        .map(|s| s.trim().trim_end_matches(';').trim())
        .filter(|s| !s.is_empty())
        .collect();

    let mut result: HashMap<String, HashSet<String>> = HashMap::new();
    for sql in statements {
        let Ok(parsed) = Parser::parse_sql(dialect, sql) else {
            continue;
        };
        for stmt in parsed {
            let Statement::CreateTable(ct) = stmt else {
                continue;
            };
            let Some(table) = ct
                .name
                .0
                .iter()
                .filter_map(|part| match part {
                    ObjectNamePart::Identifier(ident) => Some(ident.value.clone()),
                    ObjectNamePart::Function(_) => None,
                })
                .next_back()
            else {
                continue;
            };
            let cols = ct.columns.iter().map(|col| col.name.value.clone()).collect();
            result.insert(table, cols);
        }
    }
    result
}

/// Columns present in `previous` but absent in `desired`, per table.
///
/// These are the columns an UP migration drops. A DOWN migration can re-add
/// the column but cannot restore the original values, so the down-migration
/// data check omits them. Only tables present in both schemas are considered;
/// a wholesale table drop is handled separately.
pub fn dropped_columns(dialect: &dyn Dialect, previous: &str, desired: &str) -> HashMap<String, HashSet<String>> {
    let previous_cols = table_columns(dialect, previous);
    let desired_cols = table_columns(dialect, desired);

    let mut result = HashMap::new();
    for (table, cols) in previous_cols {
        let Some(kept) = desired_cols.get(&table) else {
            continue;
        };
        let dropped: HashSet<String> = cols.difference(kept).cloned().collect();
        if !dropped.is_empty() {
            result.insert(table, dropped);
        }
    }
    result
}

/// Identify array-typed columns per table in a DDL dump.
///
/// Only PostgreSQL has true array column types; for other dialects this
/// returns an empty map because their DDL never parses to [`DataType::Array`].
/// Used so seed values for array columns render as array literals (`'{...}'`)
/// rather than JSON (`'[...]'`).
pub fn array_columns(dialect: &dyn Dialect, ddl: &str) -> ArrayColumns {
    let statements: Vec<&str> = ddl
        .split(";\n\n")
        .map(|s| s.trim().trim_end_matches(';').trim())
        .filter(|s| !s.is_empty())
        .collect();

    let mut result = ArrayColumns::new();
    for sql in statements {
        let Ok(parsed) = Parser::parse_sql(dialect, sql) else {
            continue;
        };
        for stmt in parsed {
            let Statement::CreateTable(ct) = stmt else {
                continue;
            };
            let Some(table) = ct
                .name
                .0
                .iter()
                .filter_map(|part| match part {
                    ObjectNamePart::Identifier(ident) => Some(ident.value.clone()),
                    ObjectNamePart::Function(_) => None,
                })
                .next_back()
            else {
                continue;
            };
            let cols: HashSet<String> = ct
                .columns
                .iter()
                .filter(|col| is_array_type(&col.data_type))
                .map(|col| col.name.value.clone())
                .collect();
            if !cols.is_empty() {
                result.insert(table, cols);
            }
        }
    }
    result
}

/// Whether a sqlparser data type is an array type (e.g. `text[]`, `int[]`).
fn is_array_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Array(_))
}

/// Format SQL that sqlparser couldn't parse.
///
/// Splits on `;` boundaries and formats each statement individually,
/// so multi-statement strings still get proper separation.
fn format_unparseable(sql: &str) -> String {
    let statements: Vec<&str> = sql.split(';').map(|s| s.trim()).filter(|s| !s.is_empty()).collect();
    statements
        .iter()
        .map(|s| format_statement(s))
        .collect::<Vec<_>>()
        .join(";\n\n")
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
    fn test_postgres_multi_statement() {
        use sqlparser::dialect::PostgreSqlDialect;
        let pg = PostgreSqlDialect {};
        let sql = "DROP VIEW IF EXISTS group_members; DROP INDEX CONCURRENTLY IF EXISTS idx_users_email; ALTER TABLE users DROP COLUMN email";
        let normalized = normalize_ddl(&pg, sql);
        // Each statement should be on its own line, separated by ;\n\n
        assert!(
            normalized.contains(";\n\n"),
            "expected statement separation, got: {normalized}"
        );
    }

    #[test]
    fn test_table_names_basic() {
        let ddl =
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);\n\nCREATE TABLE groups (id INTEGER PRIMARY KEY)";
        let names = table_names(&sqlite(), ddl);
        assert_eq!(names, vec!["users", "groups"]);
    }

    #[test]
    fn test_table_names_ignores_views_and_indexes() {
        let ddl = "CREATE TABLE users (id INTEGER PRIMARY KEY);\n\nCREATE INDEX idx ON users (id);\n\nCREATE VIEW v AS SELECT * FROM users";
        let names = table_names(&sqlite(), ddl);
        assert_eq!(names, vec!["users"]);
    }

    #[test]
    fn test_table_names_strips_quotes() {
        let ddl = r#"CREATE TABLE "groups" (id INTEGER PRIMARY KEY)"#;
        let names = table_names(&sqlite(), ddl);
        assert_eq!(names, vec!["groups"]);
    }

    #[test]
    fn test_table_names_empty_ddl() {
        let names = table_names(&sqlite(), "");
        assert!(names.is_empty());
    }

    #[test]
    fn test_table_names_postgres_schema_qualified() {
        use sqlparser::dialect::PostgreSqlDialect;
        let pg = PostgreSqlDialect {};
        let ddl = "CREATE TABLE public.users (id SERIAL PRIMARY KEY)";
        let names = table_names(&pg, ddl);
        assert_eq!(names, vec!["users"]);
    }

    #[test]
    fn test_unparseable_falls_back() {
        let sql = "NOT VALID SQL {{{}}}";
        let normalized = normalize_ddl(&sqlite(), sql);
        assert_eq!(normalized, "NOT VALID SQL {{{}}}");
    }

    #[test]
    fn test_array_columns_postgres() {
        use sqlparser::dialect::PostgreSqlDialect;
        let pg = PostgreSqlDialect {};
        let ddl = "CREATE TABLE acl (id integer, args text[], tags varchar(10)[], reason text)";
        let cols = array_columns(&pg, ddl);
        let acl = cols.get("acl").expect("acl table");
        assert!(acl.contains("args"), "args should be array: {acl:?}");
        assert!(acl.contains("tags"), "tags should be array: {acl:?}");
        assert!(!acl.contains("reason"), "reason is not an array");
        assert!(!acl.contains("id"), "id is not an array");
    }

    #[test]
    fn test_table_columns_basic() {
        let ddl = "CREATE TABLE users (id INTEGER, name TEXT, created_at TEXT);\n\nCREATE TABLE groups (id INTEGER)";
        let cols = table_columns(&sqlite(), ddl);
        let users = cols.get("users").expect("users table");
        assert!(users.contains("id") && users.contains("name") && users.contains("created_at"));
        assert_eq!(users.len(), 3);
        assert_eq!(cols.get("groups").expect("groups").len(), 1);
    }

    #[test]
    fn test_table_columns_ignores_views_and_indexes() {
        let ddl = "CREATE TABLE users (id INTEGER);\n\nCREATE INDEX idx ON users (id);\n\nCREATE VIEW v AS SELECT * FROM users";
        let cols = table_columns(&sqlite(), ddl);
        assert_eq!(cols.len(), 1);
        assert!(cols.contains_key("users"));
    }

    #[test]
    fn test_dropped_columns_detects_removed_column() {
        let previous = "CREATE TABLE users (id INTEGER, name TEXT, created_at TEXT)";
        let desired = "CREATE TABLE users (id INTEGER, name TEXT, email TEXT)";
        let dropped = dropped_columns(&sqlite(), previous, desired);
        let users = dropped.get("users").expect("users");
        assert!(users.contains("created_at"));
        assert!(!users.contains("email"), "added columns are not dropped");
        assert_eq!(users.len(), 1);
    }

    #[test]
    fn test_dropped_columns_none_when_only_added() {
        let previous = "CREATE TABLE users (id INTEGER, name TEXT)";
        let desired = "CREATE TABLE users (id INTEGER, name TEXT, email TEXT)";
        assert!(dropped_columns(&sqlite(), previous, desired).is_empty());
    }

    #[test]
    fn test_dropped_columns_ignores_wholesale_table_drop() {
        let previous = "CREATE TABLE users (id INTEGER);\n\nCREATE TABLE legacy (id INTEGER, data TEXT)";
        let desired = "CREATE TABLE users (id INTEGER)";
        assert!(dropped_columns(&sqlite(), previous, desired).is_empty());
    }

    #[test]
    fn test_array_columns_none() {
        let ddl = "CREATE TABLE users (id INTEGER, name TEXT)";
        let cols = array_columns(&sqlite(), ddl);
        assert!(cols.is_empty(), "no array columns expected: {cols:?}");
    }

    #[test]
    fn test_array_columns_schema_qualified() {
        use sqlparser::dialect::PostgreSqlDialect;
        let pg = PostgreSqlDialect {};
        let ddl = "CREATE TABLE public.acl (id integer, args text[])";
        let cols = array_columns(&pg, ddl);
        assert!(cols.get("acl").is_some_and(|c| c.contains("args")), "{cols:?}");
    }
}
