use std::collections::{HashMap, HashSet};

use crate::agent::tools::TableSeedData;
use crate::schema::ArrayColumns;

/// Sorted column names from `row`, excluding any present in `excluded`.
///
/// Used to drop columns that a migration removes (and that a down migration
/// cannot restore) from data-preservation predicates.
fn retained_columns<'a>(
    row: &'a HashMap<String, serde_json::Value>,
    excluded: Option<&HashSet<String>>,
) -> Vec<&'a str> {
    let mut cols: Vec<&str> = row
        .keys()
        .map(String::as_str)
        .filter(|c| !excluded.is_some_and(|e| e.contains(*c)))
        .collect();
    cols.sort_unstable();
    cols
}

/// Converts a JSON value to a SQL literal string.
///
/// When `is_array` is true, a JSON array is rendered as a PostgreSQL array
/// literal (`'{...}'`) rather than as JSON text (`'[...]'`), so values for
/// array-typed columns insert correctly. Non-array JSON values are unaffected.
fn json_value_to_sql_literal(v: &serde_json::Value, is_array: bool) -> String {
    match v {
        serde_json::Value::Null => "NULL".to_string(),
        serde_json::Value::Bool(b) => {
            if *b {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            }
        }
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => {
            format!("'{}'", s.replace('\'', "''"))
        }
        serde_json::Value::Array(items) if is_array => {
            // PostgreSQL array literal, e.g. {"a","b"} or {1,2,3}.
            format!("'{}'", pg_array_body(items).replace('\'', "''"))
        }
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            // JSON / JSONB column: keep the JSON text form.
            format!("'{}'", v.to_string().replace('\'', "''"))
        }
    }
}

/// Render a JSON array as a PostgreSQL array literal body, e.g. `{"a","b"}`.
fn pg_array_body(items: &[serde_json::Value]) -> String {
    let elements: Vec<String> = items.iter().map(pg_array_element).collect();
    format!("{{{}}}", elements.join(","))
}

/// Render a single element of a PostgreSQL array literal.
///
/// Strings are double-quoted with `\` and `"` escaped; numbers and booleans
/// are bare; nested arrays recurse; null is the unquoted `NULL` token.
fn pg_array_element(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Null => "NULL".to_string(),
        serde_json::Value::Bool(b) => {
            if *b {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Array(items) => pg_array_body(items),
        serde_json::Value::String(s) => quote_array_element(s),
        // Objects are unusual inside array columns; store their JSON text.
        serde_json::Value::Object(_) => quote_array_element(&v.to_string()),
    }
}

/// Double-quote and escape a string for use as a PostgreSQL array element.
fn quote_array_element(s: &str) -> String {
    let escaped = s.replace('\\', "\\\\").replace('"', "\\\"");
    format!("\"{escaped}\"")
}

/// Generates INSERT statements for all tables' seed rows.
///
/// For each table (sorted alphabetically), produces one `INSERT INTO`
/// statement per row with columns in alphabetical order. Tables are
/// separated by a blank line.
pub fn build_insert_statements(seed_data: &HashMap<String, TableSeedData>, array_cols: &ArrayColumns) -> String {
    let mut tables: Vec<&String> = seed_data.keys().collect();
    tables.sort();

    let mut sections = Vec::with_capacity(tables.len());

    for table in tables {
        let table_seed = &seed_data[table];
        let stmts: Vec<String> = table_seed
            .rows
            .iter()
            .map(|row| {
                let mut cols: Vec<&String> = row.keys().collect();
                cols.sort();
                let col_list = cols.iter().map(|c| c.as_str()).collect::<Vec<_>>().join(", ");
                let val_list = cols
                    .iter()
                    .map(|c| json_value_to_sql_literal(&row[c.as_str()], is_array_col(array_cols, table, c)))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("INSERT INTO {table} ({col_list}) VALUES ({val_list});")
            })
            .collect();
        if !stmts.is_empty() {
            sections.push(stmts.join("\n"));
        }
    }

    sections.join("\n\n")
}

/// Which migration direction to extract expected columns from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Up,
    Down,
}

/// Whether `col` in `table` is an array-typed column.
fn is_array_col(array_cols: &ArrayColumns, table: &str, col: &str) -> bool {
    array_cols.get(table).is_some_and(|cols| cols.contains(col))
}

/// Builds a WHERE condition for a single column and value.
///
/// Uses `IS NULL` for null values, `=` for everything else.
fn where_condition(col: &str, val: &serde_json::Value, is_array: bool) -> String {
    if val.is_null() {
        format!("{col} IS NULL")
    } else {
        format!("{col} = {}", json_value_to_sql_literal(val, is_array))
    }
}

/// Generates SELECT statements that verify expected row values exist.
///
/// For each expected row (from `expected_after_up` or `expected_after_down`),
/// produces a `SELECT col1, col2 FROM table WHERE col1 = val1 AND col2 = val2;`.
/// Columns are sorted alphabetically. Tables are sorted alphabetically,
/// with a blank line between tables.
///
/// Columns listed in `exclude` for a table are omitted from the predicates
/// (e.g. columns a migration drops, which a down migration cannot restore).
pub fn build_select_statements(
    seed_data: &HashMap<String, TableSeedData>,
    direction: Direction,
    array_cols: &ArrayColumns,
    exclude: &HashMap<String, HashSet<String>>,
) -> String {
    let mut tables: Vec<&String> = seed_data.keys().collect();
    tables.sort();

    let mut sections = Vec::with_capacity(tables.len());

    for table in tables {
        let seed = &seed_data[table.as_str()];
        let expected = match direction {
            Direction::Up => &seed.expected_after_up,
            Direction::Down => &seed.expected_after_down,
        };
        let excluded = exclude.get(table.as_str());
        let stmts: Vec<String> = expected
            .iter()
            .map(|row| {
                let cols = retained_columns(row, excluded);
                if cols.is_empty() {
                    return format!("SELECT * FROM {table};");
                }
                let col_list = cols.join(", ");
                let conditions = cols
                    .iter()
                    .map(|c| where_condition(c, &row[*c], is_array_col(array_cols, table, c)))
                    .collect::<Vec<_>>()
                    .join(" AND ");
                format!("SELECT {col_list} FROM {table} WHERE {conditions};")
            })
            .collect();
        if !stmts.is_empty() {
            sections.push(stmts.join("\n"));
        }
    }

    sections.join("\n\n")
}

/// A data-preservation check derived from seed data, expressed as a
/// `SELECT COUNT(*)` query plus the count it is expected to return.
///
/// Checks are executed by the agent against a database that has had the
/// migration applied, to confirm rows were preserved/transformed correctly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowCheck {
    /// The total number of rows in `table` must equal `count`.
    Total {
        table: String,
        count_sql: String,
        count: usize,
    },
    /// At least one row in `table` must match the expected column values.
    Exists {
        table: String,
        count_sql: String,
        expected: String,
    },
}

impl RowCheck {
    /// The table this check applies to.
    pub fn table(&self) -> &str {
        match self {
            RowCheck::Total { table, .. } | RowCheck::Exists { table, .. } => table,
        }
    }

    /// The `SELECT COUNT(*)` query to execute.
    pub fn count_sql(&self) -> &str {
        match self {
            RowCheck::Total { count_sql, .. } | RowCheck::Exists { count_sql, .. } => count_sql,
        }
    }
}

/// Render a row's column values as a stable, human-readable string.
fn render_row(row: &HashMap<String, serde_json::Value>) -> String {
    let mut cols: Vec<&String> = row.keys().collect();
    cols.sort();
    let parts: Vec<String> = cols.iter().map(|c| format!("{c}={}", row[*c])).collect();
    format!("{{{}}}", parts.join(", "))
}

/// Build executable data-preservation checks for the given direction.
///
/// For each table (sorted) this produces a total-row-count check plus one
/// existence check per expected row. Existence checks reuse the same
/// `=` / `IS NULL` predicates as [`build_select_statements`], so the
/// comparison logic stays in one place.
///
/// Columns listed in `exclude` for a table are omitted from the existence
/// predicates (e.g. columns a migration drops, which a down migration cannot
/// restore). Total row-count checks are unaffected.
pub fn build_row_checks(
    seed_data: &HashMap<String, TableSeedData>,
    direction: Direction,
    array_cols: &ArrayColumns,
    exclude: &HashMap<String, HashSet<String>>,
) -> Vec<RowCheck> {
    let mut tables: Vec<&String> = seed_data.keys().collect();
    tables.sort();

    let mut checks = Vec::new();
    for table in tables {
        let seed = &seed_data[table.as_str()];
        let expected = match direction {
            Direction::Up => &seed.expected_after_up,
            Direction::Down => &seed.expected_after_down,
        };
        let excluded = exclude.get(table.as_str());

        checks.push(RowCheck::Total {
            table: table.clone(),
            count_sql: format!("SELECT COUNT(*) FROM {table}"),
            count: expected.len(),
        });

        for row in expected {
            let cols = retained_columns(row, excluded);
            let count_sql = if cols.is_empty() {
                format!("SELECT COUNT(*) FROM {table}")
            } else {
                let conditions = cols
                    .iter()
                    .map(|c| where_condition(c, &row[*c], is_array_col(array_cols, table, c)))
                    .collect::<Vec<_>>()
                    .join(" AND ");
                format!("SELECT COUNT(*) FROM {table} WHERE {conditions}")
            };
            checks.push(RowCheck::Exists {
                table: table.clone(),
                count_sql,
                expected: render_row(row),
            });
        }
    }
    checks
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashSet;

    /// No array columns (the common case for non-PostgreSQL schemas).
    fn no_arrays() -> ArrayColumns {
        ArrayColumns::new()
    }

    #[test]
    fn literal_null() {
        assert_eq!(json_value_to_sql_literal(&json!(null), false), "NULL");
    }

    #[test]
    fn literal_bool() {
        assert_eq!(json_value_to_sql_literal(&json!(true), false), "TRUE");
        assert_eq!(json_value_to_sql_literal(&json!(false), false), "FALSE");
    }

    #[test]
    fn literal_integer() {
        assert_eq!(json_value_to_sql_literal(&json!(42), false), "42");
    }

    #[test]
    fn literal_float() {
        assert_eq!(json_value_to_sql_literal(&json!(2.5), false), "2.5");
    }

    #[test]
    fn literal_string() {
        assert_eq!(json_value_to_sql_literal(&json!("hello"), false), "'hello'");
    }

    #[test]
    fn literal_string_with_quotes() {
        assert_eq!(
            json_value_to_sql_literal(&json!("it's a test"), false),
            "'it''s a test'"
        );
    }

    #[test]
    fn literal_array_as_json() {
        // Non-array column (e.g. JSON/JSONB): keep JSON text form.
        let val = json!([1, 2, 3]);
        assert_eq!(json_value_to_sql_literal(&val, false), "'[1,2,3]'");
    }

    #[test]
    fn literal_array_as_pg_array() {
        // Array column: render as a PostgreSQL array literal.
        assert_eq!(json_value_to_sql_literal(&json!([1, 2, 3]), true), "'{1,2,3}'");
        assert_eq!(json_value_to_sql_literal(&json!(["a", "b"]), true), r#"'{"a","b"}'"#);
        assert_eq!(json_value_to_sql_literal(&json!([]), true), "'{}'");
        assert_eq!(json_value_to_sql_literal(&json!([true, false]), true), "'{true,false}'");
    }

    #[test]
    fn literal_array_element_escaping() {
        // Commas, quotes and backslashes in string elements are escaped.
        assert_eq!(
            json_value_to_sql_literal(&json!(["a,b", "c\"d"]), true),
            r#"'{"a,b","c\"d"}'"#
        );
        // Single quotes are doubled for the surrounding SQL literal.
        assert_eq!(
            json_value_to_sql_literal(&json!(["O'Brien"]), true),
            r#"'{"O''Brien"}'"#
        );
    }

    #[test]
    fn literal_object() {
        let val = json!({"a": 1});
        assert_eq!(json_value_to_sql_literal(&val, false), "'{\"a\":1}'");
    }

    fn make_seed_data() -> HashMap<String, TableSeedData> {
        let mut data = HashMap::new();
        data.insert(
            "users".to_string(),
            TableSeedData {
                rows: vec![
                    HashMap::from([("id".to_string(), json!(1)), ("name".to_string(), json!("alice"))]),
                    HashMap::from([("id".to_string(), json!(2)), ("name".to_string(), json!("bob"))]),
                ],
                expected_after_up: vec![],
                expected_after_down: vec![],
            },
        );
        data.insert(
            "orders".to_string(),
            TableSeedData {
                rows: vec![HashMap::from([
                    ("id".to_string(), json!(10)),
                    ("user_id".to_string(), json!(1)),
                    ("amount".to_string(), json!(99.5)),
                ])],
                expected_after_up: vec![],
                expected_after_down: vec![],
            },
        );
        data
    }

    #[test]
    fn insert_statements_multiple_tables() {
        let data = make_seed_data();
        let sql = build_insert_statements(&data, &no_arrays());

        // "orders" comes before "users" alphabetically
        let expected = "\
INSERT INTO orders (amount, id, user_id) VALUES (99.5, 10, 1);\n\
\n\
INSERT INTO users (id, name) VALUES (1, 'alice');\n\
INSERT INTO users (id, name) VALUES (2, 'bob');";

        assert_eq!(sql, expected);
    }

    #[test]
    fn insert_statements_empty() {
        let data: HashMap<String, TableSeedData> = HashMap::new();
        assert_eq!(build_insert_statements(&data, &no_arrays()), "");
    }

    #[test]
    fn insert_statements_render_array_columns_as_pg_literals() {
        let data = HashMap::from([(
            "acl".to_string(),
            TableSeedData {
                rows: vec![HashMap::from([
                    ("id".to_string(), json!(1)),
                    ("args".to_string(), json!(["read", "write"])),
                    ("tags".to_string(), json!([])),
                ])],
                expected_after_up: vec![],
                expected_after_down: vec![],
            },
        )]);
        let array_cols: ArrayColumns = HashMap::from([(
            "acl".to_string(),
            HashSet::from(["args".to_string(), "tags".to_string()]),
        )]);
        let sql = build_insert_statements(&data, &array_cols);
        // args/tags use PG array literals; id stays a bare integer.
        assert_eq!(
            sql,
            r#"INSERT INTO acl (args, id, tags) VALUES ('{"read","write"}', 1, '{}');"#
        );
    }

    #[test]
    fn select_statements_match_array_columns() {
        let data = HashMap::from([(
            "acl".to_string(),
            TableSeedData {
                rows: vec![],
                expected_after_up: vec![HashMap::from([
                    ("id".to_string(), json!(1)),
                    ("args".to_string(), json!(["read"])),
                ])],
                expected_after_down: vec![],
            },
        )]);
        let array_cols: ArrayColumns = HashMap::from([("acl".to_string(), HashSet::from(["args".to_string()]))]);
        let sql = build_select_statements(&data, Direction::Up, &array_cols, &no_arrays());
        assert_eq!(sql, r#"SELECT args, id FROM acl WHERE args = '{"read"}' AND id = 1;"#);
    }

    fn make_seed_data_with_expected() -> HashMap<String, TableSeedData> {
        let mut data = HashMap::new();
        data.insert(
            "users".to_string(),
            TableSeedData {
                rows: vec![
                    HashMap::from([("id".to_string(), json!(1)), ("name".to_string(), json!("alice"))]),
                    HashMap::from([("id".to_string(), json!(2)), ("name".to_string(), json!("bob"))]),
                ],
                expected_after_up: vec![
                    HashMap::from([
                        ("id".to_string(), json!(1)),
                        ("name".to_string(), json!("alice")),
                        ("email".to_string(), json!("")),
                    ]),
                    HashMap::from([
                        ("id".to_string(), json!(2)),
                        ("name".to_string(), json!("bob")),
                        ("email".to_string(), json!("")),
                    ]),
                ],
                expected_after_down: vec![
                    HashMap::from([("id".to_string(), json!(1)), ("name".to_string(), json!("alice"))]),
                    HashMap::from([("id".to_string(), json!(2)), ("name".to_string(), json!("bob"))]),
                ],
            },
        );
        data.insert(
            "orders".to_string(),
            TableSeedData {
                rows: vec![HashMap::from([
                    ("id".to_string(), json!(10)),
                    ("user_id".to_string(), json!(1)),
                ])],
                expected_after_up: vec![HashMap::from([
                    ("id".to_string(), json!(10)),
                    ("user_id".to_string(), json!(1)),
                ])],
                expected_after_down: vec![HashMap::from([
                    ("id".to_string(), json!(10)),
                    ("user_id".to_string(), json!(1)),
                ])],
            },
        );
        data
    }

    #[test]
    fn select_statements_after_up() {
        let data = make_seed_data_with_expected();
        let sql = build_select_statements(&data, Direction::Up, &no_arrays(), &no_arrays());

        let expected = "\
SELECT id, user_id FROM orders WHERE id = 10 AND user_id = 1;\n\
\n\
SELECT email, id, name FROM users WHERE email = '' AND id = 1 AND name = 'alice';\n\
SELECT email, id, name FROM users WHERE email = '' AND id = 2 AND name = 'bob';";

        assert_eq!(sql, expected);
    }

    #[test]
    fn select_statements_after_down() {
        let data = make_seed_data_with_expected();
        let sql = build_select_statements(&data, Direction::Down, &no_arrays(), &no_arrays());

        let expected = "\
SELECT id, user_id FROM orders WHERE id = 10 AND user_id = 1;\n\
\n\
SELECT id, name FROM users WHERE id = 1 AND name = 'alice';\n\
SELECT id, name FROM users WHERE id = 2 AND name = 'bob';";

        assert_eq!(sql, expected);
    }

    #[test]
    fn select_statements_null_uses_is_null() {
        let data = HashMap::from([(
            "settings".to_string(),
            TableSeedData {
                rows: vec![],
                expected_after_up: vec![HashMap::from([
                    ("id".to_string(), json!(1)),
                    ("value".to_string(), json!(null)),
                ])],
                expected_after_down: vec![],
            },
        )]);
        let sql = build_select_statements(&data, Direction::Up, &no_arrays(), &no_arrays());
        assert_eq!(sql, "SELECT id, value FROM settings WHERE id = 1 AND value IS NULL;");
    }

    #[test]
    fn select_statements_skips_empty_expected() {
        let data = make_seed_data();
        let sql = build_select_statements(&data, Direction::Up, &no_arrays(), &no_arrays());
        assert_eq!(sql, "");
    }

    #[test]
    fn select_statements_empty() {
        let data: HashMap<String, TableSeedData> = HashMap::new();
        assert_eq!(
            build_select_statements(&data, Direction::Up, &no_arrays(), &no_arrays()),
            ""
        );
    }

    #[test]
    fn row_checks_total_and_existence() {
        let data = make_seed_data_with_expected();
        let checks = build_row_checks(&data, Direction::Up, &no_arrays(), &no_arrays());

        // orders: 1 total + 1 existence; users: 1 total + 2 existence = 5.
        assert_eq!(checks.len(), 5);

        // First check per table is the total-count check.
        assert_eq!(
            checks[0],
            RowCheck::Total {
                table: "orders".into(),
                count_sql: "SELECT COUNT(*) FROM orders".into(),
                count: 1,
            }
        );
        match &checks[1] {
            RowCheck::Exists { table, count_sql, .. } => {
                assert_eq!(table, "orders");
                assert_eq!(count_sql, "SELECT COUNT(*) FROM orders WHERE id = 10 AND user_id = 1");
            }
            other => panic!("expected existence check, got {other:?}"),
        }
    }

    #[test]
    fn row_checks_null_uses_is_null() {
        let data = HashMap::from([(
            "settings".to_string(),
            TableSeedData {
                rows: vec![],
                expected_after_up: vec![HashMap::from([
                    ("id".to_string(), json!(1)),
                    ("value".to_string(), json!(null)),
                ])],
                expected_after_down: vec![],
            },
        )]);
        let checks = build_row_checks(&data, Direction::Up, &no_arrays(), &no_arrays());
        let exists = checks
            .iter()
            .find(|c| matches!(c, RowCheck::Exists { .. }))
            .expect("existence check");
        assert_eq!(
            exists.count_sql(),
            "SELECT COUNT(*) FROM settings WHERE id = 1 AND value IS NULL"
        );
    }

    #[test]
    fn row_checks_empty() {
        let data: HashMap<String, TableSeedData> = HashMap::new();
        assert!(build_row_checks(&data, Direction::Up, &no_arrays(), &no_arrays()).is_empty());
    }

    #[test]
    fn row_checks_down_excludes_dropped_columns() {
        // `created_at` is dropped by UP; after DOWN it is re-added with a default,
        // so its original value is unrecoverable and must be excluded from the
        // existence predicate. `id`/`name` survive and are still checked.
        let data = HashMap::from([(
            "users".to_string(),
            TableSeedData {
                rows: vec![],
                expected_after_up: vec![],
                expected_after_down: vec![HashMap::from([
                    ("id".to_string(), json!(1)),
                    ("name".to_string(), json!("alice")),
                    ("created_at".to_string(), json!("2024-01-01 10:00:00")),
                ])],
            },
        )]);
        let exclude = HashMap::from([("users".to_string(), HashSet::from(["created_at".to_string()]))]);
        let checks = build_row_checks(&data, Direction::Down, &no_arrays(), &exclude);

        // Total check still asserts the row count is preserved.
        assert_eq!(
            checks[0],
            RowCheck::Total {
                table: "users".into(),
                count_sql: "SELECT COUNT(*) FROM users".into(),
                count: 1,
            }
        );
        // Existence predicate omits the dropped column but keeps the survivors.
        assert_eq!(
            checks[1].count_sql(),
            "SELECT COUNT(*) FROM users WHERE id = 1 AND name = 'alice'"
        );
    }
}
