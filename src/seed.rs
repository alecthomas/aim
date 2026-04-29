use std::collections::HashMap;

use crate::agent::tools::TableSeedData;

/// Converts a JSON value to a SQL literal string.
fn json_value_to_sql_literal(v: &serde_json::Value) -> String {
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
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            format!("'{}'", v.to_string().replace('\'', "''"))
        }
    }
}

/// Generates INSERT statements for all tables' seed rows.
///
/// For each table (sorted alphabetically), produces one `INSERT INTO`
/// statement per row with columns in alphabetical order. Tables are
/// separated by a blank line.
pub fn build_insert_statements(seed_data: &HashMap<String, TableSeedData>) -> String {
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
                    .map(|c| json_value_to_sql_literal(&row[c.as_str()]))
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

/// Builds a WHERE condition for a single column and value.
///
/// Uses `IS NULL` for null values, `=` for everything else.
fn where_condition(col: &str, val: &serde_json::Value) -> String {
    if val.is_null() {
        format!("{col} IS NULL")
    } else {
        format!("{col} = {}", json_value_to_sql_literal(val))
    }
}

/// Generates SELECT statements that verify expected row values exist.
///
/// For each expected row (from `expected_after_up` or `expected_after_down`),
/// produces a `SELECT col1, col2 FROM table WHERE col1 = val1 AND col2 = val2;`.
/// Columns are sorted alphabetically. Tables are sorted alphabetically,
/// with a blank line between tables.
pub fn build_select_statements(seed_data: &HashMap<String, TableSeedData>, direction: Direction) -> String {
    let mut tables: Vec<&String> = seed_data.keys().collect();
    tables.sort();

    let mut sections = Vec::with_capacity(tables.len());

    for table in tables {
        let seed = &seed_data[table.as_str()];
        let expected = match direction {
            Direction::Up => &seed.expected_after_up,
            Direction::Down => &seed.expected_after_down,
        };
        let stmts: Vec<String> = expected
            .iter()
            .map(|row| {
                let mut cols: Vec<&str> = row.keys().map(String::as_str).collect();
                cols.sort();
                let col_list = cols.to_vec().join(", ");
                let conditions = cols
                    .iter()
                    .map(|c| where_condition(c, &row[*c]))
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn literal_null() {
        assert_eq!(json_value_to_sql_literal(&json!(null)), "NULL");
    }

    #[test]
    fn literal_bool() {
        assert_eq!(json_value_to_sql_literal(&json!(true)), "TRUE");
        assert_eq!(json_value_to_sql_literal(&json!(false)), "FALSE");
    }

    #[test]
    fn literal_integer() {
        assert_eq!(json_value_to_sql_literal(&json!(42)), "42");
    }

    #[test]
    fn literal_float() {
        assert_eq!(json_value_to_sql_literal(&json!(3.14)), "3.14");
    }

    #[test]
    fn literal_string() {
        assert_eq!(json_value_to_sql_literal(&json!("hello")), "'hello'");
    }

    #[test]
    fn literal_string_with_quotes() {
        assert_eq!(json_value_to_sql_literal(&json!("it's a test")), "'it''s a test'");
    }

    #[test]
    fn literal_array() {
        let val = json!([1, 2, 3]);
        assert_eq!(json_value_to_sql_literal(&val), "'[1,2,3]'");
    }

    #[test]
    fn literal_object() {
        let val = json!({"a": 1});
        assert_eq!(json_value_to_sql_literal(&val), "'{\"a\":1}'");
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
        let sql = build_insert_statements(&data);

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
        assert_eq!(build_insert_statements(&data), "");
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
        let sql = build_select_statements(&data, Direction::Up);

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
        let sql = build_select_statements(&data, Direction::Down);

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
        let sql = build_select_statements(&data, Direction::Up);
        assert_eq!(sql, "SELECT id, value FROM settings WHERE id = 1 AND value IS NULL;");
    }

    #[test]
    fn select_statements_skips_empty_expected() {
        let data = make_seed_data();
        let sql = build_select_statements(&data, Direction::Up);
        assert_eq!(sql, "");
    }

    #[test]
    fn select_statements_empty() {
        let data: HashMap<String, TableSeedData> = HashMap::new();
        assert_eq!(build_select_statements(&data, Direction::Up), "");
    }
}
