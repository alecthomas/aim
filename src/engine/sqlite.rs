use std::collections::BTreeMap;
use std::path::PathBuf;

use rusqlite::Connection;
use sqlparser::dialect::SQLiteDialect;

use super::{DatabaseEngine, EphemeralDb, Error};
use crate::diff::text_diff;
use crate::schema::normalize_ddl;

/// SQLite engine using temporary files for ephemeral databases.
pub struct SqliteEngine;

const DIALECT: SQLiteDialect = SQLiteDialect {};

impl SqliteEngine {
    fn path_for(db: &EphemeralDb) -> PathBuf {
        PathBuf::from(&db.id)
    }

    fn open(db: &EphemeralDb) -> Result<Connection, Error> {
        Connection::open(Self::path_for(db)).map_err(|e| Error::Connection(format!("opening {}: {e}", db.id)))
    }

    /// Extract all schema objects from sqlite_master, keyed by (type, name).
    fn schema_objects(conn: &Connection) -> Result<BTreeMap<(String, String), String>, Error> {
        let mut stmt = conn
            .prepare(
                "SELECT type, name, sql FROM sqlite_master \
                 WHERE sql IS NOT NULL \
                 AND name NOT LIKE 'sqlite_%' \
                 ORDER BY type, name",
            )
            .map_err(|e| Error::Diff(format!("preparing schema query: {e}")))?;

        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(|e| Error::Diff(format!("querying sqlite_master: {e}")))?;

        let mut objects = BTreeMap::new();
        for row in rows {
            let (obj_type, name, sql) = row.map_err(|e| Error::Diff(format!("reading row: {e}")))?;
            objects.insert((obj_type, name), sql);
        }
        Ok(objects)
    }
}

impl DatabaseEngine for SqliteEngine {
    fn create_ephemeral(&self) -> Result<EphemeralDb, Error> {
        let path = std::env::temp_dir().join(format!("aim_{}.db", unique_id()));
        // Create the file by opening and immediately closing a connection.
        Connection::open(&path).map_err(|e| Error::Connection(format!("creating {}: {e}", path.display())))?;
        Ok(EphemeralDb {
            id: path.to_string_lossy().into_owned(),
        })
    }

    fn execute(&self, db: &EphemeralDb, sql: &str) -> Result<(), Error> {
        let conn = Self::open(db)?;
        // Disable FK checks so DDL can be executed in any order.
        conn.execute_batch("PRAGMA foreign_keys = OFF;")
            .map_err(|e| Error::Execution(format!("disabling foreign keys: {e}")))?;
        conn.execute_batch(sql).map_err(|e| Error::Execution(format!("{e}")))?;
        Ok(())
    }

    fn diff(
        &self,
        left: &EphemeralDb,
        left_label: &str,
        right: &EphemeralDb,
        right_label: &str,
    ) -> Result<String, Error> {
        let left_conn = Self::open(left)?;
        let right_conn = Self::open(right)?;

        let left_objects = Self::schema_objects(&left_conn)?;
        let right_objects = Self::schema_objects(&right_conn)?;

        let mut diffs = Vec::new();
        let mut has_diff = false;

        // Objects in left but not in right (or different after normalization).
        for (key, left_sql) in &left_objects {
            match right_objects.get(key) {
                None => {
                    if has_diff {
                        diffs.push(String::new());
                    }
                    has_diff = true;
                    let norm = normalize_ddl(&DIALECT, left_sql);
                    for line in norm.lines() {
                        diffs.push(format!("- {line}"));
                    }
                }
                Some(right_sql) => {
                    let left_norm = normalize_ddl(&DIALECT, left_sql);
                    let right_norm = normalize_ddl(&DIALECT, right_sql);
                    if left_norm != right_norm {
                        if has_diff {
                            diffs.push(String::new());
                        }
                        has_diff = true;
                        diffs.push(text_diff(&left_norm, &right_norm));
                    }
                }
            }
        }

        // Objects in right but not in left.
        for (key, right_sql) in &right_objects {
            if !left_objects.contains_key(key) {
                if has_diff {
                    diffs.push(String::new());
                }
                has_diff = true;
                let norm = normalize_ddl(&DIALECT, right_sql);
                for line in norm.lines() {
                    diffs.push(format!("+ {line}"));
                }
            }
        }

        // Add header if there are any diffs.
        if has_diff {
            diffs.insert(0, format!("--- {left_label}"));
            diffs.insert(1, format!("+++ {right_label}"));
        }

        Ok(diffs.join("\n"))
    }

    fn dump_schema(&self, db: &EphemeralDb) -> Result<String, Error> {
        let conn = Self::open(db)?;
        // Use creation order (sqlite_master rowid) to preserve FK dependencies,
        // not the alphabetically-sorted BTreeMap from schema_objects.
        let mut stmt = conn
            .prepare(
                "SELECT sql FROM sqlite_master \
                 WHERE sql IS NOT NULL \
                 AND name NOT LIKE 'sqlite_%' \
                 ORDER BY rowid",
            )
            .map_err(|e| Error::Diff(format!("preparing dump query: {e}")))?;

        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|e| Error::Diff(format!("querying sqlite_master: {e}")))?;

        let mut parts = Vec::new();
        for row in rows {
            let sql = row.map_err(|e| Error::Diff(format!("reading row: {e}")))?;
            parts.push(format!("{};", normalize_ddl(&DIALECT, &sql)));
        }
        Ok(parts.join("\n\n"))
    }

    fn drop_ephemeral(&self, db: EphemeralDb) -> Result<(), Error> {
        let path = Self::path_for(&db);
        if path.exists() {
            std::fs::remove_file(&path).map_err(|e| Error::Connection(format!("removing {}: {e}", path.display())))?;
        }
        Ok(())
    }

    fn migration_prefix(&self) -> &str {
        "PRAGMA foreign_keys = OFF;\n"
    }

    fn migration_suffix(&self) -> &str {
        "PRAGMA foreign_keys = ON;\n"
    }

    fn format_sql(&self, sql: &str) -> String {
        normalize_ddl(&DIALECT, sql)
    }

    fn dialect_description(&self) -> &str {
        "SQLite (version 3.35.0+). Use standard SQLite DDL syntax. \
         ALTER TABLE supports ADD COLUMN, DROP COLUMN, and RENAME. \
         No ALTER COLUMN — to change a column type you must recreate the table. \
         Use INTEGER PRIMARY KEY for auto-increment. \
         Never recreate a table when a simple ALTER TABLE will do."
    }
}

/// Generate a simple random hex string as a unique identifier.
fn unique_id() -> String {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};

    let state = RandomState::new();
    let mut hasher = state.build_hasher();
    hasher.write_u64(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64,
    );
    format!("{:016x}", hasher.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_execute_drop() {
        let engine = SqliteEngine;
        let db = engine.create_ephemeral().expect("create");
        engine
            .execute(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);")
            .expect("execute");
        engine.drop_ephemeral(db).expect("drop");
    }

    #[test]
    fn test_diff_identical() {
        let engine = SqliteEngine;
        let left = engine.create_ephemeral().expect("create left");
        let right = engine.create_ephemeral().expect("create right");

        let ddl = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);";
        engine.execute(&left, ddl).expect("exec left");
        engine.execute(&right, ddl).expect("exec right");

        let result = engine.diff(&left, "left", &right, "right").expect("diff");
        assert!(result.is_empty(), "expected no diff, got: {result}");

        engine.drop_ephemeral(left).expect("drop left");
        engine.drop_ephemeral(right).expect("drop right");
    }

    #[test]
    fn test_diff_missing_table() {
        let engine = SqliteEngine;
        let left = engine.create_ephemeral().expect("create left");
        let right = engine.create_ephemeral().expect("create right");

        engine
            .execute(&left, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")
            .expect("exec left");

        let result = engine.diff(&left, "left", &right, "right").expect("diff");
        assert!(
            result.contains("- CREATE TABLE"),
            "expected removed table in diff: {result}"
        );

        engine.drop_ephemeral(left).expect("drop left");
        engine.drop_ephemeral(right).expect("drop right");
    }

    #[test]
    fn test_diff_extra_table() {
        let engine = SqliteEngine;
        let left = engine.create_ephemeral().expect("create left");
        let right = engine.create_ephemeral().expect("create right");

        engine
            .execute(&right, "CREATE TABLE orders (id INTEGER PRIMARY KEY);")
            .expect("exec right");

        let result = engine.diff(&left, "left", &right, "right").expect("diff");
        assert!(
            result.contains("+ CREATE TABLE"),
            "expected added table in diff: {result}"
        );

        engine.drop_ephemeral(left).expect("drop left");
        engine.drop_ephemeral(right).expect("drop right");
    }

    #[test]
    fn test_diff_different_schema() {
        let engine = SqliteEngine;
        let left = engine.create_ephemeral().expect("create left");
        let right = engine.create_ephemeral().expect("create right");

        engine
            .execute(&left, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")
            .expect("exec left");
        engine
            .execute(
                &right,
                "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT);",
            )
            .expect("exec right");

        let result = engine.diff(&left, "left", &right, "right").expect("diff");
        assert!(
            result.contains("--- left") && result.contains("+++ right"),
            "expected unified diff headers: {result}"
        );

        engine.drop_ephemeral(left).expect("drop left");
        engine.drop_ephemeral(right).expect("drop right");
    }

    /// Verify that column order doesn't matter — ALTER TABLE ADD COLUMN
    /// appends to the end, but the schema should still match.
    #[test]
    fn test_diff_column_order_independent() {
        let engine = SqliteEngine;
        let left = engine.create_ephemeral().expect("create left");
        let right = engine.create_ephemeral().expect("create right");

        // left: schema.sql with email in the middle
        engine
            .execute(
                &left,
                "CREATE TABLE users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    email TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL DEFAULT (datetime('now'))
                );",
            )
            .expect("exec left");

        // right: created via migration (ALTER TABLE ADD COLUMN appends)
        engine
            .execute(
                &right,
                "CREATE TABLE users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT (datetime('now'))
                );",
            )
            .expect("exec right base");
        engine
            .execute(&right, "ALTER TABLE users ADD COLUMN email TEXT NOT NULL DEFAULT '';")
            .expect("exec right alter");

        let result = engine.diff(&left, "left", &right, "right").expect("diff");
        assert!(
            result.is_empty(),
            "expected no diff (column order independent), got: {result}"
        );

        engine.drop_ephemeral(left).expect("drop left");
        engine.drop_ephemeral(right).expect("drop right");
    }
}
