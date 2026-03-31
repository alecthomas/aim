use std::path::PathBuf;

use rusqlite::Connection;
use sqlparser::dialect::{Dialect, SQLiteDialect};

use super::{DatabaseEngine, EphemeralDb, Error};
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
}

impl DatabaseEngine for SqliteEngine {
    fn create_ephemeral(&self) -> Result<EphemeralDb, Error> {
        let path = std::env::temp_dir().join(format!("aim_{}.db", unique_id()));
        Connection::open(&path).map_err(|e| Error::Connection(format!("creating {}: {e}", path.display())))?;
        Ok(EphemeralDb {
            id: path.to_string_lossy().into_owned(),
        })
    }

    fn execute(&self, db: &EphemeralDb, sql: &str) -> Result<(), Error> {
        let conn = Self::open(db)?;
        conn.execute_batch("PRAGMA foreign_keys = OFF;")
            .map_err(|e| Error::Execution(format!("disabling foreign keys: {e}")))?;
        conn.execute_batch(sql).map_err(|e| Error::Execution(format!("{e}")))?;
        Ok(())
    }

    fn dump_schema(&self, db: &EphemeralDb) -> Result<String, Error> {
        let conn = Self::open(db)?;
        let mut stmt = conn
            .prepare(
                "SELECT sql FROM sqlite_master \
                 WHERE sql IS NOT NULL \
                 AND name NOT LIKE 'sqlite_%' \
                 ORDER BY rowid",
            )
            .map_err(|e| Error::Execution(format!("preparing dump query: {e}")))?;

        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|e| Error::Execution(format!("querying sqlite_master: {e}")))?;

        let mut parts = Vec::new();
        for row in rows {
            let sql = row.map_err(|e| Error::Execution(format!("reading row: {e}")))?;
            parts.push(sql);
        }
        Ok(parts.join(";\n\n"))
    }

    fn dialect(&self) -> Box<dyn Dialect> {
        Box::new(SQLiteDialect {})
    }

    fn drop_ephemeral(&self, db: EphemeralDb) -> Result<(), Error> {
        let path = Self::path_for(&db);
        if path.exists() {
            std::fs::remove_file(&path).map_err(|e| Error::Connection(format!("removing {}: {e}", path.display())))?;
        }
        Ok(())
    }

    fn migration_prefix(&self) -> &str {
        "PRAGMA defer_foreign_keys = true;\n"
    }

    fn migration_suffix(&self) -> &str {
        ""
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
    use crate::engine::schema_diff;
    use sqlparser::dialect::SQLiteDialect;

    fn dialect() -> SQLiteDialect {
        SQLiteDialect {}
    }

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

        let left_schema = engine.dump_schema(&left).expect("dump left");
        let right_schema = engine.dump_schema(&right).expect("dump right");
        let result = schema_diff(&dialect(), &left_schema, "left", &right_schema, "right");
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

        let left_schema = engine.dump_schema(&left).expect("dump left");
        let right_schema = engine.dump_schema(&right).expect("dump right");
        let result = schema_diff(&dialect(), &left_schema, "left", &right_schema, "right");
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

        let left_schema = engine.dump_schema(&left).expect("dump left");
        let right_schema = engine.dump_schema(&right).expect("dump right");
        let result = schema_diff(&dialect(), &left_schema, "left", &right_schema, "right");
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

        let left_schema = engine.dump_schema(&left).expect("dump left");
        let right_schema = engine.dump_schema(&right).expect("dump right");
        let result = schema_diff(&dialect(), &left_schema, "left", &right_schema, "right");
        assert!(
            result.contains("--- left") && result.contains("+++ right"),
            "expected unified diff headers: {result}"
        );

        engine.drop_ephemeral(left).expect("drop left");
        engine.drop_ephemeral(right).expect("drop right");
    }

    #[test]
    fn test_diff_column_order_independent() {
        let engine = SqliteEngine;
        let left = engine.create_ephemeral().expect("create left");
        let right = engine.create_ephemeral().expect("create right");

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

        let left_schema = engine.dump_schema(&left).expect("dump left");
        let right_schema = engine.dump_schema(&right).expect("dump right");
        let result = schema_diff(&dialect(), &left_schema, "left", &right_schema, "right");
        assert!(
            result.is_empty(),
            "expected no diff (column order independent), got: {result}"
        );

        engine.drop_ephemeral(left).expect("drop left");
        engine.drop_ephemeral(right).expect("drop right");
    }
}
