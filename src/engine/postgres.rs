use std::process::Command;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use sqlparser::dialect::{Dialect, PostgreSqlDialect};

use super::{DatabaseEngine, EphemeralDb, Error};
use crate::output::Output;

/// PostgreSQL engine using a single Docker container with multiple databases.
///
/// The container is started lazily on first use and stopped when the engine
/// is dropped or the process receives SIGINT/SIGTERM. Each ephemeral database
/// is a separate Postgres database within the container.
pub struct PostgresEngine {
    version: String,
    container: Arc<Mutex<Option<String>>>,
    db_counter: AtomicU32,
}

const DIALECT: PostgreSqlDialect = PostgreSqlDialect {};
const ADMIN_DB: &str = "postgres";
const DB_USER: &str = "aim";
const DB_PASS: &str = "aim";

impl PostgresEngine {
    pub fn new(version: &str) -> Self {
        Self {
            version: version.to_owned(),
            container: Arc::new(Mutex::new(None)),
            db_counter: AtomicU32::new(0),
        }
    }

    /// Get or start the shared container, returning its ID.
    fn ensure_container(&self) -> Result<String, Error> {
        let mut guard = self
            .container
            .lock()
            .map_err(|e| Error::Connection(format!("container lock poisoned: {e}")))?;

        if let Some(ref id) = *guard {
            return Ok(id.clone());
        }

        let image = format!("postgres:{}", self.version);
        Output::phase(&format!("Starting {image} container..."));

        // Let stderr pass through so the user sees Docker pull progress.
        let output = Command::new("docker")
            .args([
                "run",
                "-d",
                "--tmpfs",
                "/var/lib/postgresql/data",
                "-e",
                &format!("POSTGRES_USER={DB_USER}"),
                "-e",
                &format!("POSTGRES_PASSWORD={DB_PASS}"),
                "-e",
                "PGDATA=/var/lib/postgresql/data",
                &image,
                "-c",
                "fsync=off",
                "-c",
                "synchronous_commit=off",
                "-c",
                "full_page_writes=off",
            ])
            .stderr(std::process::Stdio::inherit())
            .output()
            .map_err(|e| Error::Connection(format!("starting postgres container: {e}")))?;

        if !output.status.success() {
            return Err(Error::Connection("docker run failed".into()));
        }

        let id = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        if id.is_empty() {
            return Err(Error::Connection("docker run returned empty container id".into()));
        }

        // Install a ctrlc handler that stops the container on SIGINT/SIGTERM.
        let cleanup_slot = Arc::clone(&self.container);
        ctrlc::set_handler(move || {
            // Take the container ID so we only stop once.
            if let Ok(mut guard) = cleanup_slot.lock()
                && let Some(cid) = guard.take()
            {
                eprintln!("\nStopping postgres container...");
                let _ = Command::new("docker")
                    .args(["rm", "-f", &cid])
                    .stdout(std::process::Stdio::null())
                    .stderr(std::process::Stdio::null())
                    .output();
            }
            std::process::exit(130);
        })
        .map_err(|e| {
            // Non-fatal: if we can't install the handler, the container
            // will still be cleaned up on normal exit via Drop.
            eprintln!("warning: could not install ctrl-c handler: {e}");
            let _ = e;
        })
        .ok();

        Output::phase("Waiting for database to be ready...");
        wait_ready(&id)?;

        *guard = Some(id.clone());
        Ok(id)
    }

    /// Run a single SQL command via psql -c (no transaction wrapping).
    fn psql_cmd(container: &str, db_name: &str, sql: &str) -> Result<(), Error> {
        let output = Command::new("docker")
            .args([
                "exec",
                container,
                "psql",
                "-U",
                DB_USER,
                "-d",
                db_name,
                "-v",
                "ON_ERROR_STOP=1",
                "--no-psqlrc",
                "-c",
                sql,
            ])
            .output()
            .map_err(|e| Error::Execution(format!("running psql: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Execution(format!("psql error: {stderr}")));
        }

        Ok(())
    }

    /// Run multi-statement SQL via psql stdin.
    fn psql_exec(container: &str, db_name: &str, sql: &str) -> Result<(), Error> {
        let output = Command::new("docker")
            .args([
                "exec",
                "-i",
                container,
                "psql",
                "-U",
                DB_USER,
                "-d",
                db_name,
                "-v",
                "ON_ERROR_STOP=1",
                "--no-psqlrc",
            ])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .and_then(|mut child| {
                use std::io::Write;
                if let Some(ref mut stdin) = child.stdin {
                    stdin.write_all(sql.as_bytes())?;
                }
                child.wait_with_output()
            })
            .map_err(|e| Error::Execution(format!("running psql: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Execution(format!("psql error: {stderr}")));
        }

        Ok(())
    }

    /// Dump the schema using pg_dump.
    fn pg_dump(container: &str, db_name: &str) -> Result<String, Error> {
        let output = Command::new("docker")
            .args([
                "exec",
                container,
                "pg_dump",
                "-U",
                DB_USER,
                "-d",
                db_name,
                "--schema-only",
                "--no-owner",
                "--no-privileges",
                "--no-comments",
                "--no-tablespaces",
            ])
            .output()
            .map_err(|e| Error::Execution(format!("running pg_dump: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Execution(format!("pg_dump error: {stderr}")));
        }

        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }

    /// Strip pg_dump preamble (comments, SET, SELECT, psql metacommands).
    ///
    /// Returns raw DDL statements separated by `;\n\n`.
    fn strip_preamble(dump: &str) -> String {
        let mut parts = Vec::new();
        let mut current = String::new();

        for line in dump.lines() {
            if line.starts_with("--")
                || line.starts_with("SET ")
                || line.starts_with("SELECT ")
                || line.starts_with('\\')
            {
                continue;
            }

            if line.is_empty() {
                if !current.trim().is_empty() {
                    parts.push(std::mem::take(&mut current));
                }
                continue;
            }

            current.push_str(line);
            current.push('\n');
        }
        if !current.trim().is_empty() {
            parts.push(current);
        }

        parts.iter().map(|s| s.trim()).collect::<Vec<_>>().join(";\n\n")
    }
}

impl Drop for PostgresEngine {
    fn drop(&mut self) {
        if let Some(container) = self.container.lock().ok().and_then(|mut g| g.take()) {
            Output::phase("Stopping postgres container...");
            let _ = Command::new("docker")
                .args(["rm", "-f", &container])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .output();
        }
    }
}

/// Wait for the Postgres container to be ready to accept connections.
fn wait_ready(container: &str) -> Result<(), Error> {
    for _ in 0..60 {
        let output = Command::new("docker")
            .args(["exec", container, "pg_isready", "-U", DB_USER])
            .output()
            .map_err(|e| Error::Connection(format!("checking readiness: {e}")))?;

        if output.status.success() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(500));
    }
    super::dump_container_logs(container);
    Err(Error::Connection(
        "postgres container did not become ready within 30s".into(),
    ))
}

impl DatabaseEngine for PostgresEngine {
    fn create_ephemeral(&self) -> Result<EphemeralDb, Error> {
        let container = self.ensure_container()?;
        let n = self.db_counter.fetch_add(1, Ordering::Relaxed);
        let db_name = format!("aim_ephemeral_{n}");

        // CREATE DATABASE cannot run inside a transaction, so use -c
        // for each statement rather than piping via stdin.
        Self::psql_cmd(&container, ADMIN_DB, &format!("CREATE DATABASE {db_name}"))?;

        Ok(EphemeralDb { id: db_name })
    }

    fn execute(&self, db: &EphemeralDb, sql: &str) -> Result<(), Error> {
        let container = self.ensure_container()?;
        Self::psql_exec(&container, &db.id, sql)
    }

    fn execute_in_transaction(&self, db: &EphemeralDb, sql: &str) -> Result<(), Error> {
        let container = self.ensure_container()?;
        Self::psql_exec(&container, &db.id, &format!("BEGIN;\n{sql}\nCOMMIT;"))
    }

    fn dump_schema(&self, db: &EphemeralDb) -> Result<String, Error> {
        let container = self.ensure_container()?;
        let dump = Self::pg_dump(&container, &db.id)?;
        Ok(Self::strip_preamble(&dump))
    }

    fn drop_ephemeral(&self, db: EphemeralDb) -> Result<(), Error> {
        let container = self.ensure_container()?;
        Self::psql_cmd(&container, ADMIN_DB, &format!("DROP DATABASE IF EXISTS {}", db.id))?;
        Ok(())
    }

    fn dialect(&self) -> Box<dyn Dialect> {
        Box::new(PostgreSqlDialect {})
    }

    fn format_sql(&self, sql: &str) -> String {
        crate::schema::normalize_ddl(&DIALECT, sql)
    }

    fn dialect_description(&self) -> &str {
        "PostgreSQL. Write clean, idiomatic PostgreSQL DDL. \
         The read_schema and read_previous_schema tools return pg_dump output \
         which is verbose and decomposed — do NOT mimic that style. \
         Instead, write migration SQL the way a human DBA would.

## Style rules
- Do NOT schema-qualify names. Write `users`, not `public.users`.
- Use short type aliases: `BIGSERIAL` not `bigint` + separate sequence, \
  `VARCHAR(n)` not `character varying(n)`, `TIMESTAMPTZ` not `timestamp with time zone`, \
  `BOOL` not `boolean`, `INT` not `integer`.
- Use BIGSERIAL/SERIAL for auto-increment primary keys — never decompose into \
  CREATE SEQUENCE + ALTER COLUMN SET DEFAULT nextval(...).
- Put PRIMARY KEY, UNIQUE, NOT NULL, DEFAULT, and FOREIGN KEY constraints inline \
  in CREATE TABLE — do NOT split them into separate ALTER TABLE statements.
- For CREATE INDEX, omit `USING btree` (it is the default).
- Do NOT use CREATE INDEX CONCURRENTLY — most migration runners execute inside \
  a transaction where it is not allowed.
- Do NOT include transaction wrappers (BEGIN/COMMIT).
- Prefer IF EXISTS / IF NOT EXISTS where appropriate.
- ALTER TABLE supports ADD COLUMN, DROP COLUMN, ALTER COLUMN (SET/DROP NOT NULL, \
  SET DATA TYPE, SET DEFAULT, DROP DEFAULT), and RENAME COLUMN.
- When enum values are renamed or replaced, UPDATE existing rows to map old values \
  to their new equivalents before altering the type. Use ALTER TYPE ... RENAME VALUE \
  when simply renaming. When restructuring an enum, create the new type, ALTER COLUMN \
  SET DATA TYPE using a USING clause to map old values, then drop the old type."
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::schema_diff;

    #[test]
    fn test_strip_preamble() {
        let dump = "\
-- PostgreSQL database dump
SET statement_timeout = 0;
SELECT pg_catalog.set_config('search_path', '', false);
\\connect aim_ephemeral_0

CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL
);

CREATE TABLE public.orders (
    id integer NOT NULL
);
";
        let cleaned = PostgresEngine::strip_preamble(dump);
        assert!(!cleaned.contains("SET "));
        assert!(!cleaned.contains("SELECT "));
        assert!(!cleaned.contains("--"));
        assert!(!cleaned.contains("\\connect"));
        assert!(cleaned.contains("CREATE TABLE"));
    }

    #[test]
    fn test_schema_diff_order_independent() {
        let dump_a = "CREATE TABLE users (id integer NOT NULL, name text NOT NULL);\n\nCREATE TABLE orders (id integer NOT NULL)";
        let dump_b = "CREATE TABLE orders (id integer NOT NULL);\n\nCREATE TABLE users (id integer NOT NULL, name text NOT NULL)";
        let diff = schema_diff(&PostgreSqlDialect {}, dump_a, "a", dump_b, "b");
        assert!(diff.is_empty(), "expected no diff, got: {diff}");
    }
}
