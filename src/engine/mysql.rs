use std::process::Command;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use sqlparser::dialect::{Dialect, MySqlDialect};

use super::{DatabaseEngine, EphemeralDb, Error};
use crate::output::Output;
use crate::schema::normalize_ddl;

/// MySQL engine using a single Docker container with multiple databases.
///
/// The container is started lazily on first use and stopped when the engine
/// is dropped or the process receives SIGINT/SIGTERM.
pub struct MysqlEngine {
    image: String,
    client: &'static str,
    container: Arc<Mutex<Option<String>>>,
    db_counter: AtomicU32,
}

const DIALECT: MySqlDialect = MySqlDialect {};
const ADMIN_DB: &str = "mysql";
const DB_USER: &str = "root";
const DB_PASS: &str = "aim";

impl MysqlEngine {
    pub fn new(image: &str) -> Self {
        // MariaDB images ship `mariadb` as the client binary.
        let client = if image.starts_with("mariadb") {
            "mariadb"
        } else {
            "mysql"
        };
        Self {
            image: image.to_owned(),
            client,
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

        Output::phase(&format!("Starting {} container...", self.image));

        let output = Command::new("docker")
            .args([
                "run",
                "-d",
                "--tmpfs",
                "/var/lib/mysql",
                "-e",
                &format!("MYSQL_ROOT_PASSWORD={DB_PASS}"),
                "-e",
                &format!("MARIADB_ROOT_PASSWORD={DB_PASS}"),
                &self.image,
                "--innodb-doublewrite=0",
                "--innodb-flush-log-at-trx-commit=0",
            ])
            .stderr(std::process::Stdio::inherit())
            .output()
            .map_err(|e| Error::Connection(format!("starting mysql container: {e}")))?;

        if !output.status.success() {
            return Err(Error::Connection("docker run failed".into()));
        }

        let id = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        if id.is_empty() {
            return Err(Error::Connection("docker run returned empty container id".into()));
        }

        let cleanup_slot = Arc::clone(&self.container);
        ctrlc::set_handler(move || {
            if let Ok(mut guard) = cleanup_slot.lock()
                && let Some(cid) = guard.take()
            {
                eprintln!("\nStopping container...");
                let _ = Command::new("docker")
                    .args(["rm", "-f", &cid])
                    .stdout(std::process::Stdio::null())
                    .stderr(std::process::Stdio::null())
                    .output();
            }
            std::process::exit(130);
        })
        .ok();

        Output::phase("Waiting for database to be ready...");
        wait_ready(&id, self.client)?;

        *guard = Some(id.clone());
        Ok(id)
    }

    /// Run a single SQL command via the client -e (no implicit transaction).
    fn run_cmd(&self, container: &str, db_name: &str, sql: &str) -> Result<(), Error> {
        let output = Command::new("docker")
            .args([
                "exec",
                container,
                self.client,
                &format!("--user={DB_USER}"),
                &format!("--password={DB_PASS}"),
                db_name,
                "-e",
                sql,
            ])
            .output()
            .map_err(|e| Error::Execution(format!("running mysql: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Execution(format!("mysql error: {stderr}")));
        }

        Ok(())
    }

    /// Run multi-statement SQL via client stdin.
    fn run_exec(&self, container: &str, db_name: &str, sql: &str) -> Result<(), Error> {
        let output = Command::new("docker")
            .args([
                "exec",
                "-i",
                container,
                self.client,
                &format!("--user={DB_USER}"),
                &format!("--password={DB_PASS}"),
                db_name,
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
            .map_err(|e| Error::Execution(format!("running mysql: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Execution(format!("mysql error: {stderr}")));
        }

        Ok(())
    }

    /// Run a query and return the raw stdout.
    fn run_query(&self, container: &str, db_name: &str, sql: &str) -> Result<String, Error> {
        let output = Command::new("docker")
            .args([
                "exec",
                container,
                self.client,
                &format!("--user={DB_USER}"),
                &format!("--password={DB_PASS}"),
                "--skip-column-names",
                "--raw",
                db_name,
                "-e",
                sql,
            ])
            .output()
            .map_err(|e| Error::Execution(format!("running mysql: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Execution(format!("mysql error: {stderr}")));
        }

        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }

    /// Dump the full schema using SHOW CREATE for each object type.
    fn query_schema(&self, container: &str, db_name: &str) -> Result<String, Error> {
        let mut parts = Vec::new();

        // Tables and views.
        let objects = self.run_query(
            container,
            db_name,
            "SELECT TABLE_NAME, TABLE_TYPE FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = DATABASE() ORDER BY TABLE_NAME",
        )?;
        for line in objects.lines() {
            let Some((name, obj_type)) = line.split_once('\t') else {
                continue;
            };
            if obj_type == "VIEW" {
                let ddl = self.show_create(container, db_name, "VIEW", name)?;
                // Truncate at last `)` to strip trailing charset/collation columns.
                let ddl = ddl.rfind(')').map_or(ddl.as_str(), |pos| &ddl[..=pos]);
                parts.push(clean_view_definition(ddl));
            } else {
                parts.push(self.show_create(container, db_name, "TABLE", name)?);
            }
        }

        // Triggers.
        let triggers = self.run_query(
            container,
            db_name,
            "SELECT TRIGGER_NAME FROM information_schema.TRIGGERS \
             WHERE TRIGGER_SCHEMA = DATABASE() ORDER BY TRIGGER_NAME",
        )?;
        for name in triggers.lines().filter(|l| !l.is_empty()) {
            parts.push(self.show_create(container, db_name, "TRIGGER", name)?);
        }

        // Procedures.
        let procs = self.run_query(
            container,
            db_name,
            "SELECT ROUTINE_NAME FROM information_schema.ROUTINES \
             WHERE ROUTINE_SCHEMA = DATABASE() AND ROUTINE_TYPE = 'PROCEDURE' \
             ORDER BY ROUTINE_NAME",
        )?;
        for name in procs.lines().filter(|l| !l.is_empty()) {
            let ddl = self.show_create(container, db_name, "PROCEDURE", name)?;
            parts.push(clean_routine_definition(&ddl));
        }

        // Functions.
        let funcs = self.run_query(
            container,
            db_name,
            "SELECT ROUTINE_NAME FROM information_schema.ROUTINES \
             WHERE ROUTINE_SCHEMA = DATABASE() AND ROUTINE_TYPE = 'FUNCTION' \
             ORDER BY ROUTINE_NAME",
        )?;
        for name in funcs.lines().filter(|l| !l.is_empty()) {
            let ddl = self.show_create(container, db_name, "FUNCTION", name)?;
            parts.push(clean_routine_definition(&ddl));
        }

        // Events.
        let events = self.run_query(
            container,
            db_name,
            "SELECT EVENT_NAME FROM information_schema.EVENTS \
             WHERE EVENT_SCHEMA = DATABASE() ORDER BY EVENT_NAME",
        )?;
        for name in events.lines().filter(|l| !l.is_empty()) {
            parts.push(self.show_create(container, db_name, "EVENT", name)?);
        }

        Ok(parts.join(";\n\n"))
    }

    /// Run `SHOW CREATE <obj_type> <name>` and extract the DDL.
    fn show_create(&self, container: &str, db_name: &str, obj_type: &str, name: &str) -> Result<String, Error> {
        let raw = self.run_query(container, db_name, &format!("SHOW CREATE {obj_type} `{name}`"))?;
        // Output is: name\tDDL (DDL may span multiple lines).
        let ddl = raw.split_once('\t').map(|(_, rest)| rest).unwrap_or(&raw);
        Ok(ddl.trim().to_owned())
    }
}

impl Drop for MysqlEngine {
    fn drop(&mut self) {
        if let Some(container) = self.container.lock().ok().and_then(|mut g| g.take()) {
            Output::phase("Stopping container...");
            let _ = Command::new("docker")
                .args(["rm", "-f", &container])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .output();
        }
    }
}

/// Strip MySQL view noise like ALGORITHM=UNDEFINED DEFINER=... SQL SECURITY DEFINER.
///
/// `SHOW CREATE VIEW` returns:
/// `CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW ...`
/// This strips everything between CREATE and VIEW.
fn clean_view_definition(sql: &str) -> String {
    let upper = sql.to_uppercase();
    if let Some(pos) = upper.find(" VIEW ") {
        format!("CREATE{}", &sql[pos..])
    } else {
        sql.to_owned()
    }
}

/// Strip DEFINER=... from procedure/function definitions.
///
/// `SHOW CREATE PROCEDURE` returns:
/// `CREATE DEFINER=`root`@`%` PROCEDURE `name`(...) ...`
/// This strips the DEFINER clause.
fn clean_routine_definition(sql: &str) -> String {
    let upper = sql.to_uppercase();
    for keyword in [" PROCEDURE ", " FUNCTION "] {
        if let Some(pos) = upper.find(keyword) {
            return format!("CREATE{}", &sql[pos..]);
        }
    }
    sql.to_owned()
}

/// Wait for the MySQL container to be ready to accept authenticated connections.
fn wait_ready(container: &str, client: &str) -> Result<(), Error> {
    for _ in 0..60 {
        let output = Command::new("docker")
            .args([
                "exec",
                container,
                client,
                &format!("--user={DB_USER}"),
                &format!("--password={DB_PASS}"),
                "-e",
                "SELECT 1",
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .output()
            .map_err(|e| Error::Connection(format!("checking readiness: {e}")))?;

        if output.status.success() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(500));
    }
    super::dump_container_logs(container);
    Err(Error::Connection("container did not become ready within 30s".into()))
}

impl DatabaseEngine for MysqlEngine {
    fn create_ephemeral(&self) -> Result<EphemeralDb, Error> {
        let container = self.ensure_container()?;
        let n = self.db_counter.fetch_add(1, Ordering::Relaxed);
        let db_name = format!("aim_ephemeral_{n}");

        self.run_cmd(&container, ADMIN_DB, &format!("CREATE DATABASE {db_name}"))?;

        Ok(EphemeralDb { id: db_name })
    }

    fn execute(&self, db: &EphemeralDb, sql: &str) -> Result<(), Error> {
        let container = self.ensure_container()?;
        self.run_exec(&container, &db.id, sql)
    }

    fn dump_schema(&self, db: &EphemeralDb) -> Result<String, Error> {
        let container = self.ensure_container()?;
        self.query_schema(&container, &db.id)
    }

    fn drop_ephemeral(&self, db: EphemeralDb) -> Result<(), Error> {
        let container = self.ensure_container()?;
        self.run_cmd(&container, ADMIN_DB, &format!("DROP DATABASE IF EXISTS {}", db.id))?;
        Ok(())
    }

    fn dialect(&self) -> Box<dyn Dialect> {
        Box::new(MySqlDialect {})
    }

    fn format_sql(&self, sql: &str) -> String {
        normalize_ddl(&DIALECT, sql)
    }

    fn dialect_description(&self) -> &str {
        "MySQL. Use standard MySQL DDL syntax. \
         Use AUTO_INCREMENT for auto-increment columns. \
         ALTER TABLE supports ADD COLUMN, DROP COLUMN, MODIFY COLUMN, \
         CHANGE COLUMN, and RENAME COLUMN. \
         Use IF EXISTS / IF NOT EXISTS where appropriate. \
         Do NOT include transaction wrappers (BEGIN/COMMIT)."
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clean_view_definition() {
        let raw = "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `group_members` AS select g.name AS group_name";
        let cleaned = clean_view_definition(raw);
        assert_eq!(cleaned, "CREATE VIEW `group_members` AS select g.name AS group_name");
    }
}
