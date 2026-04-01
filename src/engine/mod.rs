pub mod mysql;
pub mod postgres;
pub mod sqlite;

use std::fmt;
use std::process::Command;

use sqlparser::dialect::Dialect;

use crate::diff::text_diff;
use crate::schema::normalize_ddl;

/// Errors from database engine operations.
#[derive(Debug)]
pub enum Error {
    /// Failed to create or connect to an ephemeral database.
    Connection(String),
    /// Failed to execute SQL.
    Execution(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Connection(msg) => write!(f, "engine connection: {msg}"),
            Error::Execution(msg) => write!(f, "engine execution: {msg}"),
        }
    }
}

impl std::error::Error for Error {}

/// Fetch and print Docker container logs to stderr for debugging.
pub fn dump_container_logs(container: &str) {
    let output = Command::new("docker")
        .args(["logs", "--tail", "50", container])
        .output();
    if let Ok(output) = output {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        if !stdout.is_empty() {
            eprintln!("--- container logs (stdout) ---\n{stdout}");
        }
        if !stderr.is_empty() {
            eprintln!("--- container logs (stderr) ---\n{stderr}");
        }
    }
}

/// Handle to an ephemeral database used for verification.
///
/// The concrete type is engine-specific; this wrapper holds an opaque identifier
/// that the engine uses to locate/manage the database.
#[derive(Debug)]
pub struct EphemeralDb {
    /// Engine-specific identifier (e.g. temp file path for SQLite, container ID for Docker).
    pub id: String,
}

/// Portability boundary — every supported database implements this trait.
///
/// All operations target ephemeral (disposable) databases used for migration verification.
pub trait DatabaseEngine: Send + Sync {
    /// Spin up a new ephemeral database and return a handle to it.
    fn create_ephemeral(&self) -> Result<EphemeralDb, Error>;

    /// Execute arbitrary SQL against an ephemeral database.
    fn execute(&self, db: &EphemeralDb, sql: &str) -> Result<(), Error>;

    /// Dump the raw schema of an ephemeral database as DDL statements
    /// separated by `;\n\n`.
    ///
    /// The engine should strip engine-specific noise (e.g. pg_dump preamble,
    /// `public.` schema qualifiers) but should NOT normalize or sort —
    /// that is handled by `schema_diff`.
    fn dump_schema(&self, db: &EphemeralDb) -> Result<String, Error>;

    /// Tear down an ephemeral database and clean up resources.
    fn drop_ephemeral(&self, db: EphemeralDb) -> Result<(), Error>;

    /// The sqlparser dialect for this engine, used for normalization.
    fn dialect(&self) -> Box<dyn Dialect>;

    /// SQL to prepend to migration files (e.g. disabling FK checks).
    fn migration_prefix(&self) -> &str {
        ""
    }

    /// SQL to append to migration files (e.g. re-enabling FK checks).
    fn migration_suffix(&self) -> &str {
        ""
    }

    /// Format SQL for display, adding line breaks at clause boundaries.
    fn format_sql(&self, sql: &str) -> String;

    /// Human-readable description of the SQL dialect for LLM prompts.
    fn dialect_description(&self) -> &str;
}

/// Normalize a raw schema dump and return a canonical string.
///
/// Parses each statement with sqlparser, sorts columns within CREATE TABLE,
/// strips identifier quoting, and sorts statements alphabetically.
fn normalize_schema(dialect: &dyn Dialect, raw: &str) -> String {
    let mut statements: Vec<String> = raw
        .split(";\n\n")
        .map(|s| s.trim().trim_end_matches(';').trim())
        .filter(|s| !s.is_empty())
        .map(|s| normalize_ddl(dialect, s))
        .collect();
    statements.sort();
    let mut result = statements.join("\n\n");
    result.push('\n');
    result
}

/// Compare two raw schema dumps and return a unified diff.
///
/// Normalizes both sides (parses, sorts columns, sorts statements) before
/// comparing. Returns an empty string if the schemas match.
pub fn schema_diff(dialect: &dyn Dialect, left: &str, left_label: &str, right: &str, right_label: &str) -> String {
    let left_norm = normalize_schema(dialect, left);
    let right_norm = normalize_schema(dialect, right);

    if left_norm == right_norm {
        return String::new();
    }
    let diff = text_diff(&left_norm, &right_norm);
    format!("--- {left_label}\n+++ {right_label}\n{diff}")
}
