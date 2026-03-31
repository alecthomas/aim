pub mod sqlite;

use std::fmt;

/// Errors from database engine operations.
#[derive(Debug)]
pub enum Error {
    /// Failed to create or connect to an ephemeral database.
    Connection(String),
    /// Failed to execute SQL.
    Execution(String),
    /// Failed to compute schema diff.
    Diff(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Connection(msg) => write!(f, "engine connection: {msg}"),
            Error::Execution(msg) => write!(f, "engine execution: {msg}"),
            Error::Diff(msg) => write!(f, "engine diff: {msg}"),
        }
    }
}

impl std::error::Error for Error {}

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

    /// Compare the schemas of two ephemeral databases.
    ///
    /// `left_label` and `right_label` describe what each side represents
    /// (e.g. "schema.sql" vs "migration result") for human-readable output.
    ///
    /// Returns an empty string if the schemas match, or a unified diff
    /// of the differences.
    fn diff(
        &self,
        left: &EphemeralDb,
        left_label: &str,
        right: &EphemeralDb,
        right_label: &str,
    ) -> Result<String, Error>;

    /// Dump the full schema of an ephemeral database as normalized DDL.
    ///
    /// Used to produce a canonical representation of the schema for the LLM,
    /// ensuring it sees the same form that the diff comparison uses.
    fn dump_schema(&self, db: &EphemeralDb) -> Result<String, Error>;

    /// Tear down an ephemeral database and clean up resources.
    fn drop_ephemeral(&self, db: EphemeralDb) -> Result<(), Error>;

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
