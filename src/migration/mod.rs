pub mod dbmate;
pub mod flyway;
pub mod golang_migrate;
pub mod goose;
pub mod refinery;
pub mod sqitch;
pub mod sqlx;

use std::fmt;
use std::path::Path;

/// Errors related to migration file I/O and parsing.
#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(err) => write!(f, "migration I/O: {err}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(err) => Some(err),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::Io(err)
    }
}

/// Direction of a migration (up = apply, down = rollback).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Up,
    Down,
}

impl fmt::Display for Direction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Direction::Up => write!(f, "up"),
            Direction::Down => write!(f, "down"),
        }
    }
}

/// A single migration with its sequence number, description, and SQL content.
#[derive(Debug, Clone)]
pub struct Migration {
    pub sequence: u64,
    pub description: String,
    pub up_sql: String,
    pub down_sql: String,
}

/// Trait for reading and writing migration files in different tool formats.
pub trait MigrationFormat: Send + Sync {
    /// List all migrations in the directory, ordered by sequence.
    fn list(&self, dir: &Path) -> Result<Vec<Migration>, Error>;

    /// Write a migration to the directory.
    ///
    /// `prefix` and `suffix` are engine-specific SQL to wrap the migration
    /// body (e.g. PRAGMA statements for SQLite).
    fn write(
        &self,
        dir: &Path,
        migration: &Migration,
        prefix: &str,
        suffix: &str,
    ) -> Result<(), Error>;

    /// Determine the next sequence number for a new migration.
    fn next_sequence(&self, dir: &Path) -> Result<u64, Error>;

    /// Human-readable description of files written, for display.
    fn describe_written(&self, migration: &Migration) -> String;
}

/// Helper: generate a UTC timestamp string as `YYYYMMDDHHMMSS`.
fn timestamp_now() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_secs();
    // Convert unix seconds to a UTC datetime string.
    let days = secs / 86400;
    let time_of_day = secs % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;

    // Days since 1970-01-01 to (year, month, day).
    let (year, month, day) = days_to_ymd(days);

    format!("{year:04}{month:02}{day:02}{hours:02}{minutes:02}{seconds:02}")
}

/// Convert days since 1970-01-01 to (year, month, day).
fn days_to_ymd(days: u64) -> (u64, u64, u64) {
    // Civil calendar algorithm from Howard Hinnant.
    let z = days + 719468;
    let era = z / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

/// Helper: wrap SQL body with optional prefix/suffix.
fn wrap_sql(sql: &str, prefix: &str, suffix: &str) -> String {
    let mut content = format!("{prefix}{sql}");
    if !suffix.is_empty() {
        content.push('\n');
        content.push_str(suffix);
    }
    content
}
