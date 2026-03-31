use std::fmt;
use std::path::{Path, PathBuf};

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
    pub sequence: u32,
    pub description: String,
    pub up_sql: String,
    pub down_sql: String,
}

impl Migration {
    /// Filename for one direction, e.g. `001_initial.up.sql`.
    pub fn filename(&self, direction: Direction) -> String {
        format!("{:03}_{}.{direction}.sql", self.sequence, self.description)
    }
}

/// List all migrations in `migrations_dir`, ordered by sequence number.
///
/// Reads both `.up.sql` and `.down.sql` for each sequence number.
pub fn list(migrations_dir: &Path) -> Result<Vec<Migration>, Error> {
    if !migrations_dir.exists() {
        return Ok(Vec::new());
    }

    let mut entries: Vec<(u32, String, PathBuf)> = Vec::new();

    for entry in std::fs::read_dir(migrations_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(parsed) = parse_filename(&name) {
            entries.push((parsed.0, parsed.1, entry.path()));
        }
    }

    // Collect unique sequence numbers, sorted.
    let mut sequences: Vec<u32> = entries.iter().map(|(seq, _, _)| *seq).collect();
    sequences.sort_unstable();
    sequences.dedup();

    let mut migrations = Vec::with_capacity(sequences.len());
    for seq in sequences {
        let description = entries
            .iter()
            .find(|(s, _, _)| *s == seq)
            .map(|(_, desc, _)| desc.clone())
            .expect("sequence exists in entries");

        let up_path = migrations_dir.join(format!("{seq:03}_{description}.up.sql"));
        let down_path = migrations_dir.join(format!("{seq:03}_{description}.down.sql"));

        let up_sql = std::fs::read_to_string(&up_path)?;
        let down_sql = std::fs::read_to_string(&down_path)?;

        migrations.push(Migration {
            sequence: seq,
            description,
            up_sql,
            down_sql,
        });
    }

    Ok(migrations)
}

/// Write a migration pair to disk.
pub fn write(migrations_dir: &Path, migration: &Migration, prefix: &str, suffix: &str) -> Result<(), Error> {
    std::fs::create_dir_all(migrations_dir)?;

    for direction in [Direction::Up, Direction::Down] {
        let sql = match direction {
            Direction::Up => &migration.up_sql,
            Direction::Down => &migration.down_sql,
        };
        let path = migrations_dir.join(migration.filename(direction));
        let mut content = format!("{prefix}{sql}");
        if !suffix.is_empty() {
            content.push('\n');
            content.push_str(suffix);
        }
        std::fs::write(&path, content)?;
    }

    Ok(())
}

/// Parse a migration filename like `001_add_users.up.sql` into (sequence, description).
/// Returns `None` if the name doesn't match the expected pattern.
fn parse_filename(name: &str) -> Option<(u32, String)> {
    let stem = name
        .strip_suffix(".up.sql")
        .or_else(|| name.strip_suffix(".down.sql"))?;

    let (seq_str, description) = stem.split_once('_')?;
    let seq: u32 = seq_str.parse().ok()?;
    Some((seq, description.to_owned()))
}

/// Determine the next sequence number based on existing migrations.
pub fn next_sequence(migrations_dir: &Path) -> Result<u32, Error> {
    let existing = list(migrations_dir)?;
    Ok(existing.last().map_or(1, |m| m.sequence + 1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_filename() {
        assert_eq!(parse_filename("001_initial.up.sql"), Some((1, "initial".to_owned())));
        assert_eq!(
            parse_filename("042_add_email_to_users.down.sql"),
            Some((42, "add_email_to_users".to_owned()))
        );
        assert_eq!(parse_filename("not_a_migration.sql"), None);
        assert_eq!(parse_filename("abc_bad.up.sql"), None);
    }

    #[test]
    fn test_roundtrip_write_and_list() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let m = Migration {
            sequence: 1,
            description: "initial".to_owned(),
            up_sql: "CREATE TABLE users (id INT);".to_owned(),
            down_sql: "DROP TABLE users;".to_owned(),
        };

        write(dir.path(), &m, "", "").expect("write migration");

        assert!(dir.path().join("001_initial.up.sql").exists());
        assert!(dir.path().join("001_initial.down.sql").exists());

        let migrations = list(dir.path()).expect("list migrations");
        assert_eq!(migrations.len(), 1);
        assert_eq!(migrations[0].sequence, 1);
        assert_eq!(migrations[0].description, "initial");
        assert_eq!(migrations[0].up_sql, "CREATE TABLE users (id INT);");
        assert_eq!(migrations[0].down_sql, "DROP TABLE users;");
    }

    #[test]
    fn test_next_sequence_empty() {
        let dir = tempfile::tempdir().expect("create temp dir");
        assert_eq!(next_sequence(dir.path()).expect("next seq"), 1);
    }

    #[test]
    fn test_next_sequence_with_existing() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let m = Migration {
            sequence: 3,
            description: "foo".to_owned(),
            up_sql: "SELECT 1;".to_owned(),
            down_sql: "SELECT 2;".to_owned(),
        };
        write(dir.path(), &m, "", "").expect("write");
        assert_eq!(next_sequence(dir.path()).expect("next seq"), 4);
    }
}
