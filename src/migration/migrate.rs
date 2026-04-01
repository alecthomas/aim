use std::path::Path;

use super::{Direction, Error, Migration, MigrationFormat, wrap_sql};

/// migrate format: numbered pairs `000001_desc.up.sql` / `000001_desc.down.sql`.
pub struct Migrate;

impl MigrationFormat for Migrate {
    fn list(&self, dir: &Path) -> Result<Vec<Migration>, Error> {
        if !dir.exists() {
            return Ok(Vec::new());
        }

        // Collect (sequence, description, prefix) from .up.sql files.
        // We store the original numeric prefix so we can reconstruct the
        // exact filename for both up and down.
        let mut entries: Vec<(u64, String, String)> = Vec::new();

        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some((seq, desc, prefix)) = parse_filename(&name)
                && !entries.iter().any(|(s, _, _)| *s == seq)
            {
                entries.push((seq, desc, prefix));
            }
        }

        entries.sort_unstable_by_key(|(seq, _, _)| *seq);

        let mut migrations = Vec::with_capacity(entries.len());
        for (seq, description, prefix) in &entries {
            let up_path = dir.join(format!("{prefix}_{description}.up.sql"));
            let down_path = dir.join(format!("{prefix}_{description}.down.sql"));

            let up_sql = std::fs::read_to_string(&up_path)?;
            let down_sql = std::fs::read_to_string(&down_path)?;

            migrations.push(Migration {
                sequence: *seq,
                description: description.clone(),
                up_sql,
                down_sql,
            });
        }

        Ok(migrations)
    }

    fn write(&self, dir: &Path, migration: &Migration, prefix: &str, suffix: &str) -> Result<(), Error> {
        std::fs::create_dir_all(dir)?;

        for direction in [Direction::Up, Direction::Down] {
            let sql = match direction {
                Direction::Up => &migration.up_sql,
                Direction::Down => &migration.down_sql,
            };
            let filename = format!("{:06}_{}.{direction}.sql", migration.sequence, migration.description);
            let path = dir.join(filename);
            std::fs::write(&path, wrap_sql(sql, prefix, suffix))?;
        }

        Ok(())
    }

    fn next_sequence(&self, dir: &Path) -> Result<u64, Error> {
        let existing = self.list(dir)?;
        Ok(existing.last().map_or(1, |m| m.sequence + 1))
    }

    fn describe_written(&self, migration: &Migration) -> String {
        format!("{:06}_{}.{{up,down}}.sql", migration.sequence, migration.description)
    }
}

/// Parse a filename like `000001_add_users.up.sql` into (sequence, description, prefix).
///
/// The prefix is the original numeric string (e.g. "000001" or "001") so we
/// can reconstruct the exact filename without assuming a fixed width.
fn parse_filename(name: &str) -> Option<(u64, String, String)> {
    let stem = name
        .strip_suffix(".up.sql")
        .or_else(|| name.strip_suffix(".down.sql"))?;

    let (seq_str, description) = stem.split_once('_')?;
    let seq: u64 = seq_str.parse().ok()?;
    Some((seq, description.to_owned(), seq_str.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_filename() {
        assert_eq!(
            parse_filename("000001_initial.up.sql"),
            Some((1, "initial".to_owned(), "000001".to_owned()))
        );
        assert_eq!(
            parse_filename("000042_add_email.down.sql"),
            Some((42, "add_email".to_owned(), "000042".to_owned()))
        );
        // Also accept shorter prefixes (e.g. legacy 3-digit).
        assert_eq!(
            parse_filename("001_initial.up.sql"),
            Some((1, "initial".to_owned(), "001".to_owned()))
        );
        assert_eq!(parse_filename("not_a_migration.sql"), None);
        assert_eq!(parse_filename("abc_bad.up.sql"), None);
    }

    #[test]
    fn test_roundtrip_write_and_list() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let fmt = Migrate;
        let m = Migration {
            sequence: 1,
            description: "initial".to_owned(),
            up_sql: "CREATE TABLE users (id INT);".to_owned(),
            down_sql: "DROP TABLE users;".to_owned(),
        };

        fmt.write(dir.path(), &m, "", "").expect("write migration");

        assert!(dir.path().join("000001_initial.up.sql").exists());
        assert!(dir.path().join("000001_initial.down.sql").exists());

        let migrations = fmt.list(dir.path()).expect("list migrations");
        assert_eq!(migrations.len(), 1);
        assert_eq!(migrations[0].sequence, 1);
        assert_eq!(migrations[0].description, "initial");
        assert_eq!(migrations[0].up_sql, "CREATE TABLE users (id INT);");
        assert_eq!(migrations[0].down_sql, "DROP TABLE users;");
    }

    #[test]
    fn test_next_sequence_empty() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let fmt = Migrate;
        assert_eq!(fmt.next_sequence(dir.path()).expect("next seq"), 1);
    }

    #[test]
    fn test_next_sequence_with_existing() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let fmt = Migrate;
        let m = Migration {
            sequence: 3,
            description: "foo".to_owned(),
            up_sql: "SELECT 1;".to_owned(),
            down_sql: "SELECT 2;".to_owned(),
        };
        fmt.write(dir.path(), &m, "", "").expect("write");
        assert_eq!(fmt.next_sequence(dir.path()).expect("next seq"), 4);
    }

    #[test]
    fn test_describe_written() {
        let fmt = Migrate;
        let m = Migration {
            sequence: 1,
            description: "initial".to_owned(),
            up_sql: String::new(),
            down_sql: String::new(),
        };
        assert_eq!(fmt.describe_written(&m), "000001_initial.{up,down}.sql");
    }
}
