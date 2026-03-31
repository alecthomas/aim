use std::path::Path;

use super::{Error, Migration, MigrationFormat, wrap_sql};

/// Refinery format: prefixed files `V{n}__description.sql`.
///
/// Sequential versioning. Down migrations are not natively supported;
/// the down SQL is stored but not written to disk.
pub struct Refinery;

impl MigrationFormat for Refinery {
    fn list(&self, dir: &Path) -> Result<Vec<Migration>, Error> {
        if !dir.exists() {
            return Ok(Vec::new());
        }

        let mut entries: Vec<(u64, String, std::path::PathBuf)> = Vec::new();

        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(parsed) = parse_filename(&name) {
                entries.push((parsed.0, parsed.1, entry.path()));
            }
        }

        entries.sort_unstable_by_key(|(seq, _, _)| *seq);

        let mut migrations = Vec::with_capacity(entries.len());
        for (seq, description, path) in &entries {
            let up_sql = std::fs::read_to_string(path)?;

            migrations.push(Migration {
                sequence: *seq,
                description: description.clone(),
                up_sql,
                down_sql: String::new(),
            });
        }

        Ok(migrations)
    }

    fn write(
        &self,
        dir: &Path,
        migration: &Migration,
        prefix: &str,
        suffix: &str,
    ) -> Result<(), Error> {
        std::fs::create_dir_all(dir)?;

        let filename = format!(
            "V{}__{}",
            migration.sequence, migration.description
        );
        std::fs::write(
            dir.join(format!("{filename}.sql")),
            wrap_sql(&migration.up_sql, prefix, suffix),
        )?;

        Ok(())
    }

    fn next_sequence(&self, dir: &Path) -> Result<u64, Error> {
        let existing = self.list(dir)?;
        Ok(existing.last().map_or(1, |m| m.sequence + 1))
    }

    fn describe_written(&self, migration: &Migration) -> String {
        format!(
            "V{}__{}.sql (no down migration)",
            migration.sequence, migration.description
        )
    }
}

/// Parse `V{n}__description.sql`.
fn parse_filename(name: &str) -> Option<(u64, String)> {
    let stem = name.strip_suffix(".sql")?;
    let rest = stem.strip_prefix('V')?;
    let (seq_str, desc) = rest.split_once("__")?;
    let seq: u64 = seq_str.parse().ok()?;
    Some((seq, desc.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_filename() {
        assert_eq!(
            parse_filename("V1__create_users.sql"),
            Some((1, "create_users".to_owned()))
        );
        assert_eq!(parse_filename("U1__create_users.sql"), None);
        assert_eq!(parse_filename("001_bad.sql"), None);
    }

    #[test]
    fn test_roundtrip() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let fmt = Refinery;
        let m = Migration {
            sequence: 1,
            description: "create_users".to_owned(),
            up_sql: "CREATE TABLE users (id INT);".to_owned(),
            down_sql: "DROP TABLE users;".to_owned(),
        };

        fmt.write(dir.path(), &m, "", "").expect("write");

        assert!(dir.path().join("V1__create_users.sql").exists());

        let migrations = fmt.list(dir.path()).expect("list");
        assert_eq!(migrations.len(), 1);
        assert_eq!(migrations[0].up_sql, "CREATE TABLE users (id INT);");
        // Refinery doesn't support down migrations on disk.
        assert_eq!(migrations[0].down_sql, "");
    }
}
