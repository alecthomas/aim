use std::path::Path;

use super::{Direction, Error, Migration, MigrationFormat, timestamp_now, wrap_sql};

/// sqlx-cli format: timestamped pairs `YYYYMMDDHHMMSS_description.up.sql` /
/// `YYYYMMDDHHMMSS_description.down.sql`.
pub struct Sqlx;

impl MigrationFormat for Sqlx {
    fn list(&self, dir: &Path) -> Result<Vec<Migration>, Error> {
        if !dir.exists() {
            return Ok(Vec::new());
        }

        let mut entries: Vec<(u64, String)> = Vec::new();

        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(parsed) = parse_filename(&name)
                && !entries.iter().any(|(s, _)| *s == parsed.0)
            {
                entries.push(parsed);
            }
        }

        entries.sort_unstable_by_key(|(seq, _)| *seq);

        let mut migrations = Vec::with_capacity(entries.len());
        for (seq, description) in &entries {
            let up_path = dir.join(format!("{seq}_{description}.up.sql"));
            let down_path = dir.join(format!("{seq}_{description}.down.sql"));

            let up_sql = std::fs::read_to_string(&up_path)?;
            let down_sql = if down_path.exists() {
                std::fs::read_to_string(&down_path)?
            } else {
                String::new()
            };

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
            let filename = format!("{}_{}.{direction}.sql", migration.sequence, migration.description);
            std::fs::write(dir.join(filename), wrap_sql(sql, prefix, suffix))?;
        }

        Ok(())
    }

    fn next_sequence(&self, dir: &Path) -> Result<u64, Error> {
        let ts: u64 = timestamp_now().parse().expect("timestamp is numeric");
        let existing = self.list(dir)?;
        let max_existing = existing.last().map_or(0, |m| m.sequence);
        Ok(ts.max(max_existing + 1))
    }

    fn describe_written(&self, migration: &Migration) -> String {
        format!("{}_{}.{{up,down}}.sql", migration.sequence, migration.description)
    }
}

/// Parse `YYYYMMDDHHMMSS_description.{up,down}.sql`.
fn parse_filename(name: &str) -> Option<(u64, String)> {
    let stem = name
        .strip_suffix(".up.sql")
        .or_else(|| name.strip_suffix(".down.sql"))?;

    let (seq_str, description) = stem.split_once('_')?;
    if seq_str.len() != 14 {
        return None;
    }
    let seq: u64 = seq_str.parse().ok()?;
    Some((seq, description.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_filename() {
        assert_eq!(
            parse_filename("20230101120000_create_users.up.sql"),
            Some((20230101120000, "create_users".to_owned()))
        );
        assert_eq!(
            parse_filename("20230101120000_create_users.down.sql"),
            Some((20230101120000, "create_users".to_owned()))
        );
        assert_eq!(parse_filename("001_short.up.sql"), None);
    }

    #[test]
    fn test_roundtrip() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let fmt = Sqlx;
        let m = Migration {
            sequence: 20230101120000,
            description: "create_users".to_owned(),
            up_sql: "CREATE TABLE users (id INT);".to_owned(),
            down_sql: "DROP TABLE users;".to_owned(),
        };

        fmt.write(dir.path(), &m, "", "").expect("write");

        assert!(dir.path().join("20230101120000_create_users.up.sql").exists());
        assert!(dir.path().join("20230101120000_create_users.down.sql").exists());

        let migrations = fmt.list(dir.path()).expect("list");
        assert_eq!(migrations.len(), 1);
        assert_eq!(migrations[0].up_sql, "CREATE TABLE users (id INT);");
        assert_eq!(migrations[0].down_sql, "DROP TABLE users;");
    }
}
