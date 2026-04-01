use std::path::Path;

use super::{Error, Migration, MigrationFormat, wrap_sql};

/// Flyway format: versioned files `V{n}__description.sql`, undo files `U{n}__description.sql`.
///
/// Repeatable migrations (`R__description.sql`) are not supported by aim.
pub struct Flyway;

impl MigrationFormat for Flyway {
    fn list(&self, dir: &Path) -> Result<Vec<Migration>, Error> {
        if !dir.exists() {
            return Ok(Vec::new());
        }

        let mut versioned: Vec<(u64, String, String)> = Vec::new(); // (seq, desc, up_sql)
        let mut undos: Vec<(u64, String)> = Vec::new(); // (seq, down_sql)

        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();

            if let Some((seq, desc)) = parse_versioned(&name) {
                let sql = std::fs::read_to_string(entry.path())?;
                versioned.push((seq, desc, sql));
            } else if let Some((seq, sql)) = parse_undo(&name, &entry.path())? {
                undos.push((seq, sql));
            }
        }

        versioned.sort_unstable_by_key(|(seq, _, _)| *seq);

        let mut migrations = Vec::with_capacity(versioned.len());
        for (seq, description, up_sql) in versioned {
            let down_sql = undos
                .iter()
                .find(|(s, _)| *s == seq)
                .map(|(_, sql)| sql.clone())
                .unwrap_or_default();

            migrations.push(Migration {
                sequence: seq,
                description,
                up_sql,
                down_sql,
            });
        }

        Ok(migrations)
    }

    fn write(&self, dir: &Path, migration: &Migration, prefix: &str, suffix: &str) -> Result<(), Error> {
        std::fs::create_dir_all(dir)?;

        let v_name = format!(
            "V{}__{}",
            migration.sequence,
            flyway_description(&migration.description)
        );
        std::fs::write(
            dir.join(format!("{v_name}.sql")),
            wrap_sql(&migration.up_sql, prefix, suffix),
        )?;

        if !migration.down_sql.is_empty() {
            let u_name = format!(
                "U{}__{}",
                migration.sequence,
                flyway_description(&migration.description)
            );
            std::fs::write(
                dir.join(format!("{u_name}.sql")),
                wrap_sql(&migration.down_sql, prefix, suffix),
            )?;
        }

        Ok(())
    }

    fn next_sequence(&self, dir: &Path) -> Result<u64, Error> {
        let existing = self.list(dir)?;
        Ok(existing.last().map_or(1, |m| m.sequence + 1))
    }

    fn describe_written(&self, migration: &Migration) -> String {
        let desc = flyway_description(&migration.description);
        if migration.down_sql.is_empty() {
            format!("V{}__{desc}.sql", migration.sequence)
        } else {
            format!(
                "V{}__{desc}.sql, U{}__{desc}.sql",
                migration.sequence, migration.sequence
            )
        }
    }
}

/// Flyway uses CamelCase descriptions separated by underscores in filenames,
/// but we'll keep the user's snake_case description and just use double-underscore
/// as the separator between version and description (as Flyway requires).
fn flyway_description(desc: &str) -> String {
    desc.to_owned()
}

/// Parse `V{n}__description.sql`.
fn parse_versioned(name: &str) -> Option<(u64, String)> {
    let stem = name.strip_suffix(".sql")?;
    let rest = stem.strip_prefix('V')?;
    let (seq_str, desc) = rest.split_once("__")?;
    let seq: u64 = seq_str.parse().ok()?;
    Some((seq, desc.to_owned()))
}

/// Parse `U{n}__description.sql` and read its contents.
fn parse_undo(name: &str, path: &Path) -> Result<Option<(u64, String)>, Error> {
    let Some(stem) = name.strip_suffix(".sql") else {
        return Ok(None);
    };
    let Some(rest) = stem.strip_prefix('U') else {
        return Ok(None);
    };
    let Some((seq_str, _desc)) = rest.split_once("__") else {
        return Ok(None);
    };
    let Ok(seq) = seq_str.parse::<u64>() else {
        return Ok(None);
    };
    let sql = std::fs::read_to_string(path)?;
    Ok(Some((seq, sql)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_versioned() {
        assert_eq!(
            parse_versioned("V1__create_users.sql"),
            Some((1, "create_users".to_owned()))
        );
        assert_eq!(parse_versioned("U1__create_users.sql"), None);
        assert_eq!(parse_versioned("R__views.sql"), None);
    }

    #[test]
    fn test_roundtrip() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let fmt = Flyway;
        let m = Migration {
            sequence: 1,
            description: "create_users".to_owned(),
            up_sql: "CREATE TABLE users (id INT);".to_owned(),
            down_sql: "DROP TABLE users;".to_owned(),
        };

        fmt.write(dir.path(), &m, "", "").expect("write");

        assert!(dir.path().join("V1__create_users.sql").exists());
        assert!(dir.path().join("U1__create_users.sql").exists());

        let migrations = fmt.list(dir.path()).expect("list");
        assert_eq!(migrations.len(), 1);
        assert_eq!(migrations[0].up_sql, "CREATE TABLE users (id INT);");
        assert_eq!(migrations[0].down_sql, "DROP TABLE users;");
    }

    #[test]
    fn test_no_undo() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let fmt = Flyway;
        let m = Migration {
            sequence: 1,
            description: "create_users".to_owned(),
            up_sql: "CREATE TABLE users (id INT);".to_owned(),
            down_sql: String::new(),
        };

        fmt.write(dir.path(), &m, "", "").expect("write");

        assert!(dir.path().join("V1__create_users.sql").exists());
        assert!(!dir.path().join("U1__create_users.sql").exists());
    }
}
