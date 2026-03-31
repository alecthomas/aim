use std::path::Path;

use super::{Error, Migration, MigrationFormat, timestamp_now, wrap_sql};

/// goose format: timestamped single files with `-- +goose Up` / `-- +goose Down` directives.
///
/// Filename: `YYYYMMDDHHMMSS_description.sql`
pub struct Goose;

impl MigrationFormat for Goose {
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
            let content = std::fs::read_to_string(path)?;
            let (up_sql, down_sql) = parse_sections(&content);

            migrations.push(Migration {
                sequence: *seq,
                description: description.clone(),
                up_sql,
                down_sql,
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
            "{}_{}.sql",
            migration.sequence, migration.description
        );
        let up_body = wrap_sql(&migration.up_sql, prefix, suffix);
        let down_body = wrap_sql(&migration.down_sql, prefix, suffix);
        let content = format!("-- +goose Up\n{up_body}\n-- +goose Down\n{down_body}\n");

        std::fs::write(dir.join(filename), content)?;
        Ok(())
    }

    fn next_sequence(&self, dir: &Path) -> Result<u64, Error> {
        let ts: u64 = timestamp_now().parse().expect("timestamp is numeric");
        // Ensure we don't collide with an existing migration.
        let existing = self.list(dir)?;
        let max_existing = existing.last().map_or(0, |m| m.sequence);
        Ok(ts.max(max_existing + 1))
    }

    fn describe_written(&self, migration: &Migration) -> String {
        format!("{}_{}.sql", migration.sequence, migration.description)
    }
}

/// Parse `YYYYMMDDHHMMSS_description.sql` into (sequence, description).
fn parse_filename(name: &str) -> Option<(u64, String)> {
    let stem = name.strip_suffix(".sql")?;
    let (seq_str, description) = stem.split_once('_')?;
    if seq_str.len() != 14 {
        return None;
    }
    let seq: u64 = seq_str.parse().ok()?;
    Some((seq, description.to_owned()))
}

/// Split file content on `-- +goose Up` and `-- +goose Down` markers.
fn parse_sections(content: &str) -> (String, String) {
    let mut up_lines = Vec::new();
    let mut down_lines = Vec::new();
    let mut current: Option<&str> = None;

    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.eq_ignore_ascii_case("-- +goose up") {
            current = Some("up");
            continue;
        }
        if trimmed.eq_ignore_ascii_case("-- +goose down") {
            current = Some("down");
            continue;
        }
        match current {
            Some("up") => up_lines.push(line),
            Some("down") => down_lines.push(line),
            _ => {}
        }
    }

    (
        up_lines.join("\n").trim().to_owned(),
        down_lines.join("\n").trim().to_owned(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_filename() {
        assert_eq!(
            parse_filename("20230101120000_create_users.sql"),
            Some((20230101120000, "create_users".to_owned()))
        );
        assert_eq!(parse_filename("001_short.sql"), None);
        assert_eq!(parse_filename("not_a_migration.txt"), None);
    }

    #[test]
    fn test_parse_sections() {
        let content = "-- +goose Up\nCREATE TABLE t (id INT);\n\n-- +goose Down\nDROP TABLE t;\n";
        let (up, down) = parse_sections(content);
        assert_eq!(up, "CREATE TABLE t (id INT);");
        assert_eq!(down, "DROP TABLE t;");
    }

    #[test]
    fn test_roundtrip() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let fmt = Goose;
        let m = Migration {
            sequence: 20230101120000,
            description: "create_users".to_owned(),
            up_sql: "CREATE TABLE users (id INT);".to_owned(),
            down_sql: "DROP TABLE users;".to_owned(),
        };

        fmt.write(dir.path(), &m, "", "").expect("write");
        let migrations = fmt.list(dir.path()).expect("list");
        assert_eq!(migrations.len(), 1);
        assert_eq!(migrations[0].up_sql, "CREATE TABLE users (id INT);");
        assert_eq!(migrations[0].down_sql, "DROP TABLE users;");
    }
}
