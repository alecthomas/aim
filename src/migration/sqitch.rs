use std::path::Path;

use super::{Error, Migration, MigrationFormat, wrap_sql};

/// Sqitch format: three directories (`deploy/`, `revert/`, `verify/`) plus a `sqitch.plan` file.
///
/// Each change produces `deploy/name.sql`, `revert/name.sql`, `verify/name.sql`.
/// The `sqitch.plan` file defines ordering and dependencies.
pub struct Sqitch;

impl MigrationFormat for Sqitch {
    fn list(&self, dir: &Path) -> Result<Vec<Migration>, Error> {
        let plan_path = dir.join("sqitch.plan");
        if !plan_path.exists() {
            return Ok(Vec::new());
        }

        let plan = std::fs::read_to_string(&plan_path)?;
        let names = parse_plan(&plan);

        let deploy_dir = dir.join("deploy");
        let revert_dir = dir.join("revert");

        let mut migrations = Vec::with_capacity(names.len());
        for (seq, name) in names.into_iter().enumerate() {
            let up_path = deploy_dir.join(format!("{name}.sql"));
            let down_path = revert_dir.join(format!("{name}.sql"));

            let up_sql = if up_path.exists() {
                std::fs::read_to_string(&up_path)?
            } else {
                String::new()
            };
            let down_sql = if down_path.exists() {
                std::fs::read_to_string(&down_path)?
            } else {
                String::new()
            };

            migrations.push(Migration {
                sequence: (seq + 1) as u64,
                description: name,
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
        let deploy_dir = dir.join("deploy");
        let revert_dir = dir.join("revert");
        let verify_dir = dir.join("verify");
        std::fs::create_dir_all(&deploy_dir)?;
        std::fs::create_dir_all(&revert_dir)?;
        std::fs::create_dir_all(&verify_dir)?;

        let name = &migration.description;

        std::fs::write(
            deploy_dir.join(format!("{name}.sql")),
            wrap_sql(&migration.up_sql, prefix, suffix),
        )?;
        std::fs::write(
            revert_dir.join(format!("{name}.sql")),
            wrap_sql(&migration.down_sql, prefix, suffix),
        )?;
        // Verify script is empty — aim doesn't generate verification SQL.
        std::fs::write(verify_dir.join(format!("{name}.sql")), "")?;

        // Append to sqitch.plan.
        let plan_path = dir.join("sqitch.plan");
        let mut plan = if plan_path.exists() {
            std::fs::read_to_string(&plan_path)?
        } else {
            String::new()
        };
        if !plan.is_empty() && !plan.ends_with('\n') {
            plan.push('\n');
        }
        plan.push_str(name);
        plan.push('\n');
        std::fs::write(&plan_path, plan)?;

        Ok(())
    }

    fn next_sequence(&self, dir: &Path) -> Result<u64, Error> {
        let existing = self.list(dir)?;
        Ok(existing.last().map_or(1, |m| m.sequence + 1))
    }

    fn describe_written(&self, migration: &Migration) -> String {
        let name = &migration.description;
        format!("deploy/{name}.sql, revert/{name}.sql, verify/{name}.sql")
    }
}

/// Parse change names from a sqitch.plan file.
///
/// Each non-empty, non-comment line that doesn't start with `%` is a change name
/// (possibly followed by dependencies and metadata).
fn parse_plan(content: &str) -> Vec<String> {
    content
        .lines()
        .filter(|line| {
            let trimmed = line.trim();
            !trimmed.is_empty() && !trimmed.starts_with('#') && !trimmed.starts_with('%')
        })
        .map(|line| {
            // Change name is the first whitespace-delimited token.
            line.split_whitespace()
                .next()
                .unwrap_or(line)
                .to_owned()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_plan() {
        let plan = "# project\n\ncreate_users\nadd_email [create_users]\n";
        let names = parse_plan(plan);
        assert_eq!(names, vec!["create_users", "add_email"]);
    }

    #[test]
    fn test_roundtrip() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let fmt = Sqitch;
        let m = Migration {
            sequence: 1,
            description: "create_users".to_owned(),
            up_sql: "CREATE TABLE users (id INT);".to_owned(),
            down_sql: "DROP TABLE users;".to_owned(),
        };

        fmt.write(dir.path(), &m, "", "").expect("write");

        assert!(dir.path().join("deploy/create_users.sql").exists());
        assert!(dir.path().join("revert/create_users.sql").exists());
        assert!(dir.path().join("verify/create_users.sql").exists());
        assert!(dir.path().join("sqitch.plan").exists());

        let migrations = fmt.list(dir.path()).expect("list");
        assert_eq!(migrations.len(), 1);
        assert_eq!(migrations[0].up_sql, "CREATE TABLE users (id INT);");
        assert_eq!(migrations[0].down_sql, "DROP TABLE users;");
    }
}
