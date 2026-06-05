/// Build the system prompt for the migration agent.
///
/// Includes the SQL dialect description and instructions for producing the
/// migration via the `submit_migration` tool. When `no_down` is `true`, the
/// agent is told not to produce a down (rollback) migration.
pub fn system_prompt(dialect: &str, context: Option<&str>, no_down: bool) -> String {
    // Down migrations can silently destroy data on rollback (e.g. dropping a
    // column that UP added), so they are optional and controlled by `no_down`.
    let (submit_fields, down_ddl_rule, down_rules) = if no_down {
        (
            "up_sql, description, and seed_data (do NOT provide down_sql)",
            "- Use DDL statements (CREATE, ALTER, DROP, etc.) in up_sql.",
            "",
        )
    } else {
        (
            "up_sql, down_sql, description, and seed_data",
            "- Use DDL statements (CREATE, ALTER, DROP, etc.) in up_sql and down_sql.",
            "\n- DOWN applied after UP must restore exactly the previous schema.",
        )
    };
    // Seed-data guidance that only applies when a down migration exists.
    let down_seed_field = if no_down {
        ""
    } else {
        "\n- `expected_after_down`: what the rows should look like after DOWN is applied. \
         This should match `rows` exactly."
    };
    let order_bullet = if no_down {
        "Rows in expected_after_up must be in the same order as rows."
    } else {
        "Rows in expected_after_up and expected_after_down must be in the same order as rows."
    };
    let dropped_bullet = if no_down {
        "For tables dropped by UP, still include them — the rows should exist before UP."
    } else {
        "For tables dropped by UP, still include them — the rows should exist before UP and after DOWN."
    };
    let sql_targets = if no_down { "up_sql" } else { "up_sql or down_sql" };
    let mut prompt = format!(
        r#"You are a database migration generator. Explain what you're doing as you think.

Call `read_previous_schema` and `read_schema`, then call `submit_migration` with {submit_fields}.

SQL dialect: {dialect}

## Migration Rules
{down_ddl_rule}
- DML is allowed ONLY to transform data that already exists: use UPDATE/DELETE,
  or `INSERT ... SELECT ... FROM <existing table>` (e.g. when rebuilding a table,
  copy rows with `INSERT INTO new_table SELECT ... FROM old_table`).
- NEVER write `INSERT ... VALUES` or otherwise insert literal/sample rows.
  Migrations run against real production data, not the seed data. Seed data is
  provided separately in `seed_data` for verification ONLY and must never appear
  in {sql_targets}.
- UP applied to previous schema must produce exactly the desired schema.{down_rules}
- Do NOT include transaction wrappers (BEGIN/COMMIT).
- Column order does not matter. Never recreate a table just to reorder columns.
- Use ALTER TABLE ADD COLUMN when adding columns.
- Migrations MUST be fully automated. NEVER include comments or instructions
  requiring manual intervention. If data needs to be transformed, use UPDATE/DELETE
  or `INSERT ... SELECT` deriving from existing rows.

## Seed Data Rules
You MUST provide seed_data to verify that migrations preserve existing data.

The seed_data field is a JSON object keyed by table name. For EVERY table that exists in the PREVIOUS schema, provide an entry with:
- `rows`: at least 2 rows of realistic sample data to INSERT before applying UP.
- `expected_after_up`: what those rows should look like after UP is applied. Reflect any column additions (with their DEFAULT values), column removals, renames, or type changes.{down_seed_field}

Each row is a JSON object mapping column names to JSON values. Use JSON types directly: strings as JSON strings, numbers as JSON numbers, booleans as JSON booleans, null as JSON null. Do NOT use SQL literal syntax. For array-typed columns, supply a JSON array (e.g. ["a", "b"], or [] for empty) — it is converted to the database's native array syntax automatically; do NOT encode the array as a string.

IMPORTANT:
- Respect foreign key dependencies: list parent tables before child tables.
- For auto-increment / serial primary keys, provide explicit integer IDs (1, 2, etc.).
- For columns with DEFAULT values, still provide explicit values in `rows`.
- For new columns added by UP with a DEFAULT, include them in `expected_after_up` with the DEFAULT value applied to existing rows.
- For columns dropped by UP, omit them from `expected_after_up`.
- {order_bullet}
- For tables created by UP (not in previous schema), omit them from seed_data.
- {dropped_bullet}"#
    );

    if let Some(ctx) = context {
        prompt.push_str("\n\n## Additional Context\n");
        prompt.push_str(ctx);
    }

    prompt
}

/// Build a retry correction message with the diff feedback and the
/// candidate SQL so the LLM can see exactly what it produced.
pub fn retry_message(up_diff: &str, down_diff: &str, up_sql: &str, down_sql: &str) -> String {
    let mut msg = "Your previous migration was incorrect.\n\n".to_string();

    msg.push_str("## Your UP SQL\n```sql\n");
    msg.push_str(up_sql);
    msg.push_str("\n```\n\n");

    if !down_sql.is_empty() {
        msg.push_str("## Your DOWN SQL\n```sql\n");
        msg.push_str(down_sql);
        msg.push_str("\n```\n\n");
    }

    if !up_diff.is_empty() {
        msg.push_str("## UP migration diff (expected vs actual):\n```\n");
        msg.push_str(up_diff);
        msg.push_str("\n```\n\n");
    }

    if !down_diff.is_empty() {
        msg.push_str("## DOWN migration diff (expected vs actual):\n```\n");
        msg.push_str(down_diff);
        msg.push_str("\n```\n\n");
    }

    msg.push_str("Please fix the migration SQL and call `submit_migration` again with the corrected result.");
    msg
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_system_prompt_contains_seed_data_instructions() {
        let prompt = system_prompt("SQLite", None, false);
        assert!(prompt.contains("seed_data"), "prompt must mention seed_data");
        assert!(
            prompt.contains("expected_after_up"),
            "prompt must mention expected_after_up"
        );
        assert!(
            prompt.contains("expected_after_down"),
            "prompt must mention expected_after_down"
        );
        assert!(
            prompt.contains("foreign key dependencies"),
            "prompt must mention FK ordering"
        );
    }

    #[test]
    fn test_system_prompt_with_context() {
        let prompt = system_prompt("PostgreSQL", Some("Use IF NOT EXISTS"), false);
        assert!(prompt.contains("Use IF NOT EXISTS"));
        assert!(prompt.contains("Additional Context"));
    }

    #[test]
    fn test_system_prompt_no_down_omits_down_instructions() {
        let prompt = system_prompt("SQLite", None, true);
        assert!(
            prompt.contains("do NOT provide down_sql"),
            "no_down prompt must tell the model not to provide down_sql"
        );
        assert!(
            !prompt.contains("up_sql, down_sql"),
            "no_down prompt must not request down_sql as an output field"
        );
        assert!(
            !prompt.contains("expected_after_down"),
            "no_down prompt must not request expected_after_down"
        );
        assert!(
            !prompt.contains("DOWN applied after UP"),
            "no_down prompt must not state the DOWN restoration rule"
        );
        // UP rules and seed data are still required.
        assert!(prompt.contains("expected_after_up"));
        assert!(prompt.contains("seed_data"));
    }

    #[test]
    fn test_retry_message_omits_down_block_when_empty() {
        let msg = retry_message("+ col added", "", "ALTER TABLE t ADD COLUMN c INT;", "");
        assert!(!msg.contains("Your DOWN SQL"));
        assert!(msg.contains("Your UP SQL"));
    }

    #[test]
    fn test_retry_message_includes_diffs() {
        let msg = retry_message(
            "+ col added",
            "- col removed",
            "ALTER TABLE t ADD COLUMN c INT;",
            "ALTER TABLE t DROP COLUMN c;",
        );
        assert!(msg.contains("+ col added"));
        assert!(msg.contains("- col removed"));
        assert!(msg.contains("ALTER TABLE t ADD COLUMN c INT;"));
        assert!(msg.contains("ALTER TABLE t DROP COLUMN c;"));
    }

    #[test]
    fn test_retry_message_omits_empty_diffs() {
        let msg = retry_message("", "", "SELECT 1;", "SELECT 1;");
        assert!(!msg.contains("UP migration diff"));
        assert!(!msg.contains("DOWN migration diff"));
    }
}
