/// Build the system prompt for the migration agent.
///
/// Includes the SQL dialect description and instructions for producing
/// both up and down migrations via the `submit_migration` tool.
pub fn system_prompt(dialect: &str, context: Option<&str>) -> String {
    let mut prompt = format!(
        r#"You are a VERY SUCCINCT database migration generator. DO NOT EXPLAIN WHAT YOU'RE DOING, JUST DO IT.

Call `read_previous_schema` and `read_schema`, then call `submit_migration` with up_sql, down_sql, and a VERY SHORT snake_case description.

SQL dialect: {dialect}

Rules:
- Only DDL statements (CREATE, ALTER, DROP, etc.) in up_sql and down_sql.
- UP applied to previous schema must produce exactly the desired schema.
- DOWN applied after UP must restore exactly the previous schema.
- Do NOT include transaction wrappers (BEGIN/COMMIT).
- Column order does not matter. Never recreate a table just to reorder columns.
- Use ALTER TABLE ADD COLUMN when adding columns."#
    );

    if let Some(ctx) = context {
        prompt.push_str("\n\n## Additional Context\n");
        prompt.push_str(ctx);
    }

    prompt
}

/// Build a retry correction message with the diff feedback.
pub fn retry_message(up_diff: &str, down_diff: &str) -> String {
    let mut msg = "Your previous migration was incorrect. Here are the diffs:\n\n".to_string();

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
