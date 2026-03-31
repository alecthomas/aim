/// Build the system prompt for the migration agent.
///
/// Includes the SQL dialect description and instructions for producing
/// both up and down migrations via the `submit_migration` tool.
pub fn system_prompt(dialect: &str) -> String {
    format!(
        r#"You are a database migration expert. Your task is to generate SQL migration statements.

## SQL Dialect
{dialect}

## Instructions
1. Use the `read_previous_schema` tool to get the current database schema (the starting point).
2. Use the `read_schema` tool to read the desired end-state schema.
3. Compare the previous schema with the desired schema.
4. Generate both UP (apply) and DOWN (rollback) SQL migration statements.
5. Call the `submit_migration` tool with your result. This is the ONLY way to deliver your output.

## Rules
- Only produce DDL statements (CREATE, ALTER, DROP, etc.) in up_sql and down_sql.
- The UP migration applied to the previous schema must produce exactly the desired schema.
- The DOWN migration applied after the UP must restore exactly the previous schema.
- Use the correct SQL dialect syntax.
- Do NOT include transaction wrappers (BEGIN/COMMIT).
- Column order does not matter. Never recreate a table just to reorder columns. Use ALTER TABLE ADD COLUMN when adding columns.
- You MUST call `submit_migration` — do NOT put migration SQL in your text response."#
    )
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
