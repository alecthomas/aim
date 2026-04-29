use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use rig::completion::ToolDefinition;
use rig::tool::Tool;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Tool error type for agent tools.
#[derive(Debug)]
pub struct ToolError(String);

impl std::fmt::Display for ToolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for ToolError {}

// ── read_schema ──────────────────────────────────────────────────────

/// Tool that returns the normalized desired schema (from `schema.sql`).
pub struct ReadSchema {
    pub desired_ddl: Arc<String>,
}

#[derive(Deserialize, JsonSchema)]
pub struct ReadSchemaArgs {}

impl Tool for ReadSchema {
    const NAME: &'static str = "read_schema";
    type Error = ToolError;
    type Args = ReadSchemaArgs;
    type Output = String;

    async fn definition(&self, _prompt: String) -> ToolDefinition {
        ToolDefinition {
            name: "read_schema".into(),
            description: "Read the desired end-state schema (normalized from schema.sql)".into(),
            parameters: serde_json::to_value(schemars::schema_for!(ReadSchemaArgs))
                .expect("schema serialization should not fail"),
        }
    }

    async fn call(&self, _args: Self::Args) -> Result<Self::Output, Self::Error> {
        Ok(self.desired_ddl.as_ref().clone())
    }
}

// ── read_previous_schema ──────────────────────────────────────────────

/// Tool that returns the previous schema (result of replaying all existing migrations).
pub struct ReadPreviousSchema {
    pub previous_ddl: Arc<String>,
}

#[derive(Deserialize, JsonSchema)]
pub struct ReadPreviousSchemaArgs {}

impl Tool for ReadPreviousSchema {
    const NAME: &'static str = "read_previous_schema";
    type Error = ToolError;
    type Args = ReadPreviousSchemaArgs;
    type Output = String;

    async fn definition(&self, _prompt: String) -> ToolDefinition {
        ToolDefinition {
            name: "read_previous_schema".into(),
            description: "Get the previous database schema (the result of replaying all existing \
                          migrations). This is the starting point for the new migration."
                .into(),
            parameters: serde_json::to_value(schemars::schema_for!(ReadPreviousSchemaArgs))
                .expect("schema serialization should not fail"),
        }
    }

    async fn call(&self, _args: Self::Args) -> Result<Self::Output, Self::Error> {
        Ok(self.previous_ddl.as_ref().clone())
    }
}

// ── submit_migration ─────────────────────────────────────────────────

/// A row of column values keyed by column name, as SQL literal values.
///
/// Values must be valid SQL literal expressions (e.g. `1`, `"hello"`,
/// `true`, `null`).
pub type Row = HashMap<String, serde_json::Value>;

/// Seed rows for a single table, plus the expected state after each migration direction.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct TableSeedData {
    /// Rows to INSERT before applying the UP migration.
    /// Each row is a mapping of column_name to a SQL literal value.
    pub rows: Vec<Row>,
    /// Expected rows after the UP migration is applied.
    /// Use this to reflect column additions (with default values),
    /// column removals, renames, or type changes.
    pub expected_after_up: Vec<Row>,
    /// Expected rows after the DOWN migration restores the previous state.
    /// Typically identical to `rows`.
    pub expected_after_down: Vec<Row>,
}

/// The structured migration output submitted by the LLM via tool call.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct MigrationOutput {
    /// SQL statements for the up (apply) migration.
    pub up_sql: String,
    /// SQL statements for the down (rollback) migration.
    pub down_sql: String,
    /// Short description for the migration filename (snake_case, no spaces).
    pub description: String,
    /// Seed data for verifying data preservation during migration.
    /// Keys are table names. Provide at least 2 rows per table that exists
    /// in the previous schema. Insert order must respect foreign key
    /// dependencies (parent tables first).
    #[serde(default)]
    pub seed_data: HashMap<String, TableSeedData>,
}

/// Shared slot where the submit_migration tool deposits its result.
pub type MigrationSlot = Arc<Mutex<Option<MigrationOutput>>>;

/// Tool the LLM calls to submit its migration result.
///
/// This is the *only* way the agent should return its output.
/// The result is stashed in a shared slot that the orchestrator reads.
pub struct SubmitMigration {
    pub slot: MigrationSlot,
    /// Number of tables in the previous schema that require seed data.
    pub expected_table_count: usize,
}

impl Tool for SubmitMigration {
    const NAME: &'static str = "submit_migration";
    type Error = ToolError;
    type Args = MigrationOutput;
    type Output = String;

    async fn definition(&self, _prompt: String) -> ToolDefinition {
        ToolDefinition {
            name: "submit_migration".into(),
            description: "Submit the generated migration. You MUST call this tool to deliver \
                          your result. Do not respond with JSON in your message — call this tool \
                          with up_sql, down_sql, description, and seed_data."
                .into(),
            parameters: serde_json::to_value(schemars::schema_for!(MigrationOutput))
                .expect("schema serialization should not fail"),
        }
    }

    async fn call(&self, args: Self::Args) -> Result<Self::Output, Self::Error> {
        if self.expected_table_count > 0 && args.seed_data.len() < self.expected_table_count {
            return Err(ToolError(format!(
                "seed_data has {} tables but the previous schema has {}. \
                 Provide seed data for ALL tables in the previous schema, \
                 then call submit_migration again.",
                args.seed_data.len(),
                self.expected_table_count
            )));
        }
        let mut slot = self.slot.lock().map_err(|e| ToolError(format!("lock poisoned: {e}")))?;
        if slot.is_some() {
            return Err(ToolError("Migration already submitted. Stop.".into()));
        }
        *slot = Some(args);
        Ok("Migration submitted. Task complete, stop.".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_migration_output_with_seed_data() {
        let json = serde_json::json!({
            "up_sql": "ALTER TABLE users ADD COLUMN email TEXT NOT NULL DEFAULT '';",
            "down_sql": "ALTER TABLE users DROP COLUMN email;",
            "description": "add_email_to_users",
            "seed_data": {
                "users": {
                    "rows": [
                        {"id": 1, "name": "Alice"},
                        {"id": 2, "name": "Bob"}
                    ],
                    "expected_after_up": [
                        {"id": 1, "name": "Alice", "email": ""},
                        {"id": 2, "name": "Bob", "email": ""}
                    ],
                    "expected_after_down": [
                        {"id": 1, "name": "Alice"},
                        {"id": 2, "name": "Bob"}
                    ]
                }
            }
        });

        let output: MigrationOutput = serde_json::from_value(json).expect("deserialization should succeed");
        assert_eq!(output.seed_data.len(), 1);
        let users = &output.seed_data["users"];
        assert_eq!(users.rows.len(), 2);
        assert_eq!(users.expected_after_up.len(), 2);
        assert_eq!(users.expected_after_down.len(), 2);
        assert_eq!(
            users.expected_after_up[0]["email"],
            serde_json::Value::String(String::new())
        );
    }

    #[test]
    fn test_migration_output_without_seed_data() {
        let json = serde_json::json!({
            "up_sql": "CREATE TABLE t (id INT);",
            "down_sql": "DROP TABLE t;",
            "description": "create_t"
        });

        let output: MigrationOutput = serde_json::from_value(json).expect("deserialization should succeed");
        assert!(output.seed_data.is_empty());
    }

    #[test]
    fn test_migration_output_with_fk_dependencies() {
        let json = serde_json::json!({
            "up_sql": "ALTER TABLE groups ADD COLUMN perms INTEGER NOT NULL DEFAULT 0;",
            "down_sql": "ALTER TABLE groups DROP COLUMN perms;",
            "description": "add_perms",
            "seed_data": {
                "users": {
                    "rows": [
                        {"id": 1, "name": "Alice", "created_at": "2024-01-01"}
                    ],
                    "expected_after_up": [
                        {"id": 1, "name": "Alice", "created_at": "2024-01-01"}
                    ],
                    "expected_after_down": [
                        {"id": 1, "name": "Alice", "created_at": "2024-01-01"}
                    ]
                },
                "groups": {
                    "rows": [
                        {"id": 1, "name": "admins"}
                    ],
                    "expected_after_up": [
                        {"id": 1, "name": "admins", "perms": 0}
                    ],
                    "expected_after_down": [
                        {"id": 1, "name": "admins"}
                    ]
                },
                "groups_users": {
                    "rows": [
                        {"group_id": 1, "user_id": 1}
                    ],
                    "expected_after_up": [
                        {"group_id": 1, "user_id": 1}
                    ],
                    "expected_after_down": [
                        {"group_id": 1, "user_id": 1}
                    ]
                }
            }
        });

        let output: MigrationOutput = serde_json::from_value(json).expect("deserialization should succeed");
        assert_eq!(output.seed_data.len(), 3);
    }

    #[test]
    fn test_seed_data_with_null_and_bool_values() {
        let json = serde_json::json!({
            "up_sql": "SELECT 1;",
            "down_sql": "SELECT 1;",
            "description": "test",
            "seed_data": {
                "settings": {
                    "rows": [
                        {"id": 1, "enabled": true, "description": null, "count": 42}
                    ],
                    "expected_after_up": [
                        {"id": 1, "enabled": true, "description": null, "count": 42}
                    ],
                    "expected_after_down": [
                        {"id": 1, "enabled": true, "description": null, "count": 42}
                    ]
                }
            }
        });

        let output: MigrationOutput = serde_json::from_value(json).expect("deserialization should succeed");
        let row = &output.seed_data["settings"].rows[0];
        assert_eq!(row["enabled"], serde_json::Value::Bool(true));
        assert_eq!(row["description"], serde_json::Value::Null);
        assert_eq!(row["count"], serde_json::json!(42));
    }
}
