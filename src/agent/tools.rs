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

/// The structured migration output submitted by the LLM via tool call.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct MigrationOutput {
    /// SQL statements for the up (apply) migration.
    pub up_sql: String,
    /// SQL statements for the down (rollback) migration.
    pub down_sql: String,
    /// Short description for the migration filename (snake_case, no spaces).
    pub description: String,
}

/// Shared slot where the submit_migration tool deposits its result.
pub type MigrationSlot = Arc<Mutex<Option<MigrationOutput>>>;

/// Tool the LLM calls to submit its migration result.
///
/// This is the *only* way the agent should return its output.
/// The result is stashed in a shared slot that the orchestrator reads.
pub struct SubmitMigration {
    pub slot: MigrationSlot,
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
                          with up_sql, down_sql, and description."
                .into(),
            parameters: serde_json::to_value(schemars::schema_for!(MigrationOutput))
                .expect("schema serialization should not fail"),
        }
    }

    async fn call(&self, args: Self::Args) -> Result<Self::Output, Self::Error> {
        let mut slot = self.slot.lock().map_err(|e| ToolError(format!("lock poisoned: {e}")))?;
        if slot.is_some() {
            return Err(ToolError("Migration already submitted. Stop.".into()));
        }
        *slot = Some(args);
        Ok("Migration submitted. Task complete, stop.".into())
    }
}
