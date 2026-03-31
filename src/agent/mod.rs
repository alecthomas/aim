pub mod prompt;
pub mod tools;

use std::fmt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use rig::client::{CompletionClient, ProviderClient};
use rig::completion::Prompt;

use crate::config::ModelSpec;
use crate::engine::{self, DatabaseEngine};
use crate::migration::Migration;
use crate::output::Output;

use tools::MigrationOutput;

/// Errors from the agent loop.
#[derive(Debug)]
pub enum Error {
    /// Schema already matches — no migration needed.
    NoChanges,
    /// LLM API or response parsing error.
    Llm(String),
    /// Verification failed after exhausting all retries.
    VerificationFailed {
        attempts: usize,
        last_up_diff: String,
        last_down_diff: String,
    },
    /// Database engine error during verification.
    Engine(crate::engine::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::NoChanges => write!(f, "no changes to migrate"),
            Error::Llm(msg) => write!(f, "LLM error: {msg}"),
            Error::VerificationFailed {
                attempts,
                last_up_diff,
                last_down_diff,
            } => {
                write!(
                    f,
                    "verification failed after {attempts} attempts\n\
                     up diff:\n{last_up_diff}\n\
                     down diff:\n{last_down_diff}"
                )
            }
            Error::Engine(err) => write!(f, "engine error during verification: {err}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<crate::engine::Error> for Error {
    fn from(err: crate::engine::Error) -> Self {
        Error::Engine(err)
    }
}

/// Result of a successful migration generation and verification.
#[derive(Debug)]
pub struct MigrationResult {
    pub migration: Migration,
}

/// Orchestrates the LLM agent loop: generate candidate migrations,
/// verify them against ephemeral databases, retry with diff feedback.
pub struct AgentLoop<'a> {
    engine: &'a dyn DatabaseEngine,
    schema_path: PathBuf,
    model: ModelSpec,
    max_retries: usize,
}

impl<'a> AgentLoop<'a> {
    pub fn new(engine: &'a dyn DatabaseEngine, schema_path: PathBuf, model: ModelSpec, max_retries: usize) -> Self {
        Self {
            engine,
            schema_path,
            model,
            max_retries,
        }
    }

    /// Run the agent loop: generate, verify, retry, return result.
    ///
    /// `prior_migrations` are the existing migrations that define the previous state.
    /// `next_sequence` is the sequence number for the new migration.
    pub async fn run(&self, prior_migrations: &[Migration], next_sequence: u64) -> Result<MigrationResult, Error> {
        // Dispatch to the correct provider. Each provider has a different
        // concrete Client type, so we use a macro to avoid duplication.
        macro_rules! run_with_provider {
            ($provider_mod:path) => {{
                use $provider_mod as provider;
                // Suppress the default panic hook output so we can
                // report the error cleanly.
                let prev_hook = std::panic::take_hook();
                std::panic::set_hook(Box::new(|_| {}));
                let result = std::panic::catch_unwind(provider::Client::from_env);
                std::panic::set_hook(prev_hook);
                let client = result.map_err(|e| {
                    let msg = e
                        .downcast_ref::<String>()
                        .map(|s| s.as_str())
                        .or_else(|| e.downcast_ref::<&str>().copied())
                        .unwrap_or("unknown error");
                    Error::Llm(format!("failed to create {} client: {msg}", self.model.provider))
                })?;
                self.run_with_client(&client, prior_migrations, next_sequence)
                    .await
            }};
        }

        match self.model.provider {
            "anthropic" => run_with_provider!(rig::providers::anthropic),
            "openai" => run_with_provider!(rig::providers::openai),
            "cohere" => run_with_provider!(rig::providers::cohere),
            "deepseek" => run_with_provider!(rig::providers::deepseek),
            "gemini" => run_with_provider!(rig::providers::gemini),
            "groq" => run_with_provider!(rig::providers::groq),
            "mistral" => run_with_provider!(rig::providers::mistral),
            "openrouter" => run_with_provider!(rig::providers::openrouter),
            "together" => run_with_provider!(rig::providers::together),
            "xai" => run_with_provider!(rig::providers::xai),
            "ollama" => run_with_provider!(rig::providers::ollama),
            "perplexity" => run_with_provider!(rig::providers::perplexity),
            other => Err(Error::Llm(format!("unsupported provider: {other}"))),
        }
    }

    /// Inner implementation that works with any provider client.
    async fn run_with_client<C>(
        &self,
        client: &C,
        prior_migrations: &[Migration],
        next_sequence: u64,
    ) -> Result<MigrationResult, Error>
    where
        C: CompletionClient,
        C::CompletionModel: rig::completion::CompletionModel,
    {
        let previous_ddl = Arc::new(self.build_previous_ddl(prior_migrations)?);
        let desired_ddl = Arc::new(self.build_desired_ddl()?);

        // Check for no-op: if schemas already match, nothing to do.
        if *previous_ddl == *desired_ddl {
            Output::success("schema.sql matches current state, nothing to migrate");
            return Err(Error::NoChanges);
        }

        let preamble = prompt::system_prompt(self.engine.dialect_description());

        // Shared slot where the submit_migration tool deposits its result.
        let slot: tools::MigrationSlot = Arc::new(Mutex::new(None));

        Output::phase("Generating migration...");

        let agent = client
            .agent(&self.model.model)
            .preamble(&preamble)
            .max_tokens(4096)
            .default_max_turns(10)
            .hook(Output)
            .tool(tools::ReadPreviousSchema {
                previous_ddl: previous_ddl.clone(),
            })
            .tool(tools::ReadSchema {
                desired_ddl: desired_ddl.clone(),
            })
            .tool(tools::SubmitMigration { slot: slot.clone() })
            .build();

        let initial_prompt = "Generate the migration. Use the tools to read the schema and \
             existing migrations, then call the submit_migration tool with your result.";

        // First attempt.
        prompt_agent(&agent, initial_prompt, &slot).await?;
        let mut candidate = take_slot(&slot)?;

        // Verify + retry loop.
        for attempt in 1..=self.max_retries + 1 {
            println!();
            Output::phase("Verifying migration...");

            // Verification can fail with an engine error (e.g. invalid SQL).
            // Treat that as a retryable failure, not a fatal error.
            let (up_diff, down_diff) = match self.verify(&candidate, prior_migrations) {
                Ok(result) => result,
                Err(Error::Engine(e)) => {
                    let msg = format!("execution error: {e}");
                    Output::error(&msg);
                    (msg.clone(), msg)
                }
                Err(e) => return Err(e),
            };

            if up_diff.is_empty() {
                Output::success("up migration verified");
            }
            if down_diff.is_empty() {
                Output::success("down migration verified");
            }

            if up_diff.is_empty() && down_diff.is_empty() {
                let migration = Migration {
                    sequence: next_sequence,
                    description: candidate.description,
                    up_sql: candidate.up_sql,
                    down_sql: candidate.down_sql,
                };
                return Ok(MigrationResult { migration });
            }

            Output::diff("up migration does not produce identical schema", &up_diff);
            Output::diff("down migration does not restore previous schema", &down_diff);

            if attempt > self.max_retries {
                Output::error("verification failed after all retries");
                return Err(Error::VerificationFailed {
                    attempts: attempt,
                    last_up_diff: up_diff,
                    last_down_diff: down_diff,
                });
            }

            Output::retry(attempt, self.max_retries);

            // Retry: include diff feedback in a new prompt.
            let retry_prompt = prompt::retry_message(&up_diff, &down_diff);
            prompt_agent(&agent, &retry_prompt, &slot).await?;
            candidate = take_slot(&slot)?;
        }

        unreachable!("loop always returns or errors")
    }

    /// Build the desired DDL by loading schema.sql into an ephemeral DB
    /// and reading back the normalized schema.
    fn build_desired_ddl(&self) -> Result<String, Error> {
        let schema_sql =
            std::fs::read_to_string(&self.schema_path).map_err(|e| Error::Llm(format!("reading schema.sql: {e}")))?;
        if schema_sql.trim().is_empty() {
            return Ok("-- empty schema".into());
        }
        let db = self.engine.create_ephemeral()?;
        self.engine.execute(&db, &schema_sql)?;
        let ddl = self.engine.dump_schema(&db)?;
        self.engine.drop_ephemeral(db)?;
        Ok(ddl)
    }

    /// Build the previous DDL by replaying migrations into an ephemeral DB
    /// and reading back the normalized schema. This ensures the LLM sees the
    /// same schema representation that the diff comparison uses.
    fn build_previous_ddl(&self, migrations: &[Migration]) -> Result<String, Error> {
        if migrations.is_empty() {
            return Ok("-- empty schema (no prior migrations)".into());
        }
        let db = self.engine.create_ephemeral()?;
        for m in migrations {
            self.engine.execute(&db, &m.up_sql)?;
        }
        let ddl = self.engine.dump_schema(&db)?;
        self.engine.drop_ephemeral(db)?;
        Ok(ddl)
    }

    /// Verify a candidate migration against ephemeral databases.
    ///
    /// Returns (up_diff, down_diff) where empty strings mean success.
    fn verify(&self, candidate: &MigrationOutput, prior_migrations: &[Migration]) -> Result<(String, String), Error> {
        // DB-Left: run schema.sql directly (desired end state).
        let db_left = self.engine.create_ephemeral()?;
        let schema_sql =
            std::fs::read_to_string(&self.schema_path).map_err(|e| Error::Llm(format!("reading schema.sql: {e}")))?;
        self.engine.execute(&db_left, &schema_sql)?;

        // DB-Right: replay prior migrations, then apply candidate up.
        let db_right = self.engine.create_ephemeral()?;
        for m in prior_migrations {
            self.engine.execute(&db_right, &m.up_sql)?;
        }
        self.engine.execute(&db_right, &candidate.up_sql)?;

        // Compare up migration result.
        let desired = self.engine.dump_schema(&db_left)?;
        let after_up = self.engine.dump_schema(&db_right)?;
        let dialect = self.engine.dialect();
        let up_diff = engine::schema_diff(dialect.as_ref(), &desired, "schema.sql", &after_up, "migration result");

        // Verify down: apply down to db_right, compare with previous state.
        self.engine.execute(&db_right, &candidate.down_sql)?;

        let db_prev = self.engine.create_ephemeral()?;
        for m in prior_migrations {
            self.engine.execute(&db_prev, &m.up_sql)?;
        }

        let prev_schema = self.engine.dump_schema(&db_prev)?;
        let after_down = self.engine.dump_schema(&db_right)?;
        let down_diff = engine::schema_diff(dialect.as_ref(), &prev_schema, "previous state", &after_down, "after rollback");

        // Clean up.
        self.engine.drop_ephemeral(db_left)?;
        self.engine.drop_ephemeral(db_right)?;
        self.engine.drop_ephemeral(db_prev)?;

        Ok((up_diff, down_diff))
    }
}

/// Extract the migration output from the shared slot, clearing it for reuse.
fn take_slot(slot: &tools::MigrationSlot) -> Result<MigrationOutput, Error> {
    let mut guard = slot
        .lock()
        .map_err(|e| Error::Llm(format!("slot lock poisoned: {e}")))?;
    guard
        .take()
        .ok_or_else(|| Error::Llm("LLM did not call submit_migration tool".into()))
}

/// Prompt the agent and handle providers that return empty responses after
/// tool calls (e.g. Gemini). If the LLM already called submit_migration
/// before the error, we have what we need and can ignore the error.
async fn prompt_agent(agent: &impl Prompt, prompt: &str, slot: &tools::MigrationSlot) -> Result<(), Error> {
    let result: Result<String, _> = agent.prompt(prompt).await;
    if let Err(e) = result {
        // Check if the tool was called before the error.
        let has_result = slot.lock().map(|s| s.is_some()).unwrap_or(false);
        if !has_result {
            return Err(Error::Llm(format!("{e}")));
        }
    }
    Ok(())
}
