pub mod prompt;
pub mod tools;

use std::fmt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use rig::agent::Agent;
use rig::client::{CompletionClient, ProviderClient};
use rig::completion::{CompletionModel, Prompt};
use rig::message::Message;

use crate::auth;
use crate::config::ModelSpec;
use crate::engine::{self, DatabaseEngine};
use crate::migration::Migration;
use crate::output::{Output, Spinner};
use crate::schema;

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
    max_tokens: u64,
    context: Option<String>,
}

impl<'a> AgentLoop<'a> {
    pub fn new(
        engine: &'a dyn DatabaseEngine,
        schema_path: PathBuf,
        model: ModelSpec,
        max_retries: usize,
        max_tokens: u64,
        context: Option<String>,
    ) -> Self {
        Self {
            engine,
            schema_path,
            model,
            max_retries,
            max_tokens,
            context,
        }
    }

    /// Run the agent loop: generate, verify, retry, return result.
    ///
    /// `prior_migrations` are the existing migrations that define the previous state.
    /// `next_sequence` is the sequence number for the new migration.
    pub async fn run(&self, prior_migrations: &[Migration], next_sequence: u64) -> Result<MigrationResult, Error> {
        // Ollama runs locally and doesn't need an API key.
        let api_key = if self.model.provider == "ollama" {
            None
        } else {
            Some(auth::resolve_api_key(self.model.provider).ok_or_else(|| {
                let hint = auth::provider_info(self.model.provider)
                    .map(|info| format!(" (set {} or run `aim auth`)", info.env_var))
                    .unwrap_or_default();
                Error::Llm(format!("no API key found for {}{hint}", self.model.provider))
            })?)
        };

        // Dispatch to the correct provider. Each provider has a different
        // concrete Client type, so we use a macro to avoid duplication.
        // The `.into()` call converts String to the provider-specific key
        // type (e.g. BearerAuth, GeminiApiKey) — all implement From<String>.
        macro_rules! run_with_provider {
            ($provider_mod:path, $key:expr) => {{
                use $provider_mod as provider;
                // Suppress the default panic hook output so we can
                // report the error cleanly.
                let prev_hook = std::panic::take_hook();
                std::panic::set_hook(Box::new(|_| {}));
                let result = std::panic::catch_unwind(|| provider::Client::from_val($key));
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

        // Unwrap is safe: we checked above that api_key is Some for all
        // non-ollama providers. The `.into()` is a no-op for providers
        // whose Input type is String, but required for others.
        #[allow(clippy::useless_conversion)]
        match self.model.provider {
            "anthropic" => run_with_provider!(rig::providers::anthropic, api_key.unwrap().into()),
            "openai" => run_with_provider!(rig::providers::openai, api_key.unwrap().into()),
            "cohere" => run_with_provider!(rig::providers::cohere, api_key.unwrap().into()),
            "deepseek" => run_with_provider!(rig::providers::deepseek, api_key.unwrap().into()),
            "gemini" => run_with_provider!(rig::providers::gemini, api_key.unwrap().into()),
            "groq" => run_with_provider!(rig::providers::groq, api_key.unwrap().into()),
            "mistral" => run_with_provider!(rig::providers::mistral, api_key.unwrap().into()),
            "openrouter" => run_with_provider!(rig::providers::openrouter, api_key.unwrap().into()),
            "together" => run_with_provider!(rig::providers::together, api_key.unwrap().into()),
            "xai" => run_with_provider!(rig::providers::xai, api_key.unwrap().into()),
            "ollama" => run_with_provider!(rig::providers::ollama, rig::client::Nothing),
            "perplexity" => run_with_provider!(rig::providers::perplexity, api_key.unwrap().into()),
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

        let preamble = prompt::system_prompt(self.engine.dialect_description(), self.context.as_deref());

        // Shared slot where the submit_migration tool deposits its result.
        let slot: tools::MigrationSlot = Arc::new(Mutex::new(None));

        let dialect = self.engine.dialect();
        let expected_table_count = schema::table_names(dialect.as_ref(), &previous_ddl).len();

        Output::phase("Generating migration...");

        let agent = client
            .agent(&self.model.model)
            .preamble(&preamble)
            .max_tokens(self.max_tokens)
            .default_max_turns(10)
            .hook(Output)
            .tool(tools::ReadPreviousSchema {
                previous_ddl: previous_ddl.clone(),
            })
            .tool(tools::ReadSchema {
                desired_ddl: desired_ddl.clone(),
            })
            .tool(tools::SubmitMigration {
                slot: slot.clone(),
                expected_table_count,
            })
            .build();

        let initial_prompt = "Generate the migration. Use the tools to read the schema and \
             existing migrations, then call the submit_migration tool with your result.";

        // Chat history persists across retries so the LLM can see its
        // prior attempts, the schemas it read, and the error feedback.
        let mut history: Vec<Message> = Vec::new();

        // First attempt.
        prompt_agent(&agent, initial_prompt, &mut history, &slot, self.max_tokens).await?;
        let mut candidate = take_slot(&slot)?;

        // Verify + retry loop.
        for attempt in 1..=self.max_retries + 1 {
            println!();
            Output::phase("Verifying migration...");

            let seed_issues = validate_seed_coverage(dialect.as_ref(), &previous_ddl, &candidate);
            if !seed_issues.is_empty() {
                let msg = format!(
                    "Seed data validation failed:\n{}",
                    seed_issues
                        .iter()
                        .map(|i| format!("- {i}"))
                        .collect::<Vec<_>>()
                        .join("\n")
                );
                Output::error(&msg);

                if attempt > self.max_retries {
                    Output::error("verification failed after all retries");
                    return Err(Error::VerificationFailed {
                        attempts: attempt,
                        last_up_diff: msg.clone(),
                        last_down_diff: msg,
                    });
                }

                Output::retry(attempt, self.max_retries);
                let retry_prompt = format!(
                    "Your seed data is incomplete or invalid:\n{msg}\n\n\
                     Fix the seed_data and call `submit_migration` again."
                );
                prompt_agent(&agent, &retry_prompt, &mut history, &slot, self.max_tokens).await?;
                candidate = take_slot(&slot)?;
                continue;
            }
            Output::success("seed data covers all tables");

            // Verification can fail with an engine error (e.g. invalid SQL).
            // Treat that as a retryable failure, not a fatal error.
            let (up_diff, down_diff) = match self.verify(&candidate, prior_migrations) {
                Ok(result) => result,
                Err(Error::Engine(e)) => {
                    let msg = format!("{e}");
                    Output::error(&msg);

                    if attempt > self.max_retries {
                        Output::error("verification failed after all retries");
                        return Err(Error::VerificationFailed {
                            attempts: attempt,
                            last_up_diff: msg.clone(),
                            last_down_diff: msg,
                        });
                    }

                    Output::retry(attempt, self.max_retries);
                    let retry_prompt = format!(
                        "Your migration SQL failed during verification.\n\n\
                         ## Error\n```\n{msg}\n```\n\n\
                         ## Your UP SQL\n```sql\n{}\n```\n\n\
                         ## Your DOWN SQL\n```sql\n{}\n```\n\n\
                         Fix the SQL and call `submit_migration` again.",
                        candidate.up_sql, candidate.down_sql
                    );
                    prompt_agent(&agent, &retry_prompt, &mut history, &slot, self.max_tokens).await?;
                    candidate = take_slot(&slot)?;
                    continue;
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
            let retry_prompt = prompt::retry_message(&up_diff, &down_diff, &candidate.up_sql, &candidate.down_sql);
            prompt_agent(&agent, &retry_prompt, &mut history, &slot, self.max_tokens).await?;
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
        let down_diff = engine::schema_diff(
            dialect.as_ref(),
            &prev_schema,
            "previous state",
            &after_down,
            "after rollback",
        );

        // Clean up.
        self.engine.drop_ephemeral(db_left)?;
        self.engine.drop_ephemeral(db_right)?;
        self.engine.drop_ephemeral(db_prev)?;

        Ok((up_diff, down_diff))
    }
}

/// Validate that seed data covers every table in the previous schema.
///
/// Returns a list of issues, or an empty vec if all tables are covered
/// with valid seed data. Checks that:
/// - every table in the previous DDL has a `seed_data` entry
/// - each entry has at least 2 rows
/// - `expected_after_up` and `expected_after_down` have matching row counts
fn validate_seed_coverage(
    dialect: &dyn sqlparser::dialect::Dialect,
    previous_ddl: &str,
    candidate: &MigrationOutput,
) -> Vec<String> {
    let tables = schema::table_names(dialect, previous_ddl);
    let mut issues = Vec::new();

    for table in &tables {
        match candidate.seed_data.get(table) {
            None => issues.push(format!("missing seed data for table `{table}`")),
            Some(seed) => {
                let row_count = seed.rows.len();
                if row_count < 2 {
                    issues.push(format!("table `{table}`: need at least 2 seed rows, got {row_count}"));
                }
                if seed.expected_after_up.len() != row_count {
                    issues.push(format!(
                        "table `{table}`: expected_after_up has {} rows but rows has {row_count}",
                        seed.expected_after_up.len()
                    ));
                }
                if seed.expected_after_down.len() != row_count {
                    issues.push(format!(
                        "table `{table}`: expected_after_down has {} rows but rows has {row_count}",
                        seed.expected_after_down.len()
                    ));
                }
            }
        }
    }

    issues
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

/// Prompt the agent, preserving conversation history across calls.
///
/// Uses `.with_history()` so the LLM sees prior tool calls, schemas,
/// and submitted migrations when retrying. Also handles providers that
/// return empty responses after tool calls (e.g. Gemini).
async fn prompt_agent<M: CompletionModel>(
    agent: &Agent<M, Output>,
    prompt: &str,
    history: &mut Vec<Message>,
    slot: &tools::MigrationSlot,
    max_tokens: u64,
) -> Result<(), Error> {
    if !history.is_empty() {
        Output::history_size(history);
    }
    let spinner = Spinner::start();
    let result: Result<String, _> = agent.prompt(prompt).with_history(history).await;
    spinner.stop();
    match result {
        Ok(_) => {
            // If the LLM responded with text but never called submit_migration,
            // check if this is possibly a truncation issue (handled by take_slot later).
            Ok(())
        }
        Err(e) => {
            // Check if the tool was called before the error.
            let has_result = slot.lock().map(|s| s.is_some()).unwrap_or(false);
            if has_result {
                return Ok(());
            }

            let msg = format!("{e}");
            if msg.contains("missing field") && msg.contains("JsonError") {
                return Err(Error::Llm(format!(
                    "LLM output was truncated (max_tokens = {max_tokens}). \
                     Increase max_tokens in aim.toml or pass --max-tokens on the CLI."
                )));
            }
            Err(Error::Llm(msg))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use tools::TableSeedData;

    fn two_rows() -> Vec<tools::Row> {
        vec![
            HashMap::from([
                ("id".into(), serde_json::json!(1)),
                ("name".into(), serde_json::json!("a")),
            ]),
            HashMap::from([
                ("id".into(), serde_json::json!(2)),
                ("name".into(), serde_json::json!("b")),
            ]),
        ]
    }

    fn make_seed(rows: Vec<tools::Row>) -> TableSeedData {
        TableSeedData {
            expected_after_up: rows.clone(),
            expected_after_down: rows.clone(),
            rows,
        }
    }

    fn make_candidate(seed_data: HashMap<String, TableSeedData>) -> MigrationOutput {
        MigrationOutput {
            up_sql: String::new(),
            down_sql: String::new(),
            description: "test".into(),
            seed_data,
        }
    }

    #[test]
    fn test_validate_seed_coverage_all_tables_covered() {
        let ddl = "CREATE TABLE users (id INTEGER, name TEXT);\n\nCREATE TABLE groups (id INTEGER)";
        let dialect = sqlparser::dialect::SQLiteDialect {};
        let candidate = make_candidate(HashMap::from([
            ("users".into(), make_seed(two_rows())),
            (
                "groups".into(),
                make_seed(vec![
                    HashMap::from([("id".into(), serde_json::json!(1))]),
                    HashMap::from([("id".into(), serde_json::json!(2))]),
                ]),
            ),
        ]));
        let issues = validate_seed_coverage(&dialect, ddl, &candidate);
        assert!(issues.is_empty(), "expected no issues, got: {issues:?}");
    }

    #[test]
    fn test_validate_seed_coverage_missing_table() {
        let ddl = "CREATE TABLE users (id INTEGER);\n\nCREATE TABLE groups (id INTEGER)";
        let dialect = sqlparser::dialect::SQLiteDialect {};
        let candidate = make_candidate(HashMap::from([("users".into(), make_seed(two_rows()))]));
        let issues = validate_seed_coverage(&dialect, ddl, &candidate);
        assert_eq!(issues.len(), 1);
        assert!(issues[0].contains("groups"), "should mention groups: {}", issues[0]);
    }

    #[test]
    fn test_validate_seed_coverage_too_few_rows() {
        let ddl = "CREATE TABLE users (id INTEGER)";
        let dialect = sqlparser::dialect::SQLiteDialect {};
        let one_row = vec![HashMap::from([("id".into(), serde_json::json!(1))])];
        let candidate = make_candidate(HashMap::from([("users".into(), make_seed(one_row))]));
        let issues = validate_seed_coverage(&dialect, ddl, &candidate);
        assert_eq!(issues.len(), 1);
        assert!(issues[0].contains("at least 2"), "{}", issues[0]);
    }

    #[test]
    fn test_validate_seed_coverage_row_count_mismatch() {
        let ddl = "CREATE TABLE users (id INTEGER)";
        let dialect = sqlparser::dialect::SQLiteDialect {};
        let candidate = make_candidate(HashMap::from([(
            "users".into(),
            TableSeedData {
                rows: two_rows(),
                expected_after_up: vec![HashMap::from([("id".into(), serde_json::json!(1))])],
                expected_after_down: two_rows(),
            },
        )]));
        let issues = validate_seed_coverage(&dialect, ddl, &candidate);
        assert_eq!(issues.len(), 1);
        assert!(issues[0].contains("expected_after_up"), "{}", issues[0]);
    }

    #[test]
    fn test_validate_seed_coverage_empty_ddl() {
        let dialect = sqlparser::dialect::SQLiteDialect {};
        let candidate = make_candidate(HashMap::new());
        let issues = validate_seed_coverage(&dialect, "", &candidate);
        assert!(issues.is_empty());
    }
}
