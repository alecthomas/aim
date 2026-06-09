pub mod prompt;
pub mod tools;

use std::collections::HashSet;
use std::fmt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use rig::agent::Agent;
use rig::client::{CompletionClient, ProviderClient};
use rig::completion::{CompletionModel, Prompt};
use rig::message::Message;
use sqlparser::ast::{SetExpr, Statement};
use sqlparser::dialect::Dialect;
use sqlparser::parser::Parser;

use crate::auth;
use crate::config::{Config, ModelSpec};
use crate::display;
use crate::engine::{self, DatabaseEngine};
use crate::migration::Migration;
use crate::output::{Output, Spinner};
use crate::schema;
use crate::seed;
use crate::validation::{self, RuleLevel, RuleVerdict, ValidationRule, VerdictStatus};

use tools::MigrationOutput;

/// Whether a provider authenticates without an aim-managed API key.
///
/// Ollama runs locally, and Bedrock relies on the AWS credential chain
/// (environment variables, shared config/profile, or IMDS).
fn provider_is_keyless(provider: &str) -> bool {
    matches!(provider, "ollama" | "bedrock")
}

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
    /// The up migration matched one or more error-level validation rules.
    ValidationFailed { violations: Vec<RuleViolation> },
}

/// A validation rule the up migration matched, with the validator's explanation.
#[derive(Debug, Clone)]
pub struct RuleViolation {
    /// Identifier of the matched rule.
    pub rule_id: String,
    /// Validator's explanation of what triggered the rule.
    pub detail: String,
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
            Error::ValidationFailed { violations } => {
                writeln!(f, "migration failed schema validation:")?;
                for v in violations {
                    writeln!(f, "  [{}] {}", v.rule_id, v.detail)?;
                }
                write!(
                    f,
                    "disable the offending rule(s) in aim.toml under [validation] if intended"
                )
            }
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
    pub seed_data: std::collections::HashMap<String, tools::TableSeedData>,
    /// Array-typed columns in the previous schema (for rendering seed inserts
    /// and `expected_after_down` checks).
    pub previous_array_columns: schema::ArrayColumns,
    /// Array-typed columns in the desired schema (for `expected_after_up` checks).
    pub desired_array_columns: schema::ArrayColumns,
}

/// Outcome of verifying a candidate migration against ephemeral databases.
///
/// Schema diffs and seed-data preservation issues are kept separate so the
/// agent loop can report them with distinct, accurate messaging.
#[derive(Debug, Default)]
struct VerifyOutcome {
    /// Schema diff after UP; empty means the resulting schema matched.
    up_diff: String,
    /// Schema diff after DOWN rollback; empty means it matched.
    down_diff: String,
    /// Seed-data mismatch detected after UP, if any.
    up_data_issue: Option<String>,
    /// Seed-data mismatch detected after DOWN, if any.
    down_data_issue: Option<String>,
}

impl VerifyOutcome {
    /// Whether the migration passed every schema and data-preservation check.
    fn is_clean(&self) -> bool {
        self.up_diff.is_empty()
            && self.down_diff.is_empty()
            && self.up_data_issue.is_none()
            && self.down_data_issue.is_none()
    }
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
    /// When `true`, no down (rollback) migration is generated or verified.
    no_down: bool,
    /// Schema-change validation rules the agent checks the up migration against.
    rules: Vec<ValidationRule>,
}

impl<'a> AgentLoop<'a> {
    /// Build an agent loop from resolved configuration and a target engine.
    ///
    /// `model` is passed separately because [`Config::model`] is optional, but
    /// the agent loop requires a concrete model resolved by the caller.
    pub fn new(engine: &'a dyn DatabaseEngine, config: &Config, model: ModelSpec) -> Self {
        Self {
            engine,
            schema_path: config.schema_path.clone(),
            model,
            max_retries: config.max_retries,
            max_tokens: config.max_tokens,
            context: config.context.clone(),
            no_down: config.no_down,
            rules: config.validation_rules.clone(),
        }
    }

    /// Run the agent loop: generate, verify, retry, return result.
    ///
    /// `prior_migrations` are the existing migrations that define the previous state.
    /// `next_sequence` is the sequence number for the new migration.
    pub async fn run(
        &self,
        prior_migrations: &[Migration],
        next_sequence: u64,
        schema_diff: &str,
    ) -> Result<MigrationResult, Error> {
        // Some providers don't use an aim-managed API key: Ollama runs
        // locally, and Bedrock authenticates via the AWS credential chain
        // (env vars, shared config/profile, or IMDS).
        let api_key = if provider_is_keyless(self.model.provider) {
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
                let client = provider::Client::from_val($key)
                    .map_err(|e| Error::Llm(format!("failed to create {} client: {e:?}", self.model.provider)))?;
                self.run_with_client(&client, prior_migrations, next_sequence, schema_diff)
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
            "ollama" => run_with_provider!(rig::providers::ollama, rig::client::Nothing.into()),
            "bedrock" => run_with_provider!(rig_bedrock::client, rig::client::Nothing.into()),
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
        schema_diff: &str,
    ) -> Result<MigrationResult, Error>
    where
        C: CompletionClient,
        C::CompletionModel: rig::completion::CompletionModel + 'static,
    {
        let previous_ddl = Arc::new(self.build_previous_ddl(prior_migrations)?);
        let desired_ddl = Arc::new(self.build_desired_ddl()?);

        // Check for no-op: if schemas already match, nothing to do.
        if *previous_ddl == *desired_ddl {
            Output::success("schema.sql matches current state, nothing to migrate");
            return Err(Error::NoChanges);
        }

        let preamble = prompt::system_prompt(self.engine.dialect_description(), self.context.as_deref(), self.no_down);

        // Shared slot where the submit_migration tool deposits its result.
        let slot: tools::MigrationSlot = Arc::new(Mutex::new(None));

        let dialect = self.engine.dialect();
        let previous_tables = schema::table_names(dialect.as_ref(), &previous_ddl);
        // Array-typed columns per schema, so seed values for them render as
        // PostgreSQL array literals instead of JSON.
        let previous_array_columns = schema::array_columns(dialect.as_ref(), &previous_ddl);
        let desired_array_columns = schema::array_columns(dialect.as_ref(), &desired_ddl);

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
                required_tables: previous_tables.clone(),
                no_down: self.no_down,
            })
            .build();

        // Spell out exactly which tables need seed_data so the LLM does not
        // have to infer the full set from the schema dump (a common cause of
        // rejected submissions).
        let seed_requirement = if previous_tables.is_empty() {
            "The previous schema has no tables, so seed_data must be empty.".to_string()
        } else {
            format!(
                "seed_data MUST contain an entry for EVERY one of these {} previous-schema \
                 tables, or the submission will be rejected: {}.",
                previous_tables.len(),
                previous_tables.join(", ")
            )
        };

        let initial_prompt = format!(
            "Generate the migration. Use the tools to read the schemas, then call \
             submit_migration with your result.\n\n\
             {seed_requirement}\n\n\
             Here is a summary of what changed between the previous and desired schemas:\n\
             ```\n{schema_diff}\n```"
        );

        // Chat history persists across retries so the LLM can see its
        // prior attempts, the schemas it read, and the error feedback.
        let mut history: Vec<Message> = Vec::new();

        // First attempt.
        prompt_agent(&agent, &initial_prompt, &mut history, &slot, self.max_tokens).await?;
        let mut candidate = take_slot(&slot)?;

        // Verify + retry loop.
        for attempt in 1..=self.max_retries + 1 {
            println!();
            Output::phase("Verifying migration...");

            let seed_issues = validate_seed_coverage(dialect.as_ref(), &previous_ddl, &candidate, self.no_down);
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

            // Reject migrations that embed literal/seed data. These run against
            // real production data, so only transforms deriving from existing
            // rows are allowed.
            let dml_issues = migration_literal_data_issues(dialect.as_ref(), &candidate);
            if !dml_issues.is_empty() {
                let msg = dml_issues
                    .iter()
                    .map(|i| format!("- {i}"))
                    .collect::<Vec<_>>()
                    .join("\n");
                Output::error(&format!("Migration contains disallowed literal data:\n{msg}"));

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
                    "Your migration includes literal data INSERTs, which are not allowed:\n{msg}\n\n\
                     Migrations must NEVER contain seed or sample data — they run against real \
                     production data, not the seed rows. Only transform EXISTING data using \
                     UPDATE/DELETE, or `INSERT ... SELECT ... FROM <existing table>` (e.g. copy \
                     rows when rebuilding a table). Remove the literal INSERTs and call \
                     `submit_migration` again."
                );
                prompt_agent(&agent, &retry_prompt, &mut history, &slot, self.max_tokens).await?;
                candidate = take_slot(&slot)?;
                continue;
            }
            Output::success("no literal seed data in migration");

            // Verification can fail with an engine error (e.g. invalid SQL).
            // Treat that as a retryable failure, not a fatal error.
            let outcome = match self.verify(
                &candidate,
                prior_migrations,
                &previous_array_columns,
                &desired_array_columns,
            ) {
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

            if outcome.up_diff.is_empty() {
                Output::success("up migration verified");
            }
            if !self.no_down && outcome.down_diff.is_empty() {
                Output::success("down migration verified");
            }

            // Report seed-data preservation separately from schema diffs.
            if let Some(msg) = &outcome.up_data_issue {
                Output::error(&format!("up migration did not preserve seed data: {msg}"));
            }
            if let Some(msg) = &outcome.down_data_issue {
                Output::error(&format!("down migration did not preserve seed data: {msg}"));
            }
            // Only claim preservation when there was seed data and both
            // directions actually ran their data checks (i.e. schemas matched).
            if !candidate.seed_data.is_empty() && outcome.is_clean() {
                let scope = if self.no_down {
                    "the up migration"
                } else {
                    "up and down migrations"
                };
                Output::success(&format!("seed data preserved across {scope}"));
            }

            if outcome.is_clean() {
                // Validate the verified up migration against each rule in
                // isolation. An error-level match aborts (no retry — the change
                // is intrinsic to the requested schema); warnings are advisory.
                if !self.rules.is_empty() {
                    println!();
                    Output::phase("Validating schema changes...");
                    let verdicts = self.validate_up_migration(client, &candidate.up_sql).await?;
                    let (errors, warnings) = classify_violations(&verdicts);
                    for w in &warnings {
                        Output::warn(&format!("[{}] {}", w.rule_id, w.detail));
                    }
                    if !errors.is_empty() {
                        // Show the offending migration first so the reported
                        // errors have context the user can read.
                        println!("\n-- UP --");
                        display::highlight_sql(&candidate.up_sql);
                        println!();
                        for e in &errors {
                            Output::error(&format!("[{}] {}", e.rule_id, e.detail));
                        }
                        return Err(Error::ValidationFailed { violations: errors });
                    }
                    Output::success("schema changes passed validation");
                }

                let migration = Migration {
                    sequence: next_sequence,
                    description: candidate.description,
                    up_sql: candidate.up_sql,
                    down_sql: candidate.down_sql,
                };
                return Ok(MigrationResult {
                    migration,
                    seed_data: candidate.seed_data,
                    previous_array_columns: previous_array_columns.clone(),
                    desired_array_columns: desired_array_columns.clone(),
                });
            }

            Output::diff("up migration does not produce identical schema", &outcome.up_diff);
            Output::diff("down migration does not restore previous schema", &outcome.down_diff);

            // Fold data-preservation issues into the diff feedback sent to the
            // LLM, so it sees both schema and data problems when retrying.
            let mut up_feedback = outcome.up_diff;
            if let Some(msg) = &outcome.up_data_issue {
                append_data_issue(&mut up_feedback, "expected_after_up", msg);
            }
            let mut down_feedback = outcome.down_diff;
            if let Some(msg) = &outcome.down_data_issue {
                append_data_issue(&mut down_feedback, "expected_after_down", msg);
            }

            if attempt > self.max_retries {
                Output::error("verification failed after all retries");
                return Err(Error::VerificationFailed {
                    attempts: attempt,
                    last_up_diff: up_feedback,
                    last_down_diff: down_feedback,
                });
            }

            Output::retry(attempt, self.max_retries);

            // Retry: include diff feedback in a new prompt.
            let retry_prompt =
                prompt::retry_message(&up_feedback, &down_feedback, &candidate.up_sql, &candidate.down_sql);
            prompt_agent(&agent, &retry_prompt, &mut history, &slot, self.max_tokens).await?;
            candidate = take_slot(&slot)?;
        }

        unreachable!("loop always returns or errors")
    }

    /// Validate the up migration against every enabled rule, in parallel.
    ///
    /// Each rule gets its own isolated [`rig::extractor::Extractor`] that is
    /// handed ONLY `up_sql`, so a validator cannot see (and wrongly flag) the
    /// down migration. Returns each rule paired with its verdict.
    async fn validate_up_migration<C>(
        &self,
        client: &C,
        up_sql: &str,
    ) -> Result<Vec<(ValidationRule, RuleVerdict)>, Error>
    where
        C: CompletionClient,
        C::CompletionModel: rig::completion::CompletionModel,
    {
        let dialect = self.engine.dialect_description();
        let checks = self.rules.iter().map(|rule| {
            let extractor = client
                .extractor::<RuleVerdict>(&self.model.model)
                .preamble(&validation::validator_preamble(dialect, rule))
                .max_tokens(self.max_tokens)
                .build();
            async move {
                extractor
                    .extract(up_sql)
                    .await
                    .map(|verdict| (rule.clone(), verdict))
                    .map_err(|e| Error::Llm(format!("validating rule '{}': {e}", rule.id)))
            }
        });
        futures::future::join_all(checks).await.into_iter().collect()
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
    /// Seed data is inserted before applying UP, so migrations that fail on
    /// existing rows (e.g. a NOT NULL add without a default) surface here.
    /// Data-preservation checks only run when the schema already matches, so
    /// they are not muddied by an otherwise-wrong schema.
    fn verify(
        &self,
        candidate: &MigrationOutput,
        prior_migrations: &[Migration],
        previous_array_columns: &schema::ArrayColumns,
        desired_array_columns: &schema::ArrayColumns,
    ) -> Result<VerifyOutcome, Error> {
        // DB-Left: run schema.sql directly (desired end state).
        let db_left = self.engine.create_ephemeral()?;
        let schema_sql =
            std::fs::read_to_string(&self.schema_path).map_err(|e| Error::Llm(format!("reading schema.sql: {e}")))?;
        self.engine.execute(&db_left, &schema_sql)?;

        // DB-Right: replay prior migrations, seed data, then apply candidate up.
        let db_right = self.engine.create_ephemeral()?;
        for m in prior_migrations {
            self.engine.execute(&db_right, &m.up_sql)?;
        }
        self.seed_database(&db_right, &candidate.seed_data, previous_array_columns)?;
        self.engine.execute_in_transaction(&db_right, &candidate.up_sql)?;

        // Compare up migration result.
        let desired = self.engine.dump_schema(&db_left)?;
        let after_up = self.engine.dump_schema(&db_right)?;
        let dialect = self.engine.dialect();
        let up_diff = engine::schema_diff(dialect.as_ref(), &desired, "schema.sql", &after_up, "migration result");

        // Data-preservation check: surviving tables must match expected_after_up.
        let no_exclusions = std::collections::HashMap::new();
        let up_data_issue = if up_diff.is_empty() {
            let surviving: HashSet<String> = schema::table_names(dialect.as_ref(), &after_up).into_iter().collect();
            let up_checks = seed::build_row_checks(
                &candidate.seed_data,
                seed::Direction::Up,
                desired_array_columns,
                &no_exclusions,
            );
            self.run_seed_checks(&db_right, &up_checks, &surviving)?
        } else {
            None
        };

        // Verify down: apply down to db_right, compare with previous state.
        // Skipped entirely when down migrations are disabled.
        let (down_diff, down_data_issue) = if self.no_down {
            (String::new(), None)
        } else {
            self.engine.execute_in_transaction(&db_right, &candidate.down_sql)?;

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

            // Data-preservation check: restored tables must match expected_after_down.
            // Columns dropped by UP are re-added by DOWN with their DEFAULT, not
            // their original values, so exclude them from the row predicates: the
            // data is irrecoverable and the count check still guards row totals.
            let down_data_issue = if down_diff.is_empty() {
                let restored: HashSet<String> =
                    schema::table_names(dialect.as_ref(), &after_down).into_iter().collect();
                let dropped = schema::dropped_columns(dialect.as_ref(), &prev_schema, &desired);
                let down_checks = seed::build_row_checks(
                    &candidate.seed_data,
                    seed::Direction::Down,
                    previous_array_columns,
                    &dropped,
                );
                self.run_seed_checks(&db_right, &down_checks, &restored)?
            } else {
                None
            };

            self.engine.drop_ephemeral(db_prev)?;
            (down_diff, down_data_issue)
        };

        // Clean up.
        self.engine.drop_ephemeral(db_left)?;
        self.engine.drop_ephemeral(db_right)?;

        Ok(VerifyOutcome {
            up_diff,
            down_diff,
            up_data_issue,
            down_data_issue,
        })
    }

    /// Insert all seed rows into `db`, disabling foreign-key enforcement so
    /// that insertion order across tables does not matter.
    fn seed_database(
        &self,
        db: &engine::EphemeralDb,
        seed_data: &std::collections::HashMap<String, tools::TableSeedData>,
        array_columns: &schema::ArrayColumns,
    ) -> Result<(), Error> {
        let inserts = seed::build_insert_statements(seed_data, array_columns);
        if inserts.is_empty() {
            return Ok(());
        }
        self.engine
            .execute(db, &format!("{}{inserts}", self.engine.fk_disable_prefix()))?;
        Ok(())
    }

    /// Run seed data-preservation checks, returning the first mismatch found.
    ///
    /// Checks whose table is absent from `present` (e.g. dropped by the
    /// migration) are skipped. Each check runs its `COUNT(*)` query and the
    /// result is compared against the expected total or existence condition.
    fn run_seed_checks(
        &self,
        db: &engine::EphemeralDb,
        checks: &[seed::RowCheck],
        present: &HashSet<String>,
    ) -> Result<Option<String>, Error> {
        for check in checks {
            if !present.contains(check.table()) {
                continue;
            }
            let actual = self.engine.count_query(db, check.count_sql())?;
            match check {
                seed::RowCheck::Total { table, count, .. } => {
                    if actual as usize != *count {
                        return Ok(Some(format!(
                            "table `{table}`: expected {count} row(s) but found {actual}"
                        )));
                    }
                }
                seed::RowCheck::Exists { table, expected, .. } => {
                    if actual == 0 {
                        return Ok(Some(format!("table `{table}`: expected row not found: {expected}")));
                    }
                }
            }
        }
        Ok(None)
    }
}

/// Validate that seed data covers every table in the previous schema.
///
/// Returns a list of issues, or an empty vec if all tables are covered
/// with valid seed data. Checks that:
/// - every table in the previous DDL has a `seed_data` entry
/// - each entry has at least 2 rows
/// - `expected_after_up` has a matching row count
/// - `expected_after_down` has a matching row count (skipped when `no_down`,
///   since no down migration is generated or verified)
fn validate_seed_coverage(
    dialect: &dyn sqlparser::dialect::Dialect,
    previous_ddl: &str,
    candidate: &MigrationOutput,
    no_down: bool,
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
                if !no_down && seed.expected_after_down.len() != row_count {
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

/// Split failing verdicts into error-level and warning-level violations by each
/// rule's configured level. Passing verdicts are ignored. Returns
/// `(errors, warnings)`.
fn classify_violations(verdicts: &[(ValidationRule, RuleVerdict)]) -> (Vec<RuleViolation>, Vec<RuleViolation>) {
    let mut errors = Vec::new();
    let mut warnings = Vec::new();
    for (rule, verdict) in verdicts {
        if verdict.status != VerdictStatus::Fail {
            continue;
        }
        let violation = RuleViolation {
            rule_id: rule.id.clone(),
            detail: verdict.detail.clone(),
        };
        match rule.level {
            RuleLevel::Error => errors.push(violation),
            RuleLevel::Warning => warnings.push(violation),
        }
    }
    (errors, warnings)
}

/// Detect disallowed literal-data INSERTs in a candidate migration.
///
/// Migrations must never embed seed/sample data: they run against real
/// production data, not the seed rows used for verification. Only transforms
/// that derive from existing rows are allowed (`UPDATE`/`DELETE`, or
/// `INSERT ... SELECT` reading FROM a table). This returns a message for each
/// `INSERT` whose source introduces literal rows.
fn migration_literal_data_issues(dialect: &dyn Dialect, candidate: &MigrationOutput) -> Vec<String> {
    let mut issues = Vec::new();
    for (label, sql) in [("up_sql", &candidate.up_sql), ("down_sql", &candidate.down_sql)] {
        for stmt in parse_statements_best_effort(dialect, sql) {
            let Statement::Insert(insert) = &stmt else {
                continue;
            };
            let introduces_literals = match &insert.source {
                Some(query) => source_introduces_literals(&query.body),
                // `INSERT ... DEFAULT VALUES` inserts a literal row.
                None => true,
            };
            if introduces_literals {
                issues.push(format!(
                    "{label} contains an INSERT with literal row values (`{}`); migrations must \
                     not insert seed/sample data",
                    one_line(&stmt.to_string())
                ));
            }
        }
    }
    issues
}

/// Whether an INSERT source introduces literal rows rather than deriving them
/// from existing tables. `VALUES` clauses and constant-only `SELECT`s (no
/// `FROM`) are literal; a `SELECT`/`TABLE` reading from a table is not.
fn source_introduces_literals(expr: &SetExpr) -> bool {
    match expr {
        SetExpr::Values(_) => true,
        SetExpr::Select(select) => select.from.is_empty(),
        SetExpr::Query(query) => source_introduces_literals(&query.body),
        SetExpr::SetOperation { left, right, .. } => {
            source_introduces_literals(left) || source_introduces_literals(right)
        }
        // `TABLE t`, or nested INSERT/UPDATE sources, read from existing data.
        _ => false,
    }
}

/// Parse SQL into statements, tolerating dialect quirks.
///
/// Tries to parse the whole input; on failure, parses statement-by-statement
/// (split on `;`) and keeps whatever parses, so an unparseable DDL statement
/// does not hide an analyzable INSERT elsewhere in the migration.
fn parse_statements_best_effort(dialect: &dyn Dialect, sql: &str) -> Vec<Statement> {
    if let Ok(statements) = Parser::parse_sql(dialect, sql) {
        return statements;
    }
    sql.split(';')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .filter_map(|s| Parser::parse_sql(dialect, s).ok())
        .flatten()
        .collect()
}

/// Collapse whitespace and truncate a SQL statement for error messages.
fn one_line(sql: &str) -> String {
    let collapsed = sql.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.len() > 80 {
        format!("{}...", &collapsed[..80])
    } else {
        collapsed
    }
}

/// Append a data-preservation mismatch to an existing schema diff string.
///
/// Mismatches are folded into the diff so the agent's existing retry loop
/// surfaces them to the LLM as feedback.
fn append_data_issue(diff: &mut String, field: &str, msg: &str) {
    if !diff.is_empty() {
        diff.push('\n');
    }
    diff.push_str(&format!("Data preservation check failed ({field}):\n- {msg}"));
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
async fn prompt_agent<M: CompletionModel + 'static>(
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
    // Pass the accumulated history as an explicit snapshot. `extended_details`
    // makes the response carry the new turn's messages (prompt, assistant
    // replies, and tool call/result pairs) so we can append them back to
    // `history` ourselves — rig no longer mutates the passed history in place.
    let result = agent
        .prompt(prompt)
        .with_history(history.clone())
        .extended_details()
        .await;
    spinner.stop();
    match result {
        Ok(response) => {
            // Accumulate this turn so subsequent retries see prior attempts,
            // the schemas the LLM read, and the error feedback.
            if let Some(messages) = response.messages {
                history.extend(messages);
            }
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
        let issues = validate_seed_coverage(&dialect, ddl, &candidate, false);
        assert!(issues.is_empty(), "expected no issues, got: {issues:?}");
    }

    #[test]
    fn test_validate_seed_coverage_missing_table() {
        let ddl = "CREATE TABLE users (id INTEGER);\n\nCREATE TABLE groups (id INTEGER)";
        let dialect = sqlparser::dialect::SQLiteDialect {};
        let candidate = make_candidate(HashMap::from([("users".into(), make_seed(two_rows()))]));
        let issues = validate_seed_coverage(&dialect, ddl, &candidate, false);
        assert_eq!(issues.len(), 1);
        assert!(issues[0].contains("groups"), "should mention groups: {}", issues[0]);
    }

    #[test]
    fn test_validate_seed_coverage_too_few_rows() {
        let ddl = "CREATE TABLE users (id INTEGER)";
        let dialect = sqlparser::dialect::SQLiteDialect {};
        let one_row = vec![HashMap::from([("id".into(), serde_json::json!(1))])];
        let candidate = make_candidate(HashMap::from([("users".into(), make_seed(one_row))]));
        let issues = validate_seed_coverage(&dialect, ddl, &candidate, false);
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
        let issues = validate_seed_coverage(&dialect, ddl, &candidate, false);
        assert_eq!(issues.len(), 1);
        assert!(issues[0].contains("expected_after_up"), "{}", issues[0]);
    }

    #[test]
    fn test_validate_seed_coverage_no_down_skips_expected_after_down() {
        let ddl = "CREATE TABLE users (id INTEGER)";
        let dialect = sqlparser::dialect::SQLiteDialect {};
        // expected_after_down is empty, as the model omits it when no_down is set.
        let candidate = make_candidate(HashMap::from([(
            "users".into(),
            TableSeedData {
                rows: two_rows(),
                expected_after_up: two_rows(),
                expected_after_down: Vec::new(),
            },
        )]));
        assert!(
            validate_seed_coverage(&dialect, ddl, &candidate, true).is_empty(),
            "no_down must skip the expected_after_down check"
        );
        assert_eq!(
            validate_seed_coverage(&dialect, ddl, &candidate, false).len(),
            1,
            "with down enabled the empty expected_after_down is an issue"
        );
    }

    #[test]
    fn test_validate_seed_coverage_empty_ddl() {
        let dialect = sqlparser::dialect::SQLiteDialect {};
        let candidate = make_candidate(HashMap::new());
        let issues = validate_seed_coverage(&dialect, "", &candidate, false);
        assert!(issues.is_empty());
    }

    /// Create a SQLite db, create the table, and insert the seed rows for it
    /// using the shared `seed` SQL builder. Returns the engine and db handle.
    fn seeded_sqlite(
        ddl: &str,
        seed_data: &HashMap<String, TableSeedData>,
    ) -> (crate::engine::sqlite::SqliteEngine, engine::EphemeralDb) {
        let engine = crate::engine::sqlite::SqliteEngine;
        let db = engine.create_ephemeral().expect("create");
        engine.execute(&db, ddl).expect("create table");
        let inserts = seed::build_insert_statements(seed_data, &schema::ArrayColumns::new());
        engine.execute(&db, &inserts).expect("insert seed rows");
        (engine, db)
    }

    /// Run all checks for a direction against a populated db, returning the
    /// first mismatch message (or None). Mirrors `AgentLoop::run_seed_checks`
    /// but without needing a full agent instance.
    fn run_checks(
        engine: &crate::engine::sqlite::SqliteEngine,
        db: &engine::EphemeralDb,
        seed_data: &HashMap<String, TableSeedData>,
        present: &[&str],
    ) -> Option<String> {
        let present: HashSet<String> = present.iter().map(|s| s.to_string()).collect();
        for check in seed::build_row_checks(
            seed_data,
            seed::Direction::Up,
            &schema::ArrayColumns::new(),
            &HashMap::new(),
        ) {
            if !present.contains(check.table()) {
                continue;
            }
            let actual = engine.count_query(db, check.count_sql()).expect("count query");
            match &check {
                seed::RowCheck::Total { count, .. } if actual as usize != *count => {
                    return Some(format!("total mismatch: expected {count} found {actual}"));
                }
                seed::RowCheck::Exists { expected, .. } if actual == 0 => {
                    return Some(format!("missing: {expected}"));
                }
                _ => {}
            }
        }
        None
    }

    fn seed_with_expected(rows: Vec<tools::Row>, expected_up: Vec<tools::Row>) -> HashMap<String, TableSeedData> {
        HashMap::from([(
            "users".to_string(),
            TableSeedData {
                expected_after_down: rows.clone(),
                rows,
                expected_after_up: expected_up,
            },
        )])
    }

    #[test]
    fn test_seed_checks_pass_when_data_preserved() {
        let data = seed_with_expected(two_rows(), two_rows());
        let (engine, db) = seeded_sqlite("CREATE TABLE users (id INTEGER, name TEXT);", &data);
        let result = run_checks(&engine, &db, &data, &["users"]);
        engine.drop_ephemeral(db).expect("drop");
        assert!(result.is_none(), "expected match, got: {result:?}");
    }

    #[test]
    fn test_seed_checks_detect_wrong_count() {
        let mut expected = two_rows();
        expected.push(HashMap::from([
            ("id".into(), serde_json::json!(3)),
            ("name".into(), serde_json::json!("c")),
        ]));
        let data = seed_with_expected(two_rows(), expected);
        let (engine, db) = seeded_sqlite("CREATE TABLE users (id INTEGER, name TEXT);", &data);
        let result = run_checks(&engine, &db, &data, &["users"]);
        engine.drop_ephemeral(db).expect("drop");
        assert!(result.expect("mismatch").contains("total mismatch"));
    }

    #[test]
    fn test_seed_checks_detect_corrupted_value() {
        // Same row count, but an expected value does not match what was stored.
        let expected = vec![
            HashMap::from([
                ("id".into(), serde_json::json!(1)),
                ("name".into(), serde_json::json!("a")),
            ]),
            HashMap::from([
                ("id".into(), serde_json::json!(2)),
                ("name".into(), serde_json::json!("WRONG")),
            ]),
        ];
        let data = seed_with_expected(two_rows(), expected);
        let (engine, db) = seeded_sqlite("CREATE TABLE users (id INTEGER, name TEXT);", &data);
        let result = run_checks(&engine, &db, &data, &["users"]);
        engine.drop_ephemeral(db).expect("drop");
        assert!(result.expect("mismatch").contains("missing"));
    }

    #[test]
    fn test_seed_checks_null_safe_match() {
        // A NULL column value must match an expected JSON null via `IS NULL`.
        let rows = vec![
            HashMap::from([
                ("id".into(), serde_json::json!(1)),
                ("note".into(), serde_json::json!(null)),
            ]),
            HashMap::from([
                ("id".into(), serde_json::json!(2)),
                ("note".into(), serde_json::json!("x")),
            ]),
        ];
        let data = HashMap::from([(
            "users".to_string(),
            TableSeedData {
                expected_after_up: rows.clone(),
                expected_after_down: rows.clone(),
                rows,
            },
        )]);
        let (engine, db) = seeded_sqlite("CREATE TABLE users (id INTEGER, note TEXT);", &data);
        let result = run_checks(&engine, &db, &data, &["users"]);
        engine.drop_ephemeral(db).expect("drop");
        assert!(result.is_none(), "expected null-safe match, got: {result:?}");
    }

    #[test]
    fn test_not_null_add_fails_against_seeded_rows() {
        // The core gap: adding a NOT NULL column with no default must fail
        // when existing rows are present. This is what seed insertion exposes.
        let data = seed_with_expected(two_rows(), two_rows());
        let (engine, db) = seeded_sqlite("CREATE TABLE users (id INTEGER, name TEXT);", &data);
        let result = engine.execute_in_transaction(&db, "ALTER TABLE users ADD COLUMN email TEXT NOT NULL;");
        engine.drop_ephemeral(db).expect("drop");
        assert!(
            result.is_err(),
            "NOT NULL add without default should fail on populated table"
        );
    }

    fn candidate_sql(up: &str, down: &str) -> MigrationOutput {
        MigrationOutput {
            up_sql: up.into(),
            down_sql: down.into(),
            description: "test".into(),
            seed_data: HashMap::new(),
        }
    }

    fn literal_issues(up: &str, down: &str) -> Vec<String> {
        let dialect = sqlparser::dialect::SQLiteDialect {};
        migration_literal_data_issues(&dialect, &candidate_sql(up, down))
    }

    #[test]
    fn test_literal_insert_values_rejected() {
        let issues = literal_issues("INSERT INTO t (id, name) VALUES (1, 'a');", "DELETE FROM t;");
        assert_eq!(issues.len(), 1, "{issues:?}");
        assert!(issues[0].contains("up_sql"), "{}", issues[0]);
    }

    #[test]
    fn test_insert_default_values_rejected() {
        let issues = literal_issues("INSERT INTO t DEFAULT VALUES;", "");
        assert_eq!(issues.len(), 1, "{issues:?}");
    }

    #[test]
    fn test_constant_select_without_from_rejected() {
        // Literal data smuggled through a constant SELECT (no FROM).
        let issues = literal_issues("INSERT INTO t (id) SELECT 1 UNION ALL SELECT 2;", "");
        assert_eq!(issues.len(), 1, "{issues:?}");
    }

    #[test]
    fn test_union_with_constant_branch_rejected() {
        let issues = literal_issues("INSERT INTO t (id) SELECT id FROM other UNION ALL SELECT 99;", "");
        assert_eq!(issues.len(), 1, "{issues:?}");
    }

    #[test]
    fn test_insert_select_from_table_allowed() {
        let issues = literal_issues(
            "INSERT INTO new_t (id, name) SELECT id, name FROM old_t;",
            "INSERT INTO old_t (id, name) SELECT id, name FROM new_t;",
        );
        assert!(issues.is_empty(), "{issues:?}");
    }

    #[test]
    fn test_insert_select_from_table_with_constant_column_allowed() {
        // Constant columns in the projection are fine when rows come from a table.
        let issues = literal_issues("INSERT INTO t (id, status) SELECT id, 'active' FROM src;", "");
        assert!(issues.is_empty(), "{issues:?}");
    }

    #[test]
    fn test_pure_ddl_and_transforms_allowed() {
        let issues = literal_issues(
            "ALTER TABLE t ADD COLUMN x INT;\nUPDATE t SET x = id;",
            "ALTER TABLE t DROP COLUMN x;",
        );
        assert!(issues.is_empty(), "{issues:?}");
    }

    #[test]
    fn test_append_data_issue_into_empty_and_nonempty() {
        let mut empty = String::new();
        append_data_issue(&mut empty, "expected_after_up", "oops");
        assert_eq!(empty, "Data preservation check failed (expected_after_up):\n- oops");

        let mut existing = String::from("--- a\n+++ b");
        append_data_issue(&mut existing, "expected_after_down", "bad");
        assert!(existing.starts_with("--- a\n+++ b\n"));
        assert!(existing.contains("Data preservation check failed (expected_after_down)"));
    }

    fn rule(id: &str, level: RuleLevel) -> ValidationRule {
        ValidationRule {
            id: id.into(),
            level,
            rule: "r".into(),
        }
    }

    fn verdict(status: VerdictStatus, detail: &str) -> RuleVerdict {
        RuleVerdict {
            status,
            detail: detail.into(),
        }
    }

    #[test]
    fn test_classify_violations_splits_failures_by_level() {
        let verdicts = vec![
            (
                rule("drop-table", RuleLevel::Error),
                verdict(VerdictStatus::Fail, "drops users"),
            ),
            (
                rule("drop-index", RuleLevel::Warning),
                verdict(VerdictStatus::Fail, "drops idx"),
            ),
        ];
        let (errors, warnings) = classify_violations(&verdicts);
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].rule_id, "drop-table");
        assert_eq!(errors[0].detail, "drops users");
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].rule_id, "drop-index");
    }

    #[test]
    fn test_classify_violations_ignores_passing_verdicts() {
        let verdicts = vec![(rule("drop-table", RuleLevel::Error), verdict(VerdictStatus::Pass, ""))];
        let (errors, warnings) = classify_violations(&verdicts);
        assert!(errors.is_empty());
        assert!(warnings.is_empty());
    }
}
