use clap::{Parser, Subcommand};

use aim::auth;
use aim::config::{self, CliOverrides, Config, EngineKind, FormatKind};
use aim::engine::mysql::MysqlEngine;
use aim::engine::postgres::PostgresEngine;
use aim::engine::sqlite::SqliteEngine;
use aim::engine::{self, DatabaseEngine};
use aim::output::Output;
use aim::{agent, display, seed};

#[derive(Parser)]
#[command(
    name = "aim",
    about = "AI Migrator (AIM) - verifiable AI powered SQL migration generator"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,

    /// Database engine (sqlite, mysql, postgres-<version>).
    #[arg(long, global = true, value_parser = EngineKind::parse)]
    engine: Option<EngineKind>,

    /// Migration file format (default: migrate).
    #[arg(long, global = true)]
    format: Option<FormatKind>,

    /// Path to schema file (default: schema.sql).
    #[arg(long, global = true)]
    schema: Option<String>,

    /// Path to migrations directory (default: migrations).
    #[arg(long, global = true)]
    migrations: Option<String>,

    /// Maximum LLM retries on verification failure.
    #[arg(long, global = true)]
    max_retries: Option<usize>,

    /// Maximum output tokens for LLM responses (increase for large schemas).
    #[arg(long, global = true)]
    max_tokens: Option<u64>,

    /// LLM model in <provider>-<model> format (e.g. anthropic-claude-haiku-4-5-20251001).
    #[arg(long, global = true)]
    model: Option<String>,

    /// Extra context to include in the LLM prompt.
    #[arg(long, global = true)]
    context: Option<String>,
}

#[derive(Subcommand)]
enum Command {
    /// Create config, schema.sql, and migrations directory.
    Init,
    /// Show the diff between schema.sql and the current migrations.
    Diff {
        /// Exit with non-zero status if schema differs from migrations.
        #[arg(long)]
        exit_code: bool,
    },
    /// Generate, verify, and write a migration.
    Generate {
        /// Generate and verify but don't write migration files.
        #[arg(long)]
        dry_run: bool,
    },
    /// Validate all migrations: UP, DOWN, and UP+DOWN+UP idempotency.
    Validate,
    /// Configure API key for an LLM provider.
    Auth {
        /// Provider name (e.g. anthropic, openai). Inferred from aim.toml if omitted.
        provider: Option<String>,
    },
}

impl Cli {
    fn overrides(&self) -> CliOverrides {
        CliOverrides {
            engine: self.engine.clone(),
            format: self.format,
            schema: self.schema.clone(),
            migrations: self.migrations.clone(),
            max_retries: self.max_retries,
            max_tokens: self.max_tokens,
            model: self.model.clone(),
            context: self.context.clone(),
        }
    }
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    yansi::whenever(yansi::Condition::TTY_AND_COLOR);
    let cli = Cli::parse();
    let result = run(cli).await;
    if let Err(err) = result {
        eprintln!("error: {err}");
        return std::process::ExitCode::FAILURE;
    }
    std::process::ExitCode::SUCCESS
}

async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    match cli.command {
        Command::Init => cmd_init(&cli)?,
        Command::Diff { exit_code } => cmd_diff(&cli, exit_code)?,
        Command::Generate { dry_run } => cmd_generate(&cli, dry_run).await?,
        Command::Validate => cmd_validate(&cli)?,
        Command::Auth { ref provider } => cmd_auth(&cli, provider.clone())?,
    }
    Ok(())
}

fn cmd_auth(cli: &Cli, provider: Option<String>) -> Result<(), Box<dyn std::error::Error>> {
    let provider = match provider {
        Some(p) => p,
        None => {
            let cwd = std::env::current_dir()?;
            let config = Config::load(&cwd, cli.overrides());
            config
                .ok()
                .and_then(|c| c.model.map(|m| m.provider.to_owned()))
                .ok_or("provider is required (pass it as an argument or set model in aim.toml)")?
        }
    };
    auth::login_interactive(&provider)?;
    Ok(())
}

fn cmd_init(cli: &Cli) -> Result<(), Box<dyn std::error::Error>> {
    let cwd = std::env::current_dir()?;
    let config_path = cwd.join("aim.toml");

    let schema = cli.schema.as_deref().unwrap_or("schema.sql");
    let migrations = cli.migrations.as_deref().unwrap_or("migrations");
    let schema_path = cwd.join(schema);
    let migrations_dir = cwd.join(migrations);

    let engine = cli.engine.clone().ok_or("--engine is required for init")?;
    let format = cli.format.unwrap_or(FormatKind::Migrate);
    let model = cli.model.as_deref().map(config::ModelSpec::parse).transpose()?;
    let max_tokens = cli.max_tokens.unwrap_or(16384);

    if config_path.exists() {
        return Err("aim.toml already exists".into());
    }

    std::fs::write(
        &config_path,
        Config::default_toml(&engine, model.as_ref(), format, schema, migrations, max_tokens),
    )?;
    if !schema_path.exists() {
        std::fs::write(&schema_path, "")?;
    }
    std::fs::create_dir_all(&migrations_dir)?;

    if let Some(model) = &model {
        println!("Initialized aim project with {engine} engine and {model} model");
    } else {
        println!("Initialized aim project with {engine} engine");
    }
    Ok(())
}

/// Create the appropriate database engine based on config.
fn create_engine(config: &Config) -> Result<Box<dyn DatabaseEngine>, Box<dyn std::error::Error>> {
    match &config.engine {
        EngineKind::Sqlite => Ok(Box::new(SqliteEngine)),
        EngineKind::Postgres { version } => Ok(Box::new(PostgresEngine::new(version))),
        EngineKind::Mysql { version } => Ok(Box::new(MysqlEngine::new(&format!("mysql:{version}")))),
        EngineKind::Mariadb { version } => Ok(Box::new(MysqlEngine::new(&format!("mariadb:{version}")))),
    }
}

fn cmd_diff(cli: &Cli, exit_code: bool) -> Result<(), Box<dyn std::error::Error>> {
    let cwd = std::env::current_dir()?;
    let config = Config::load(&cwd, cli.overrides())?;
    let engine = create_engine(&config)?;
    let format = config.format.create();

    let prior = format.list(&config.migrations_dir)?;

    // Build normalized schemas via ephemeral DBs.
    let db_desired = engine.create_ephemeral()?;
    let schema_sql = std::fs::read_to_string(&config.schema_path)?;
    engine.execute(&db_desired, &schema_sql)?;

    let db_current = engine.create_ephemeral()?;
    for m in &prior {
        engine.execute(&db_current, &m.up_sql)?;
    }

    let desired_schema = engine.dump_schema(&db_desired)?;
    let current_schema = engine.dump_schema(&db_current)?;

    engine.drop_ephemeral(db_desired)?;
    engine.drop_ephemeral(db_current)?;

    let migrations_label = config
        .migrations_dir
        .strip_prefix(&cwd)
        .unwrap_or(&config.migrations_dir)
        .display()
        .to_string();
    let schema_label = config
        .schema_path
        .strip_prefix(&cwd)
        .unwrap_or(&config.schema_path)
        .display()
        .to_string();
    let diff = engine::schema_diff(
        engine.dialect().as_ref(),
        &current_schema,
        &migrations_label,
        &desired_schema,
        &schema_label,
    );

    if diff.is_empty() {
        Output::success("schema.sql matches current migrations");
    } else {
        Output::diff("schema", &diff);
        if exit_code {
            return Err("schema.sql differs from current migrations".into());
        }
    }

    Ok(())
}

async fn cmd_generate(cli: &Cli, dry_run: bool) -> Result<(), Box<dyn std::error::Error>> {
    let cwd = std::env::current_dir()?;
    let config = Config::load(&cwd, cli.overrides())?;
    let engine = create_engine(&config)?;
    let format = config.format.create();

    let prior = format.list(&config.migrations_dir)?;

    // Check if a migration is needed before invoking the LLM.
    let db_desired = engine.create_ephemeral()?;
    let schema_sql = std::fs::read_to_string(&config.schema_path)?;
    engine.execute(&db_desired, &schema_sql)?;

    let db_current = engine.create_ephemeral()?;
    for m in &prior {
        engine.execute(&db_current, &m.up_sql)?;
    }

    let desired_schema = engine.dump_schema(&db_desired)?;
    let current_schema = engine.dump_schema(&db_current)?;

    engine.drop_ephemeral(db_desired)?;
    engine.drop_ephemeral(db_current)?;

    let diff = engine::schema_diff(
        engine.dialect().as_ref(),
        &current_schema,
        "migrations",
        &desired_schema,
        "schema.sql",
    );
    if diff.is_empty() {
        Output::success("schema.sql matches current migrations, nothing to generate");
        return Ok(());
    }
    Output::phase("Schema changes:");
    Output::diff("schema", &diff);
    println!();

    let next_seq = format.next_sequence(&config.migrations_dir)?;

    let model = config
        .model
        .ok_or("--model is required for generate (set in aim.toml or pass --model)")?;

    let agent_loop = agent::AgentLoop::new(
        engine.as_ref(),
        config.schema_path.clone(),
        model,
        config.max_retries,
        config.max_tokens,
        config.context.clone(),
    );

    let result = match agent_loop.run(&prior, next_seq).await {
        Ok(r) => r,
        Err(agent::Error::NoChanges) => return Ok(()),
        Err(e) => return Err(e.into()),
    };
    let m = &result.migration;

    use yansi::Paint;

    if dry_run {
        println!();
        println!("{}", "Dry run — not writing files.".bold());
    } else {
        format.write(
            &config.migrations_dir,
            m,
            engine.migration_prefix(),
            engine.migration_suffix(),
        )?;

        println!();
        println!("{}", "Generated...".bold());
        println!("Wrote {}", format.describe_written(m));
    }
    if !result.seed_data.is_empty() {
        let inserts = seed::build_insert_statements(&result.seed_data);
        println!("\n-- SEED INSERT --");
        display::highlight_sql(&inserts);

        let selects_up = seed::build_select_statements(&result.seed_data, seed::Direction::Up);
        println!("\n-- SEED SELECT (after up) --");
        display::highlight_sql(&selects_up);

        let selects_down = seed::build_select_statements(&result.seed_data, seed::Direction::Down);
        println!("\n-- SEED SELECT (after down) --");
        display::highlight_sql(&selects_down);
    }

    let prefix = engine.migration_prefix();
    let suffix = engine.migration_suffix();
    println!("\n-- UP --");
    display::highlight_sql(&format!("{prefix}{}\n{suffix}", m.up_sql));
    println!("\n-- DOWN --");
    display::highlight_sql(&format!("{prefix}{}\n{suffix}", m.down_sql));

    Ok(())
}

fn cmd_validate(cli: &Cli) -> Result<(), Box<dyn std::error::Error>> {
    use yansi::Paint;

    let cwd = std::env::current_dir()?;
    let config = Config::load(&cwd, cli.overrides())?;
    let engine = create_engine(&config)?;
    let format = config.format.create();
    let dialect = engine.dialect();

    let migrations = format.list(&config.migrations_dir)?;
    if migrations.is_empty() {
        Output::success("no migrations to validate");
        return Ok(());
    }

    Output::phase(&format!("Validating {} migration(s)...", migrations.len()));

    let expected_db = engine.create_ephemeral()?;
    let mut failures = 0u32;

    for (i, migration) in migrations.iter().enumerate() {
        let prev_schema = engine.dump_schema(&expected_db)?;

        engine.execute(&expected_db, &migration.up_sql)?;
        let expected_up_schema = engine.dump_schema(&expected_db)?;

        let label = format!("{:06}_{}", migration.sequence, migration.description);
        print!("  {} {label} ", "check".cyan().bold());

        let test_db = engine.create_ephemeral()?;
        for prior in &migrations[..i] {
            engine.execute(&test_db, &prior.up_sql)?;
        }

        // UP test
        let up_ok = match engine.execute_in_transaction(&test_db, &migration.up_sql) {
            Ok(()) => {
                let schema = engine.dump_schema(&test_db)?;
                let diff = engine::schema_diff(dialect.as_ref(), &expected_up_schema, "expected", &schema, "after up");
                if diff.is_empty() {
                    true
                } else {
                    println!();
                    Output::error("UP schema mismatch");
                    Output::diff("up", &diff);
                    false
                }
            }
            Err(e) => {
                println!();
                Output::error(&format!("UP failed: {e}"));
                false
            }
        };

        // DOWN test
        let down_ok = match engine.execute_in_transaction(&test_db, &migration.down_sql) {
            Ok(()) => {
                let schema = engine.dump_schema(&test_db)?;
                let diff = engine::schema_diff(dialect.as_ref(), &prev_schema, "expected", &schema, "after down");
                if diff.is_empty() {
                    true
                } else {
                    println!();
                    Output::error("DOWN schema mismatch");
                    Output::diff("down", &diff);
                    false
                }
            }
            Err(e) => {
                println!();
                Output::error(&format!("DOWN failed: {e}"));
                false
            }
        };

        // UP+DOWN+UP idempotency test
        let idem_ok = if down_ok {
            match engine.execute_in_transaction(&test_db, &migration.up_sql) {
                Ok(()) => {
                    let schema = engine.dump_schema(&test_db)?;
                    let diff = engine::schema_diff(
                        dialect.as_ref(),
                        &expected_up_schema,
                        "expected",
                        &schema,
                        "after up+down+up",
                    );
                    if diff.is_empty() {
                        true
                    } else {
                        println!();
                        Output::error("UP+DOWN+UP schema mismatch");
                        Output::diff("idempotency", &diff);
                        false
                    }
                }
                Err(e) => {
                    println!();
                    Output::error(&format!("UP+DOWN+UP failed: {e}"));
                    false
                }
            }
        } else {
            false
        };

        engine.drop_ephemeral(test_db)?;

        if up_ok && down_ok && idem_ok {
            println!("{}", "ok".green().bold());
        } else {
            failures += 1;
        }
    }

    engine.drop_ephemeral(expected_db)?;

    println!();
    if failures == 0 {
        Output::success(&format!("all {} migration(s) validated", migrations.len()));
        Ok(())
    } else {
        Err(format!("{failures} migration(s) failed validation").into())
    }
}
