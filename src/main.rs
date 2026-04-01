use clap::{Parser, Subcommand};

use aim::config::{self, CliOverrides, Config, EngineKind, FormatKind};
use aim::engine::mysql::MysqlEngine;
use aim::engine::postgres::PostgresEngine;
use aim::engine::sqlite::SqliteEngine;
use aim::engine::{self, DatabaseEngine};
use aim::output::Output;
use aim::{agent, display};

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

    /// LLM model in <provider>-<model> format (e.g. anthropic-claude-haiku-4-5-20251001).
    #[arg(long, global = true)]
    model: Option<String>,
}

#[derive(Subcommand)]
enum Command {
    /// Create config, schema.sql, and migrations directory.
    Init,
    /// Show the diff between schema.sql and the current migrations.
    Diff,
    /// Generate, verify, and write a migration.
    Generate,
}

impl Cli {
    fn overrides(&self) -> CliOverrides {
        CliOverrides {
            engine: self.engine.clone(),
            format: self.format,
            schema: self.schema.clone(),
            migrations: self.migrations.clone(),
            max_retries: self.max_retries,
            model: self.model.clone(),
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
        Command::Diff => cmd_diff(&cli)?,
        Command::Generate => cmd_generate(&cli).await?,
    }
    Ok(())
}

fn cmd_init(cli: &Cli) -> Result<(), Box<dyn std::error::Error>> {
    let cwd = std::env::current_dir()?;
    let config_path = cwd.join("aim.toml");
    let schema_path = cwd.join("schema.sql");
    let migrations_dir = cwd.join("migrations");

    let engine = cli.engine.clone().ok_or("--engine is required for init")?;
    let format = cli.format.unwrap_or(FormatKind::Migrate);
    let model = cli.model.as_deref().map(config::ModelSpec::parse).transpose()?;

    if config_path.exists() {
        return Err("aim.toml already exists".into());
    }

    std::fs::write(&config_path, Config::default_toml(&engine, model.as_ref(), format))?;
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

fn cmd_diff(cli: &Cli) -> Result<(), Box<dyn std::error::Error>> {
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
    }

    Ok(())
}

async fn cmd_generate(cli: &Cli) -> Result<(), Box<dyn std::error::Error>> {
    let cwd = std::env::current_dir()?;
    let config = Config::load(&cwd, cli.overrides())?;
    let engine = create_engine(&config)?;
    let format = config.format.create();

    let prior = format.list(&config.migrations_dir)?;
    let next_seq = format.next_sequence(&config.migrations_dir)?;

    let model = config
        .model
        .ok_or("--model is required for generate (set in aim.toml or pass --model)")?;

    let agent_loop = agent::AgentLoop::new(
        engine.as_ref(),
        config.schema_path.clone(),
        model,
        config.max_retries,
        config.context.clone(),
    );

    let result = match agent_loop.run(&prior, next_seq).await {
        Ok(r) => r,
        Err(agent::Error::NoChanges) => return Ok(()),
        Err(e) => return Err(e.into()),
    };
    let m = &result.migration;

    // Write migration files.
    format.write(
        &config.migrations_dir,
        m,
        engine.migration_prefix(),
        engine.migration_suffix(),
    )?;

    use yansi::Paint;
    println!();
    println!("{}", "Generated...".bold());
    println!("Wrote {}", format.describe_written(m));
    let prefix = engine.migration_prefix();
    let suffix = engine.migration_suffix();
    println!("\n-- UP --");
    display::highlight_sql(&format!("{prefix}{}\n{suffix}", engine.format_sql(&m.up_sql)));
    println!("\n-- DOWN --");
    display::highlight_sql(&format!("{prefix}{}\n{suffix}", engine.format_sql(&m.down_sql)));

    Ok(())
}
