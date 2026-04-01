use std::fmt;
use std::path::{Path, PathBuf};

use serde::Deserialize;

/// Errors that can occur when loading or validating configuration.
#[derive(Debug)]
pub enum Error {
    /// Failed to read the config file from disk.
    Read(std::io::Error),
    /// Failed to parse the TOML contents.
    Parse(toml::de::Error),
    /// A required field is missing or invalid.
    Validation(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Read(err) => write!(f, "reading aim.toml: {err}"),
            Error::Parse(err) => write!(f, "parsing aim.toml: {err}"),
            Error::Validation(msg) => write!(f, "config: {msg}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Read(err) => Some(err),
            Error::Parse(err) => Some(err),
            Error::Validation(_) => None,
        }
    }
}

/// Supported database engines.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EngineKind {
    /// PostgreSQL with a specific Docker image version tag (e.g. "16", "17").
    Postgres {
        version: String,
    },
    /// MySQL with a specific Docker image version tag (e.g. "8", "9").
    Mysql {
        version: String,
    },
    /// MariaDB with a specific Docker image version tag (e.g. "11", "10").
    Mariadb {
        version: String,
    },
    Sqlite,
}

impl EngineKind {
    /// Parse an engine specifier string.
    ///
    /// Accepts `"sqlite"`, `"mysql-<version>"`, `"mariadb-<version>"`, or `"postgres-<version>"`.
    pub fn parse(spec: &str) -> Result<Self, String> {
        match spec {
            "sqlite" => Ok(EngineKind::Sqlite),
            s if s.starts_with("mysql-") => {
                let version = &s["mysql-".len()..];
                if version.is_empty() {
                    return Err("mysql version is required (e.g. mysql-9)".into());
                }
                Ok(EngineKind::Mysql {
                    version: version.to_owned(),
                })
            }
            "mysql" => Err("mysql requires a version (e.g. mysql-9)".into()),
            s if s.starts_with("mariadb-") => {
                let version = &s["mariadb-".len()..];
                if version.is_empty() {
                    return Err("mariadb version is required (e.g. mariadb-11)".into());
                }
                Ok(EngineKind::Mariadb {
                    version: version.to_owned(),
                })
            }
            "mariadb" => Err("mariadb requires a version (e.g. mariadb-11)".into()),
            s if s.starts_with("postgres-") => {
                let version = &s["postgres-".len()..];
                if version.is_empty() {
                    return Err("postgres version is required (e.g. postgres-17)".into());
                }
                Ok(EngineKind::Postgres {
                    version: version.to_owned(),
                })
            }
            "postgres" => Err("postgres requires a version (e.g. postgres-17)".into()),
            _ => Err(format!(
                "unknown engine '{spec}'; expected sqlite, mysql-<version>, mariadb-<version>, or postgres-<version>"
            )),
        }
    }
}

impl<'de> Deserialize<'de> for EngineKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        EngineKind::parse(&s).map_err(serde::de::Error::custom)
    }
}

/// Supported migration file formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub enum FormatKind {
    Migrate,
    Goose,
    Flyway,
    Sqitch,
    Sqlx,
    Dbmate,
    Refinery,
}

impl fmt::Display for FormatKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FormatKind::Migrate => write!(f, "migrate"),
            FormatKind::Goose => write!(f, "goose"),
            FormatKind::Flyway => write!(f, "flyway"),
            FormatKind::Sqitch => write!(f, "sqitch"),
            FormatKind::Sqlx => write!(f, "sqlx"),
            FormatKind::Dbmate => write!(f, "dbmate"),
            FormatKind::Refinery => write!(f, "refinery"),
        }
    }
}

impl FormatKind {
    /// Create the corresponding `MigrationFormat` trait object.
    pub fn create(self) -> Box<dyn crate::migration::MigrationFormat> {
        match self {
            FormatKind::Migrate => Box::new(crate::migration::migrate::Migrate),
            FormatKind::Goose => Box::new(crate::migration::goose::Goose),
            FormatKind::Flyway => Box::new(crate::migration::flyway::Flyway),
            FormatKind::Sqitch => Box::new(crate::migration::sqitch::Sqitch),
            FormatKind::Sqlx => Box::new(crate::migration::sqlx::Sqlx),
            FormatKind::Dbmate => Box::new(crate::migration::dbmate::Dbmate),
            FormatKind::Refinery => Box::new(crate::migration::refinery::Refinery),
        }
    }
}

impl fmt::Display for EngineKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EngineKind::Postgres { version } => write!(f, "postgres-{version}"),
            EngineKind::Mysql { version } => write!(f, "mysql-{version}"),
            EngineKind::Mariadb { version } => write!(f, "mariadb-{version}"),
            EngineKind::Sqlite => write!(f, "sqlite"),
        }
    }
}

/// A parsed `<provider>-<model>` specifier.
///
/// For example, `"anthropic-claude-3-5-haiku-latest"` splits into
/// provider `"anthropic"` and model `"claude-3-5-haiku-latest"`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelSpec {
    pub provider: &'static str,
    pub model: String,
}

/// Known provider names. The first `-` in the model string separates
/// the provider prefix from the model name.
const KNOWN_PROVIDERS: &[&str] = &[
    "anthropic",
    "azure",
    "cohere",
    "deepseek",
    "galadriel",
    "gemini",
    "groq",
    "huggingface",
    "hyperbolic",
    "mira",
    "mistral",
    "moonshot",
    "ollama",
    "openai",
    "openrouter",
    "perplexity",
    "together",
    "xai",
];

impl ModelSpec {
    /// Parse a `<provider>-<model>` string.
    ///
    /// Matches the longest known provider prefix. For example,
    /// `"openrouter-foo"` matches `"openrouter"` not `"open"`.
    pub fn parse(spec: &str) -> Result<Self, Error> {
        // Find the longest provider prefix that matches.
        let mut best: Option<&'static str> = None;
        for &provider in KNOWN_PROVIDERS {
            if spec.starts_with(provider)
                && spec[provider.len()..].starts_with('-')
                && best.is_none_or(|b| provider.len() > b.len())
            {
                best = Some(provider);
            }
        }

        let provider = best.ok_or_else(|| {
            Error::Validation(format!(
                "unknown provider in model spec '{spec}'; \
                 expected format: <provider>-<model> \
                 (providers: {})",
                KNOWN_PROVIDERS.join(", ")
            ))
        })?;

        // +1 to skip the `-` separator.
        let model = &spec[provider.len() + 1..];
        if model.is_empty() {
            return Err(Error::Validation(format!("model name is empty in '{spec}'")));
        }

        Ok(ModelSpec {
            provider,
            model: model.to_owned(),
        })
    }
}

impl fmt::Display for ModelSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.provider, self.model)
    }
}

/// On-disk representation of `aim.toml`.
#[derive(Debug, Deserialize)]
struct FileConfig {
    engine: Option<EngineKind>,
    format: Option<FormatKind>,
    schema: Option<String>,
    migrations: Option<String>,
    max_retries: Option<usize>,
    /// Model specifier in `<provider>-<model>` format.
    model: Option<String>,
    /// Extra context to include in the LLM prompt.
    context: Option<String>,
}

/// Resolved configuration used at runtime.
#[derive(Debug, Clone)]
pub struct Config {
    pub engine: EngineKind,
    pub format: FormatKind,
    pub schema_path: PathBuf,
    pub migrations_dir: PathBuf,
    pub max_retries: usize,
    pub model: Option<ModelSpec>,
    pub context: Option<String>,
}

/// CLI overrides — fields are `Option` so they layer on top of the file config.
#[derive(Debug, Default)]
pub struct CliOverrides {
    pub engine: Option<EngineKind>,
    pub format: Option<FormatKind>,
    pub schema: Option<String>,
    pub migrations: Option<String>,
    pub max_retries: Option<usize>,
    pub model: Option<String>,
    pub context: Option<String>,
}

impl Config {
    /// Load config from `aim.toml` in `project_root`, then apply CLI overrides.
    pub fn load(project_root: &Path, overrides: CliOverrides) -> Result<Self, Error> {
        let config_path = project_root.join("aim.toml");
        let file_cfg: FileConfig = if config_path.exists() {
            let contents = std::fs::read_to_string(&config_path).map_err(Error::Read)?;
            toml::from_str(&contents).map_err(Error::Parse)?
        } else {
            FileConfig {
                engine: None,
                format: None,
                schema: None,
                migrations: None,
                max_retries: None,
                model: None,
                context: None,
            }
        };

        let engine = overrides
            .engine
            .or(file_cfg.engine)
            .ok_or_else(|| Error::Validation("engine must be specified".into()))?;

        let format = overrides.format.or(file_cfg.format).unwrap_or(FormatKind::Migrate);

        let schema = overrides
            .schema
            .or(file_cfg.schema)
            .unwrap_or_else(|| "schema.sql".into());
        let schema_path = project_root.join(schema);

        let migrations = overrides
            .migrations
            .or(file_cfg.migrations)
            .unwrap_or_else(|| "migrations".into());
        let migrations_dir = project_root.join(migrations);

        let max_retries = overrides.max_retries.or(file_cfg.max_retries).unwrap_or(3);

        let model = match overrides.model.or(file_cfg.model) {
            Some(s) => Some(ModelSpec::parse(&s)?),
            None => None,
        };

        Ok(Config {
            engine,
            format,
            schema_path,
            migrations_dir,
            max_retries,
            model,
            context: overrides.context.or(file_cfg.context),
        })
    }

    /// Generate a default `aim.toml` string.
    pub fn default_toml(engine: &EngineKind, model: Option<&ModelSpec>, format: FormatKind) -> String {
        let mut toml = format!(
            r#"engine = "{engine}"
format = "{format}"
schema = "schema.sql"
migrations = "migrations"
max_retries = 3
"#
        );
        if let Some(model) = model {
            toml.push_str(&format!("model = \"{model}\"\n"));
        }
        toml
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_model_spec_parse_openai() {
        let spec = ModelSpec::parse("openai-gpt-4o").expect("parse");
        assert_eq!(spec.provider, "openai");
        assert_eq!(spec.model, "gpt-4o");
    }

    #[test]
    fn test_model_spec_parse_anthropic() {
        let spec = ModelSpec::parse("anthropic-claude-haiku-4-5-20251001").expect("parse");
        assert_eq!(spec.provider, "anthropic");
        assert_eq!(spec.model, "claude-haiku-4-5-20251001");
    }

    #[test]
    fn test_model_spec_parse_openrouter() {
        let spec = ModelSpec::parse("openrouter-meta-llama/llama-3-70b").expect("parse");
        assert_eq!(spec.provider, "openrouter");
        assert_eq!(spec.model, "meta-llama/llama-3-70b");
    }

    #[test]
    fn test_model_spec_parse_unknown_provider() {
        assert!(ModelSpec::parse("fakeprovider-some-model").is_err());
    }

    #[test]
    fn test_model_spec_parse_empty_model() {
        assert!(ModelSpec::parse("openai-").is_err());
    }

    #[test]
    fn test_model_spec_parse_no_separator() {
        assert!(ModelSpec::parse("openai").is_err());
    }

    #[test]
    fn test_default_toml_with_model() {
        let model = ModelSpec::parse("anthropic-claude-haiku-4-5-20251001").expect("parse");
        let toml = Config::default_toml(&EngineKind::Sqlite, Some(&model), FormatKind::Migrate);
        assert!(toml.contains(r#"engine = "sqlite""#));
        assert!(toml.contains(r#"model = "anthropic-claude-haiku-4-5-20251001""#));
    }

    #[test]
    fn test_default_toml_without_model() {
        let toml = Config::default_toml(&EngineKind::Sqlite, None, FormatKind::Migrate);
        assert!(toml.contains(r#"engine = "sqlite""#));
        assert!(!toml.contains("model"));
    }

    #[test]
    fn test_default_toml_postgres() {
        let engine = EngineKind::Postgres { version: "16".into() };
        let toml = Config::default_toml(&engine, None, FormatKind::Migrate);
        assert!(toml.contains(r#"engine = "postgres-16""#));
    }

    #[test]
    fn test_engine_kind_parse() {
        assert_eq!(EngineKind::parse("sqlite").unwrap(), EngineKind::Sqlite);
        assert_eq!(
            EngineKind::parse("mysql-9").unwrap(),
            EngineKind::Mysql { version: "9".into() }
        );
        assert!(EngineKind::parse("mysql").is_err());
        assert_eq!(
            EngineKind::parse("mariadb-11").unwrap(),
            EngineKind::Mariadb { version: "11".into() }
        );
        assert!(EngineKind::parse("mariadb").is_err());
        assert_eq!(
            EngineKind::parse("postgres-16").unwrap(),
            EngineKind::Postgres { version: "16".into() }
        );
        assert!(EngineKind::parse("postgres").is_err());
        assert!(EngineKind::parse("postgres-").is_err());
        assert!(EngineKind::parse("unknown").is_err());
    }

    #[test]
    fn test_model_spec_display() {
        let spec = ModelSpec {
            provider: "anthropic",
            model: "claude-haiku-4-5-20251001".to_owned(),
        };
        assert_eq!(spec.to_string(), "anthropic-claude-haiku-4-5-20251001");
    }
}
