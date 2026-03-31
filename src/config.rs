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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum EngineKind {
    Postgres,
    Mysql,
    Sqlite,
}

impl fmt::Display for EngineKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EngineKind::Postgres => write!(f, "postgres"),
            EngineKind::Mysql => write!(f, "mysql"),
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
    schema: Option<String>,
    migrations: Option<String>,
    max_retries: Option<usize>,
    /// Model specifier in `<provider>-<model>` format.
    model: Option<String>,
}

/// Resolved configuration used at runtime.
#[derive(Debug, Clone)]
pub struct Config {
    pub engine: EngineKind,
    pub schema_path: PathBuf,
    pub migrations_dir: PathBuf,
    pub max_retries: usize,
    pub model: ModelSpec,
}

/// CLI overrides — fields are `Option` so they layer on top of the file config.
#[derive(Debug, Default)]
pub struct CliOverrides {
    pub engine: Option<EngineKind>,
    pub schema: Option<String>,
    pub migrations: Option<String>,
    pub max_retries: Option<usize>,
    pub model: Option<String>,
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
                schema: None,
                migrations: None,
                max_retries: None,
                model: None,
            }
        };

        let engine = overrides
            .engine
            .or(file_cfg.engine)
            .ok_or_else(|| Error::Validation("engine must be specified".into()))?;

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

        let model_str = overrides
            .model
            .or(file_cfg.model)
            .unwrap_or_else(|| "openai-gpt-4o".into());
        let model = ModelSpec::parse(&model_str)?;

        Ok(Config {
            engine,
            schema_path,
            migrations_dir,
            max_retries,
            model,
        })
    }

    /// Generate a default `aim.toml` string.
    pub fn default_toml(engine: EngineKind, model: &ModelSpec) -> String {
        format!(
            r#"engine = "{engine}"
schema = "schema.sql"
migrations = "migrations"
max_retries = 3
model = "{model}"
"#
        )
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
    fn test_default_toml() {
        let model = ModelSpec::parse("anthropic-claude-haiku-4-5-20251001").expect("parse");
        let toml = Config::default_toml(EngineKind::Sqlite, &model);
        assert_eq!(
            toml,
            r#"engine = "sqlite"
schema = "schema.sql"
migrations = "migrations"
max_retries = 3
model = "anthropic-claude-haiku-4-5-20251001"
"#
        );
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
