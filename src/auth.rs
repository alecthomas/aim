use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::io::{self, BufRead, Write};
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Metadata for a supported LLM provider's authentication.
pub struct ProviderInfo {
    /// Environment variable name (e.g. `ANTHROPIC_API_KEY`).
    pub env_var: &'static str,
    /// URL where users can create/manage API keys.
    pub console_url: &'static str,
}

/// Registry of known providers and their auth details.
///
/// To add a new provider, just add an entry here.
const PROVIDERS: &[(&str, ProviderInfo)] = &[
    (
        "anthropic",
        ProviderInfo {
            env_var: "ANTHROPIC_API_KEY",
            console_url: "https://console.anthropic.com/settings/keys",
        },
    ),
    (
        "cohere",
        ProviderInfo {
            env_var: "COHERE_API_KEY",
            console_url: "https://dashboard.cohere.com/api-keys",
        },
    ),
    (
        "deepseek",
        ProviderInfo {
            env_var: "DEEPSEEK_API_KEY",
            console_url: "https://platform.deepseek.com/api_keys",
        },
    ),
    (
        "gemini",
        ProviderInfo {
            env_var: "GEMINI_API_KEY",
            console_url: "https://aistudio.google.com/apikey",
        },
    ),
    (
        "groq",
        ProviderInfo {
            env_var: "GROQ_API_KEY",
            console_url: "https://console.groq.com/keys",
        },
    ),
    (
        "mistral",
        ProviderInfo {
            env_var: "MISTRAL_API_KEY",
            console_url: "https://console.mistral.ai/api-keys",
        },
    ),
    (
        "openai",
        ProviderInfo {
            env_var: "OPENAI_API_KEY",
            console_url: "https://platform.openai.com/api-keys",
        },
    ),
    (
        "openrouter",
        ProviderInfo {
            env_var: "OPENROUTER_API_KEY",
            console_url: "https://openrouter.ai/keys",
        },
    ),
    (
        "perplexity",
        ProviderInfo {
            env_var: "PERPLEXITY_API_KEY",
            console_url: "https://www.perplexity.ai/settings/api",
        },
    ),
    (
        "together",
        ProviderInfo {
            env_var: "TOGETHER_API_KEY",
            console_url: "https://api.together.xyz/settings/api-keys",
        },
    ),
    (
        "xai",
        ProviderInfo {
            env_var: "XAI_API_KEY",
            console_url: "https://console.x.ai/team/default/api-keys",
        },
    ),
];

/// Errors from auth operations.
#[derive(Debug)]
pub enum Error {
    /// Provider name not found in the registry.
    UnknownProvider(String),
    /// Failed to read/write the credentials file.
    Io(io::Error),
    /// Failed to parse the credentials file.
    Parse(toml::de::Error),
    /// Failed to serialize credentials.
    Serialize(toml::ser::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::UnknownProvider(name) => write!(f, "unknown provider: {name}"),
            Error::Io(err) => write!(f, "credentials file: {err}"),
            Error::Parse(err) => write!(f, "parsing credentials: {err}"),
            Error::Serialize(err) => write!(f, "serializing credentials: {err}"),
        }
    }
}

impl std::error::Error for Error {}

/// Look up a provider's auth info by name.
pub fn provider_info(provider: &str) -> Option<&'static ProviderInfo> {
    PROVIDERS
        .iter()
        .find(|(name, _)| *name == provider)
        .map(|(_, info)| info)
}

/// Path to the credentials file (`~/.config/aim/credentials.toml`).
fn credentials_path() -> Option<PathBuf> {
    dirs::config_dir().map(|d| d.join("aim").join("credentials.toml"))
}

/// On-disk format: a simple map of provider name to API key.
#[derive(Debug, Default, Serialize, Deserialize)]
struct Credentials {
    #[serde(flatten)]
    keys: BTreeMap<String, String>,
}

/// Load credentials from disk. Returns empty credentials if the file doesn't exist.
fn load_credentials() -> Result<Credentials, Error> {
    let Some(path) = credentials_path() else {
        return Ok(Credentials::default());
    };
    if !path.exists() {
        return Ok(Credentials::default());
    }
    let contents = fs::read_to_string(&path).map_err(Error::Io)?;
    toml::from_str(&contents).map_err(Error::Parse)
}

/// Save credentials to disk, creating parent directories as needed.
///
/// The file is created with mode 0600 (owner read/write only) since it
/// contains API keys.
fn save_credentials(creds: &Credentials) -> Result<(), Error> {
    let path =
        credentials_path().ok_or_else(|| Error::Io(io::Error::new(io::ErrorKind::NotFound, "no config directory")))?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(Error::Io)?;
    }
    let contents = toml::to_string_pretty(creds).map_err(Error::Serialize)?;
    write_private(&path, &contents)
}

/// Write `contents` to `path` with mode 0600 on Unix.
fn write_private(path: &std::path::Path, contents: &str) -> Result<(), Error> {
    use std::io::Write;

    let mut file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .map_err(Error::Io)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(fs::Permissions::from_mode(0o600))
            .map_err(Error::Io)?;
    }

    file.write_all(contents.as_bytes()).map_err(Error::Io)
}

/// Resolve an API key for the given provider.
///
/// Checks (in order):
/// 1. The provider's environment variable (e.g. `ANTHROPIC_API_KEY`)
/// 2. The credentials file (`~/.config/aim/credentials.toml`)
///
/// Returns `None` if no key is found.
pub fn resolve_api_key(provider: &str) -> Option<String> {
    if let Some(info) = provider_info(provider)
        && let Ok(val) = std::env::var(info.env_var)
        && !val.is_empty()
    {
        return Some(val);
    }
    load_credentials().ok().and_then(|c| c.keys.get(provider).cloned())
}

/// Store an API key for the given provider in the credentials file.
pub fn store_api_key(provider: &str, key: &str) -> Result<(), Error> {
    let mut creds = load_credentials()?;
    creds.keys.insert(provider.to_owned(), key.to_owned());
    save_credentials(&creds)
}

/// Run the interactive auth flow for a provider.
///
/// Opens the provider's API key console in the browser, prompts the user to
/// paste their key, and stores it in the credentials file.
pub fn login_interactive(provider: &str) -> Result<(), Error> {
    let info = provider_info(provider).ok_or_else(|| Error::UnknownProvider(provider.to_owned()))?;

    eprintln!("Opening {} API key page in your browser...", provider);
    eprintln!("  {}", info.console_url);
    let _ = open::that(info.console_url);

    eprint!("\nPaste your API key: ");
    io::stderr().flush().map_err(Error::Io)?;

    let key = read_line_hidden().map_err(Error::Io)?;
    eprintln!();

    let key = key.trim().to_owned();
    if key.is_empty() {
        return Err(Error::Io(io::Error::new(io::ErrorKind::InvalidInput, "empty API key")));
    }

    store_api_key(provider, &key)?;

    if let Some(path) = credentials_path() {
        eprintln!("API key stored in {}", path.display());
    }

    Ok(())
}

/// Read a line from stdin without echoing (for secret input).
///
/// Falls back to normal line reading if terminal raw mode is unavailable.
fn read_line_hidden() -> io::Result<String> {
    // Try to disable echo on Unix
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;
        let stdin_fd = io::stdin().as_raw_fd();
        let mut termios = std::mem::MaybeUninit::uninit();
        // SAFETY: we pass a valid fd and a valid pointer to termios struct.
        let rc = unsafe { libc::tcgetattr(stdin_fd, termios.as_mut_ptr()) };
        if rc == 0 {
            // SAFETY: tcgetattr succeeded, so termios is initialized.
            let mut termios = unsafe { termios.assume_init() };
            let original = termios;
            termios.c_lflag &= !libc::ECHO;
            // SAFETY: valid fd and valid termios struct.
            unsafe { libc::tcsetattr(stdin_fd, libc::TCSANOW, &termios) };
            let mut line = String::new();
            let result = io::stdin().lock().read_line(&mut line);
            // Restore original terminal settings.
            // SAFETY: valid fd and valid termios struct.
            unsafe { libc::tcsetattr(stdin_fd, libc::TCSANOW, &original) };
            result?;
            return Ok(line);
        }
    }

    let mut line = String::new();
    io::stdin().lock().read_line(&mut line)?;
    Ok(line)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_provider_info_known() {
        let info = provider_info("anthropic").expect("anthropic should be known");
        assert_eq!(info.env_var, "ANTHROPIC_API_KEY");
        assert!(info.console_url.contains("anthropic"));
    }

    #[test]
    fn test_provider_info_unknown() {
        assert!(provider_info("nonexistent").is_none());
    }

    #[test]
    fn test_resolve_from_env() {
        // Temporarily set an env var
        let key = "test-key-12345";
        unsafe { std::env::set_var("ANTHROPIC_API_KEY", key) };
        let resolved = resolve_api_key("anthropic");
        unsafe { std::env::remove_var("ANTHROPIC_API_KEY") };
        assert_eq!(resolved.as_deref(), Some(key));
    }

    #[test]
    fn test_store_and_load_credentials() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let creds_path = dir.path().join("credentials.toml");

        let mut creds = Credentials::default();
        creds.keys.insert("testprovider".into(), "sk-test".into());

        let contents = toml::to_string_pretty(&creds).expect("serialize");
        fs::write(&creds_path, &contents).expect("write");

        let loaded: Credentials = toml::from_str(&fs::read_to_string(&creds_path).expect("read")).expect("parse");
        assert_eq!(loaded.keys.get("testprovider").map(String::as_str), Some("sk-test"));
    }

    #[test]
    fn test_credentials_roundtrip() {
        let mut creds = Credentials::default();
        creds.keys.insert("anthropic".into(), "sk-ant-123".into());
        creds.keys.insert("openai".into(), "sk-oai-456".into());

        let serialized = toml::to_string_pretty(&creds).expect("serialize");
        let deserialized: Credentials = toml::from_str(&serialized).expect("deserialize");

        assert_eq!(deserialized.keys.len(), 2);
        assert_eq!(deserialized.keys["anthropic"], "sk-ant-123");
        assert_eq!(deserialized.keys["openai"], "sk-oai-456");
    }

    #[cfg(unix)]
    #[test]
    fn test_write_private_sets_mode_600() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().expect("create temp dir");
        let path = dir.path().join("secret.toml");

        write_private(&path, "key = \"value\"").expect("write");

        let mode = fs::metadata(&path).expect("metadata").permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "expected 0600, got {mode:04o}");
    }
}
