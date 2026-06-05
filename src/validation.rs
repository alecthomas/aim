//! Schema-change validation rules.
//!
//! The LLM is instructed (via the system prompt) to check each generated up
//! migration against a set of rules and report any matches. Each rule has a
//! stable identifier, a severity ([`RuleLevel`]), and an English `rule`
//! describing the behaviour to flag. The `rule` text is injected verbatim into
//! the prompt so it directly drives the LLM's checking.
//!
//! Built-in rules can be turned off via `validation.disabled`, and users can
//! add their own under `[[validation.rules]]` in `aim.toml`. An `error`-level
//! match fails the migration; a `warning`-level match is reported but
//! non-blocking.
//!
//! Each rule is evaluated by its own isolated LLM extractor that sees ONLY the
//! up migration SQL (never the down migration, which is expected to be
//! destructive). See [`validator_preamble`] and [`RuleVerdict`].

use std::collections::HashSet;
use std::fmt;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Severity of a validation rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RuleLevel {
    /// Reported to the user but does not block the migration.
    Warning,
    /// Fails the migration.
    #[default]
    Error,
}

impl fmt::Display for RuleLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            RuleLevel::Warning => "warning",
            RuleLevel::Error => "error",
        };
        f.write_str(s)
    }
}

/// Outcome of evaluating a rule against the up migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum VerdictStatus {
    /// The up migration does NOT match the rule.
    Pass,
    /// The up migration matches the rule.
    Fail,
}

/// Structured verdict extracted from an isolated per-rule validator.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RuleVerdict {
    /// Whether the up migration matches the rule.
    pub status: VerdictStatus,
    /// One-sentence explanation of what triggered the rule. Empty when `pass`.
    #[serde(default)]
    pub detail: String,
}

/// A single validation rule applied to generated up migrations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationRule {
    /// Stable identifier, used to disable the rule and to tag violations.
    pub id: String,
    /// Severity of a match.
    pub level: RuleLevel,
    /// English description of the behaviour to flag. Injected into the prompt.
    pub rule: String,
}

/// On-disk `[validation]` section of `aim.toml`.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileValidationConfig {
    /// Identifiers of built-in rules to turn off.
    #[serde(default)]
    pub disabled: Vec<String>,
    /// User-defined rules.
    #[serde(default)]
    pub rules: Vec<CustomRule>,
}

/// A user-defined rule from `[[validation.rules]]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CustomRule {
    pub id: String,
    /// Defaults to `error` when omitted.
    #[serde(default)]
    pub level: RuleLevel,
    /// English description of the behaviour to flag.
    pub rule: String,
}

/// Built-in validation rules, applied unless explicitly disabled.
pub fn builtin_rules() -> Vec<ValidationRule> {
    use RuleLevel::{Error, Warning};
    [
        (
            "drop-table",
            Error,
            "Flag any statement that DROPs a table; this permanently destroys all rows in that table.",
        ),
        (
            "drop-column",
            Error,
            "Flag any statement that DROPs a column; this permanently destroys all data in that column.",
        ),
        (
            "narrowing-type-change",
            Error,
            "Flag any column type change that can truncate values, lose precision, or fail on \
             existing data (e.g. shrinking a length, or converting text to a numeric type).",
        ),
        (
            "remove-enum-value",
            Error,
            "Flag the removal or renaming of an enum value, which can orphan rows that reference it.",
        ),
        (
            "destructive-dml",
            Error,
            "Flag TRUNCATE statements, or DELETE/UPDATE statements without a WHERE clause, since \
             they affect every row in the table.",
        ),
        (
            "add-not-null-without-default",
            Warning,
            "Flag adding a NOT NULL column or constraint without a DEFAULT, which fails when the \
             table already contains rows.",
        ),
        (
            "drop-index",
            Warning,
            "Flag dropping an index, which may degrade query performance.",
        ),
    ]
    .into_iter()
    .map(|(id, level, rule)| ValidationRule {
        id: id.to_string(),
        level,
        rule: rule.to_string(),
    })
    .collect()
}

/// Resolve the effective rule set: built-ins minus `disabled`, plus custom rules.
///
/// A custom rule may reuse the id of a disabled built-in to override it.
/// Errors on unknown disabled ids, empty fields, or id collisions with an
/// active rule.
pub fn resolve(file: &FileValidationConfig) -> Result<Vec<ValidationRule>, String> {
    let builtins = builtin_rules();
    let builtin_ids: HashSet<&str> = builtins.iter().map(|r| r.id.as_str()).collect();

    for id in &file.disabled {
        if !builtin_ids.contains(id.as_str()) {
            return Err(format!(
                "validation.disabled references unknown rule id '{id}'; known built-in rules: {}",
                sorted_ids(&builtins).join(", ")
            ));
        }
    }

    let disabled: HashSet<&str> = file.disabled.iter().map(String::as_str).collect();
    let mut rules: Vec<ValidationRule> = builtins
        .into_iter()
        .filter(|r| !disabled.contains(r.id.as_str()))
        .collect();

    let mut seen: HashSet<String> = rules.iter().map(|r| r.id.clone()).collect();
    for custom in &file.rules {
        if custom.id.trim().is_empty() {
            return Err("validation rule id must not be empty".to_string());
        }
        if custom.rule.trim().is_empty() {
            return Err(format!(
                "validation rule '{}' must specify a non-empty 'rule'",
                custom.id
            ));
        }
        if !seen.insert(custom.id.clone()) {
            return Err(format!(
                "validation rule id '{}' collides with an active rule; disable the built-in \
                 of the same id or choose a different id",
                custom.id
            ));
        }
        rules.push(ValidationRule {
            id: custom.id.clone(),
            level: custom.level,
            rule: custom.rule.clone(),
        });
    }

    Ok(rules)
}

/// Build the preamble for an isolated validator that checks the up migration
/// against a single `rule` for the given SQL `dialect`.
///
/// The validator is handed ONLY the up migration SQL, so it cannot consider the
/// down migration. It returns a [`RuleVerdict`].
pub fn validator_preamble(dialect: &str, rule: &ValidationRule) -> String {
    format!(
        "You are a SQL migration validator. You are given the UP statements of one database \
         migration and must decide whether they match a single rule.\n\n\
         SQL dialect: {dialect}\n\n\
         Rule [{id}]: {text}\n\n\
         Evaluate ONLY the migration SQL provided in the next message against this one rule. \
         Set status to \"fail\" if the migration matches the rule (it performs the flagged \
         operation), or \"pass\" if it does not. When status is \"fail\", set detail to a \
         one-sentence explanation naming the specific statement(s) that triggered the rule; \
         otherwise leave detail empty. Judge only what the SQL actually does — do not speculate \
         about statements that are not present.",
        id = rule.id,
        text = rule.rule,
    )
}

/// Sorted rule ids, for stable error messages.
fn sorted_ids(rules: &[ValidationRule]) -> Vec<&str> {
    let mut ids: Vec<&str> = rules.iter().map(|r| r.id.as_str()).collect();
    ids.sort_unstable();
    ids
}

#[cfg(test)]
mod tests {
    use super::*;

    fn custom(id: &str, level: RuleLevel, rule: &str) -> CustomRule {
        CustomRule {
            id: id.to_string(),
            level,
            rule: rule.to_string(),
        }
    }

    #[test]
    fn test_resolve_defaults_to_builtins() {
        let resolved = resolve(&FileValidationConfig::default()).expect("resolve");
        assert_eq!(resolved.len(), builtin_rules().len());
    }

    #[test]
    fn test_resolve_disables_builtin() {
        let file = FileValidationConfig {
            disabled: vec!["drop-column".to_string()],
            rules: Vec::new(),
        };
        let resolved = resolve(&file).expect("resolve");
        assert!(!resolved.iter().any(|r| r.id == "drop-column"));
        assert_eq!(resolved.len(), builtin_rules().len() - 1);
    }

    #[test]
    fn test_resolve_unknown_disabled_id_errors() {
        let file = FileValidationConfig {
            disabled: vec!["does-not-exist".to_string()],
            rules: Vec::new(),
        };
        let err = resolve(&file).expect_err("should error");
        assert!(err.contains("does-not-exist"));
    }

    #[test]
    fn test_resolve_appends_custom_rule() {
        let file = FileValidationConfig {
            disabled: Vec::new(),
            rules: vec![custom("no-cascade", RuleLevel::Warning, "flag cascade")],
        };
        let resolved = resolve(&file).expect("resolve");
        let added = resolved.iter().find(|r| r.id == "no-cascade").expect("custom rule");
        assert_eq!(added.level, RuleLevel::Warning);
        assert_eq!(added.rule, "flag cascade");
    }

    #[test]
    fn test_custom_rule_level_defaults_to_error() {
        let toml = r#"
            [[rules]]
            id = "x"
            rule = "flag x"
        "#;
        let file: FileValidationConfig = toml::from_str(toml).expect("parse");
        let resolved = resolve(&file).expect("resolve");
        let added = resolved.iter().find(|r| r.id == "x").expect("custom rule");
        assert_eq!(added.level, RuleLevel::Error);
    }

    #[test]
    fn test_resolve_custom_collision_errors() {
        let file = FileValidationConfig {
            disabled: Vec::new(),
            rules: vec![custom("drop-table", RuleLevel::Warning, "redefine")],
        };
        let err = resolve(&file).expect_err("should error");
        assert!(err.contains("drop-table"));
    }

    #[test]
    fn test_disabled_builtin_can_be_overridden_by_custom() {
        let file = FileValidationConfig {
            disabled: vec!["drop-table".to_string()],
            rules: vec![custom("drop-table", RuleLevel::Warning, "softer rule")],
        };
        let resolved = resolve(&file).expect("resolve");
        let rule = resolved.iter().find(|r| r.id == "drop-table").expect("rule");
        assert_eq!(rule.level, RuleLevel::Warning);
        assert_eq!(rule.rule, "softer rule");
    }

    #[test]
    fn test_resolve_empty_rule_text_errors() {
        let file = FileValidationConfig {
            disabled: Vec::new(),
            rules: vec![custom("x", RuleLevel::Error, "  ")],
        };
        assert!(resolve(&file).is_err());
    }

    #[test]
    fn test_validator_preamble_includes_rule_and_dialect() {
        let rule = ValidationRule {
            id: "drop-table".to_string(),
            level: RuleLevel::Error,
            rule: "no dropping tables".to_string(),
        };
        let preamble = validator_preamble("SQLite", &rule);
        assert!(preamble.contains("SQLite"));
        assert!(preamble.contains("drop-table"));
        assert!(preamble.contains("no dropping tables"));
        assert!(preamble.contains("ONLY the migration SQL"));
    }

    #[test]
    fn test_file_validation_rejects_unknown_field() {
        let toml = "disabledd = [\"drop-table\"]\n";
        let err = toml::from_str::<FileValidationConfig>(toml).expect_err("unknown field must error");
        assert!(err.to_string().contains("disabledd"), "{err}");
    }

    #[test]
    fn test_custom_rule_rejects_unknown_field() {
        let toml = "id = \"x\"\nrule = \"flag x\"\nlevl = \"warning\"\n";
        let err = toml::from_str::<CustomRule>(toml).expect_err("unknown field must error");
        assert!(err.to_string().contains("levl"), "{err}");
    }

    #[test]
    fn test_verdict_status_deserializes_lowercase() {
        let v: RuleVerdict =
            serde_json::from_value(serde_json::json!({ "status": "fail", "detail": "x" })).expect("parse");
        assert_eq!(v.status, VerdictStatus::Fail);
        assert_eq!(v.detail, "x");
    }
}
