# AIM - AI-assisted verifiable SQL migration generator

AIM uses an LLM to generate SQL migrations by comparing your desired schema against your current migrations. Every generated migration is verified against an ephemeral database before it's written to disk.

<p align="center">
  <img src="aim.svg" alt="AIM screencast">
</p>

## Install

```sh
curl -fsSL https://raw.githubusercontent.com/alecthomas/aim/main/install.sh | sh
```

Or with Cargo:

```sh
cargo install aim
```

## How it works

You maintain a single `schema.sql` describing your desired database schema. AIM figures out how to get there.

1. **Snapshot** — AIM creates two ephemeral databases: one by loading `schema.sql` (the desired state), and one by replaying all existing migrations (the current state). It dumps a stable, normalized DDL representation from each.
2. **Diff** — AIM compares the two DDL snapshots to determine what changed.
3. **Generate** — An LLM reads both schemas and produces UP and DOWN SQL migration statements (DOWN is skipped when `no_down` is set).
4. **Verify** — AIM applies the generated UP migration to a fresh ephemeral database and checks that the result exactly matches `schema.sql`. It then applies DOWN and checks that the original state is restored. If either check fails, AIM feeds the diff back to the LLM and retries. When `no_down` is set, only the UP migration is generated and verified.
5. **Write** — Once verified, the migration files are written to disk in your chosen format.

## Supported databases

| Engine | Specifier | Example | Requires |
|--------|-----------|---------|----------|
| SQLite | `sqlite` | `sqlite` | Built-in |
| PostgreSQL | `postgres-<version>` | `postgres-17` | Docker |
| MySQL | `mysql-<version>` | `mysql-9` | Docker |
| MariaDB | `mariadb-<version>` | `mariadb-11` | Docker |

SQLite uses temporary files for ephemeral databases. PostgreSQL, MySQL, and MariaDB each spin up a single Docker container on first use, create multiple databases within it for verification, and tear it down on exit (including Ctrl+C).

## Supported migration formats

[migrate](https://github.com/golang-migrate/migrate) (default), [goose](https://github.com/pressly/goose), [flyway](https://github.com/flyway/flyway), [sqitch](https://sqitch.org/), [sqlx](https://github.com/launchbadge/sqlx), [dbmate](https://github.com/amacneil/dbmate), [refinery](https://github.com/rust-db/refinery)

## Quick start

Set your LLM provider's API key:

```sh
export ANTHROPIC_API_KEY=sk-...   # or OPENAI_API_KEY, GEMINI_API_KEY, etc.
```

```sh
# Initialize a new project
aim init --engine sqlite

# Edit schema.sql with your desired schema, then:
aim diff                                                    # Preview what changed
aim generate --model anthropic-claude-haiku-4-5-20251001    # Generate a verified migration
```

If `model` is set in `aim.toml`, the `--model` flag can be omitted from `generate`.

## Model selection

AIM's verification loop means the model doesn't need to be powerful — it just needs to produce valid DDL, and AIM will
catch and retry mistakes. Small, fast, cheap models work well.

There's a really comprehensive set of LLM benchmarks [here](https://sql-benchmark.nicklothian.com/), which I highly
recommend checking out, but I've also tested a few manually on the included examples.

Proprietary models:

- `anthropic-claude-haiku-4-5-20251001`
- `gemini-gemini-3.1-flash-lite-preview`
- `gemini-gemini-3-flash-preview`
- `gemini-gemini-3.1-flash-lite-preview`
- `gemini-gemini-2.5-flash`

Open source models:

- `groq-openai/gpt-oss-20b`
- `groq-openai/gpt-oss-safeguard-20b`
- `groq-moonshotai/kimi-k2-instruct`
- `openrouter-z-ai/glm-5-turbo`
- `deepseek-deepseek-chat`

Larger models like `anthropic-claude-sonnet-4-6` or `openai-gpt-4o` also work but are overkill for most migrations.

## Configuration

`aim.toml`:

```toml
engine = "postgres-17"
format = "migrate"
schema = "schema.sql"
migrations = "migrations"
max_retries = 3
model = "anthropic-claude-haiku-4-5-20251001"
context = "Use IF NOT EXISTS for all CREATE TABLE statements."
no_down = false
```

All fields except `engine` and `model` have defaults. The `context` field is optional and appends extra instructions to the LLM prompt.

### Environment variables

An optional `[env]` section pre-sets environment variables before AIM runs — handy for provider credentials such as AWS Bedrock:

```toml
[env]
AWS_PROFILE = "my-bedrock-profile"
AWS_REGION = "us-east-1"
```

You can also set variables per-invocation with the repeatable `--env` flag:

```sh
aim generate --env AWS_PROFILE=my-bedrock-profile --env AWS_REGION=us-east-1
```

Precedence, highest to lowest: `--env` flags, variables already set in your environment, then `[env]` in `aim.toml`. (So a value exported in your shell overrides `aim.toml`, and `--env` overrides everything.)

Set `no_down = true` to generate migrations without a DOWN (rollback) section. Rollbacks are easy to get wrong in ways that silently destroy data (for example, an UP that adds an enum value paired with a DOWN that drops it). With `no_down`, only the UP migration is generated, verified, and written, and `validate` checks only that each UP applies cleanly through the full history.

Global flags (`--engine`, `--model`, `--format`, `--schema`, `--migrations`, `--max-retries`, `--no-down`, `--env`) override config file values.

## Schema validation

After a migration is generated and verified, AIM checks its UP statements against a set of validation rules that detect destructive schema changes. Each rule is evaluated by its own isolated LLM check (one per rule, run in parallel) that only ever sees the UP migration — the DOWN migration is exempt, since rollbacks are expected to be destructive. Each rule has a stable id and a level:

- `error` — a match fails the migration.
- `warning` — a match is reported but does not block the migration.

### Built-in rules

All built-in rules default to `warning`. Raise one to `error` (see below) to make it block migrations.

| Rule | Default level | Flags |
|------|---------------|-------|
| `drop-table` | `warning` | A statement that DROPs a table, permanently destroying all of its rows. |
| `drop-column` | `warning` | A statement that DROPs a column, permanently destroying all data in it. |
| `narrowing-type-change` | `warning` | A column type change that can truncate values, lose precision, or fail on existing data (e.g. shrinking a length, or converting text to a numeric type). |
| `remove-enum-value` | `warning` | Removing or renaming an enum value, which can orphan rows that reference it. |
| `destructive-dml` | `warning` | A TRUNCATE, or a DELETE/UPDATE without a WHERE clause, affecting every row in the table. |
| `add-not-null-without-default` | `warning` | Adding a NOT NULL column or constraint without a DEFAULT, which fails when the table already contains rows. |
| `drop-index` | `warning` | Dropping an index, which may degrade query performance. |

Run `aim rules` to print the full set of enabled rules (built-ins minus `disabled`, plus your custom rules) with their resolved levels.

Configure validation in `aim.toml`:

```toml
[validation]
disabled = ["drop-index"]              # turn off built-in rules by id

[[validation.rules]]                    # add your own rules
id = "no-cascade-delete"
level = "warning"                       # "error" (default) or "warning"
rule = "Flag foreign keys declared with ON DELETE CASCADE, since cascading deletes can silently remove rows."
```

The `rule` text is an English description of the behaviour to flag; it is injected into that rule's validator prompt verbatim. To redefine a built-in rule, add it to `disabled` and declare a custom rule with the same id.

## Supported LLM providers

anthropic, openai, gemini, cohere, deepseek, groq, mistral, ollama, openrouter, together, xai, perplexity, bedrock, and others via [rig](https://github.com/0xPlaygrounds/rig).

### AWS Bedrock

Bedrock uses the standard AWS credential chain (environment variables, shared
config/profile, or IMDS) rather than an aim-managed API key, so `aim auth` does
not apply. Configure credentials the usual way:

```sh
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=us-east-1
```

The model is selected with a `bedrock-<model-id>` specifier, where `<model-id>`
is a Bedrock model or inference-profile id:

```sh
aim generate --model bedrock-us.anthropic.claude-sonnet-4-6
```
