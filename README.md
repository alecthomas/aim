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

1. You maintain a `schema.sql` file describing your desired database schema.
2. `aim generate` asks an LLM to produce UP and DOWN migrations that transform the current state (from existing migrations) into the desired state.
3. AIM verifies the migration by replaying it against an ephemeral database and comparing the result with `schema.sql`. If verification fails, it retries with diff feedback.
4. Once verified, the migration files are written to disk.

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

AIM's verification loop means the model doesn't need to be powerful — it just needs to produce valid DDL, and AIM will catch and retry mistakes. Small, fast, cheap models work well.

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
```

All fields except `engine` and `model` have defaults. The `context` field is optional and appends extra instructions to the LLM prompt.

Global flags (`--engine`, `--model`, `--format`, `--schema`, `--migrations`, `--max-retries`) override config file values.

## Supported LLM providers

anthropic, openai, gemini, cohere, deepseek, groq, mistral, ollama, openrouter, together, xai, perplexity, and others via [rig](https://github.com/0xPlaygrounds/rig).
