use std::future::Future;

use rig::agent::{HookAction, ToolCallHookAction};
use rig::completion::{CompletionModel, CompletionResponse};
use rig::message::{AssistantContent, Message};
use yansi::Paint;

/// Hook that prints agent activity to stdout.
///
/// Implements rig's `PromptHook` trait to observe tool calls and results
/// as they happen during the agent loop.
#[derive(Clone)]
pub struct Output;

impl Output {
    /// Print a status line for a phase of the migration process.
    pub fn phase(msg: &str) {
        println!("{}", msg.bold());
    }

    /// Print a success message.
    pub fn success(msg: &str) {
        println!("{} {msg}", "ok".green().bold());
    }

    /// Print a warning.
    pub fn warn(msg: &str) {
        println!("{} {msg}", "warn".yellow().bold());
    }

    /// Print an error.
    pub fn error(msg: &str) {
        println!("{} {msg}", "error".red().bold());
    }

    /// Print a schema diff (empty diffs are not printed).
    /// Lines starting with `-` are red, `+` are green, `---`/`+++` are bold.
    pub fn diff(label: &str, diff: &str) {
        if diff.is_empty() {
            return;
        }
        println!("  {} {label}:", "diff".yellow().bold());
        for line in diff.lines() {
            if line.starts_with("--- ") || line.starts_with("+++ ") {
                println!("    {}", line.bold());
            } else if line.starts_with("- ") {
                println!("    {}", line.red());
            } else if line.starts_with("+ ") {
                println!("    {}", line.green());
            } else {
                println!("    {line}");
            }
        }
    }

    /// Print a retry message.
    pub fn retry(attempt: usize, max: usize) {
        println!("{} {attempt}/{max}", "retry".yellow().bold());
    }
}

impl<M: CompletionModel> rig::agent::PromptHook<M> for Output {
    fn on_tool_call(
        &self,
        tool_name: &str,
        _tool_call_id: Option<String>,
        _internal_call_id: &str,
        args: &str,
    ) -> impl Future<Output = ToolCallHookAction> + Send {
        let summary = summarize_args(tool_name, args);
        print!("  {} {tool_name}{summary} ", "tool".cyan().bold());
        async { ToolCallHookAction::cont() }
    }

    fn on_tool_result(
        &self,
        _tool_name: &str,
        _tool_call_id: Option<String>,
        _internal_call_id: &str,
        _args: &str,
        result: &str,
    ) -> impl Future<Output = HookAction> + Send {
        let preview = truncate(result, 80);
        println!("{}", preview.dim());
        async { HookAction::cont() }
    }

    fn on_completion_response(
        &self,
        _prompt: &Message,
        response: &CompletionResponse<M::Response>,
    ) -> impl Future<Output = HookAction> + Send {
        for content in response.choice.iter() {
            if let AssistantContent::Text(text) = content {
                let msg = truncate(&text.text, 120);
                if !msg.is_empty() {
                    println!("  {} {}", "agent".magenta().bold(), msg.dim());
                }
            }
        }
        async { HookAction::cont() }
    }
}

/// Summarize tool call args for display. Shows relevant details per tool.
fn summarize_args(name: &str, args: &str) -> String {
    let field = match name {
        "submit_migration" => "description",
        _ => return String::new(),
    };

    if let Ok(v) = serde_json::from_str::<serde_json::Value>(args)
        && let Some(val) = v.get(field).and_then(|v| v.as_str())
    {
        return format!(" ({val})");
    }
    String::new()
}

/// Truncate a string to `max` chars, appending "..." if truncated.
/// Collapses all whitespace runs (including literal `\n` escape sequences) to a single space.
fn truncate(s: &str, max: usize) -> String {
    let mut collapsed = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    let mut prev_was_space = false;
    while let Some(c) = chars.next() {
        if c == '\\' && chars.peek() == Some(&'n') {
            chars.next();
            if !prev_was_space {
                collapsed.push(' ');
                prev_was_space = true;
            }
        } else if c.is_whitespace() {
            if !prev_was_space {
                collapsed.push(' ');
                prev_was_space = true;
            }
        } else {
            collapsed.push(c);
            prev_was_space = false;
        }
    }
    if collapsed.len() <= max {
        collapsed
    } else {
        format!("{}...", &collapsed[..max])
    }
}
