use std::future::Future;
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use rig::agent::{HookAction, ToolCallHookAction};
use rig::completion::{CompletionModel, CompletionResponse};
use rig::message::{AssistantContent, Message};
use yansi::Paint;

/// Current time in milliseconds (unix epoch).
fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Timestamp of the last hook activity. The spinner thread reads this to
/// show elapsed time since the last output, not since it started.
static LAST_ACTIVITY: AtomicU64 = AtomicU64::new(0);

/// Record that output activity just happened (resets the spinner timer).
fn touch_activity() {
    LAST_ACTIVITY.store(now_millis(), Ordering::Relaxed);
}

/// Erase the current line on stdout (used to clear spinner output).
///
/// Acquires the stdout lock to avoid interleaving with other output.
fn clear_line() {
    let stdout = std::io::stdout();
    let mut handle = stdout.lock();
    let _ = handle.write_all(b"\r\x1b[2K");
    let _ = handle.flush();
}

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

    /// Print conversation history size before a retry.
    ///
    /// Helps diagnose slow retries caused by large accumulated history
    /// (tool results containing full schema dumps, prior attempts, etc.).
    pub fn history_size(messages: &[Message]) {
        let total_chars: usize = messages.iter().map(|m| format!("{m:?}").len()).sum();
        let approx_tokens = total_chars / 4;
        println!(
            "  {} {} messages, ~{} tokens",
            "history".dim(),
            messages.len(),
            approx_tokens
        );
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
        clear_line();
        touch_activity();
        let summary = summarize_args(tool_name, args);
        print!("  {} {tool_name}{summary} ", "tool".cyan().bold());
        let _ = std::io::stdout().flush();
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
        touch_activity();
        let preview = truncate(result, 80);
        println!("{}", preview.dim());
        async { HookAction::cont() }
    }

    fn on_completion_response(
        &self,
        _prompt: &Message,
        response: &CompletionResponse<M::Response>,
    ) -> impl Future<Output = HookAction> + Send {
        clear_line();
        touch_activity();
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

/// A background spinner that shows elapsed seconds since last hook activity.
///
/// Displays `"  waiting Xs..."` on stdout after 5s of silence, updating
/// every second. The timer resets whenever a hook fires. The spinner
/// erases its own line before writing, and hooks erase it before printing
/// their own output, so there is no interleaving.
pub struct Spinner {
    stop: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl Spinner {
    /// Start the spinner. Records the current time as the baseline.
    pub fn start() -> Self {
        touch_activity();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_clone = Arc::clone(&stop);
        let handle = std::thread::spawn(move || {
            let mut showing = false;
            while !stop_clone.load(Ordering::Relaxed) {
                std::thread::sleep(std::time::Duration::from_secs(1));
                if stop_clone.load(Ordering::Relaxed) {
                    break;
                }
                let last = LAST_ACTIVITY.load(Ordering::Relaxed);
                let idle_secs = now_millis().saturating_sub(last) / 1000;
                if idle_secs >= 5 {
                    let stdout = std::io::stdout();
                    let mut handle = stdout.lock();
                    let msg = format!("  {} {idle_secs}s...", "waiting".dim());
                    let _ = write!(handle, "\r\x1b[2K{msg}");
                    let _ = handle.flush();
                    showing = true;
                } else if showing {
                    clear_line();
                    showing = false;
                }
            }
            if showing {
                clear_line();
            }
        });
        Self {
            stop,
            handle: Some(handle),
        }
    }

    /// Stop the spinner and clear its output.
    pub fn stop(mut self) {
        self.cancel();
    }

    fn cancel(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

impl Drop for Spinner {
    fn drop(&mut self) {
        self.cancel();
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
