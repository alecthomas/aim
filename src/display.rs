use std::io::IsTerminal;

use syntect::easy::HighlightLines;
use syntect::highlighting::ThemeSet;
use syntect::parsing::SyntaxSet;
use syntect::util::{LinesWithEndings, as_24_bit_terminal_escaped};

/// Print SQL with syntax highlighting to stdout.
///
/// Falls back to plain text if stdout is not a TTY or highlighting fails.
pub fn highlight_sql(sql: &str) {
    if !std::io::stdout().is_terminal() {
        print!("{sql}");
        if !sql.ends_with('\n') {
            println!();
        }
        return;
    }

    let ss = SyntaxSet::load_defaults_newlines();
    let ts = ThemeSet::load_defaults();

    let syntax = ss
        .find_syntax_by_extension("sql")
        .unwrap_or_else(|| ss.find_syntax_plain_text());
    let theme = &ts.themes["base16-ocean.dark"];
    let mut highlighter = HighlightLines::new(syntax, theme);

    for line in LinesWithEndings::from(sql) {
        match highlighter.highlight_line(line, &ss) {
            Ok(ranges) => {
                let escaped = as_24_bit_terminal_escaped(&ranges, false);
                print!("{escaped}");
            }
            Err(_) => print!("{line}"),
        }
    }
    // Reset terminal colors.
    print!("\x1b[0m");
    println!();
}
