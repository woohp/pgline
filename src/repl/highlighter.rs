use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use nu_ansi_term::{Color, Style};
use reedline::{Highlighter, StyledText};

use super::scanner::{self, TokenKind};
use crate::output;

pub struct SqlHighlighter {
    enabled: bool,
    standard_conforming_strings: Arc<AtomicBool>,
}

impl SqlHighlighter {
    pub fn new(enabled: bool, standard_conforming_strings: Arc<AtomicBool>) -> Self {
        Self {
            enabled,
            standard_conforming_strings,
        }
    }
}

impl Highlighter for SqlHighlighter {
    fn highlight(&self, line: &str, _cursor: usize) -> StyledText {
        if !self.enabled {
            let mut text = StyledText::new();
            text.push((Style::new(), output::safe_editor_text(line)));
            return text;
        }

        let mut output = StyledText::new();
        let mut position = 0;
        for token in scanner::scan_with_standard_conforming_strings(
            line,
            self.standard_conforming_strings.load(Ordering::Relaxed),
        )
        .tokens
        {
            if position < token.start {
                output.push((
                    Style::new(),
                    output::safe_editor_text(&line[position..token.start]),
                ));
            }
            let style = match token.kind {
                TokenKind::Keyword => Style::new().fg(Color::Green).bold(),
                TokenKind::String => Style::new().fg(Color::Yellow),
                TokenKind::Number => Style::new().fg(Color::Purple),
                TokenKind::Comment => Style::new().fg(Color::DarkGray).italic(),
                TokenKind::Word | TokenKind::Symbol => Style::new(),
            };
            output.push((
                style,
                output::safe_editor_text(&line[token.start..token.end]),
            ));
            position = token.end;
        }
        if position < line.len() {
            output.push((Style::new(), output::safe_editor_text(&line[position..])));
        }
        output
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn follows_live_string_conformance_setting() {
        let setting = Arc::new(AtomicBool::new(true));
        let highlighter = SqlHighlighter::new(true, Arc::clone(&setting));
        let line = r"SELECT 'before\'; after'";
        let enabled = highlighter.highlight(line, line.len()).buffer;
        setting.store(false, Ordering::Relaxed);
        let disabled = highlighter.highlight(line, line.len()).buffer;
        assert_ne!(
            enabled.iter().map(|(_, text)| text).collect::<Vec<_>>(),
            disabled.iter().map(|(_, text)| text).collect::<Vec<_>>()
        );
    }

    #[test]
    fn highlighted_text_preserves_raw_byte_offsets() {
        let highlighter = SqlHighlighter::new(true, Arc::new(AtomicBool::new(true)));
        let line = "SELECT \"bad\x1b😀\"";
        let highlighted = highlighter.highlight(line, line.len());
        let rendered = highlighted.raw_string();
        assert_eq!(rendered.len(), line.len());
        assert!(!rendered.contains('\x1b'));
        assert!(rendered.contains("bad?😀"));

        let prompt =
            crate::repl::SqlPrompt::new("user", "host", "db", crate::repl::TransactionStatus::Idle);
        let insertion = line.find('😀').unwrap();
        let (left, right) =
            highlighted.render_around_insertion_point(insertion, &prompt, false, None);
        assert!(!left.is_empty());
        assert!(right.starts_with("😀"));
    }
}
