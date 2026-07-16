use crate::{error::AppError, repl::scanner};

pub(crate) fn unsupported_copy_error(
    sql: &str,
    standard_conforming_strings: bool,
) -> Option<AppError> {
    let scan = scanner::scan_with_standard_conforming_strings(sql, standard_conforming_strings);
    let mut state = CopyScanState::default();

    for token in scan.tokens {
        let text = &sql[token.start..token.end];
        if token.kind == scanner::TokenKind::Symbol {
            state.observe_symbol(text);
            continue;
        }
        if !matches!(
            token.kind,
            scanner::TokenKind::Keyword | scanner::TokenKind::Word
        ) || state.parenthesis_depth != 0
        {
            continue;
        }
        if let Some(error) = state.observe_word(&text.to_ascii_uppercase()) {
            return Some(error);
        }
    }
    None
}

struct CopyScanState {
    parenthesis_depth: usize,
    atomic_depth: usize,
    case_depth: usize,
    statement_start: bool,
    copy_statement: bool,
    pending_from: Option<bool>,
    previous_was_begin: bool,
}

impl Default for CopyScanState {
    fn default() -> Self {
        Self {
            parenthesis_depth: 0,
            atomic_depth: 0,
            case_depth: 0,
            statement_start: true,
            copy_statement: false,
            pending_from: None,
            previous_was_begin: false,
        }
    }
}

impl CopyScanState {
    fn observe_symbol(&mut self, symbol: &str) {
        match symbol {
            "(" => self.parenthesis_depth += 1,
            ")" => self.parenthesis_depth = self.parenthesis_depth.saturating_sub(1),
            ";" if self.parenthesis_depth == 0 && self.atomic_depth == 0 => {
                self.statement_start = true;
                self.copy_statement = false;
                self.pending_from = None;
                self.previous_was_begin = false;
            }
            _ => {}
        }
    }

    fn observe_word(&mut self, word: &str) -> Option<AppError> {
        if self.previous_was_begin && word == "ATOMIC" {
            self.atomic_depth += 1;
            self.previous_was_begin = false;
            self.statement_start = false;
            return None;
        }
        if self.atomic_depth != 0 {
            self.observe_atomic_word(word);
            return None;
        }
        let error = if self.statement_start {
            self.copy_statement = word == "COPY";
            self.statement_start = false;
            None
        } else if self.copy_statement {
            self.observe_copy_word(word)
        } else {
            None
        };
        self.previous_was_begin = word == "BEGIN";
        error
    }

    fn observe_atomic_word(&mut self, word: &str) {
        if word == "CASE" {
            self.case_depth += 1;
        } else if word == "END" && self.case_depth != 0 {
            self.case_depth -= 1;
        } else if word == "END" {
            self.atomic_depth -= 1;
        }
        self.previous_was_begin = word == "BEGIN";
    }

    fn observe_copy_word(&mut self, word: &str) -> Option<AppError> {
        if let Some(from) = self.pending_from.take() {
            if from && word == "STDIN" {
                return Some(AppError::Unsupported(
                    "COPY FROM STDIN is not implemented; no statements were executed".into(),
                ));
            }
            if !from && word == "STDOUT" {
                return Some(AppError::Unsupported(
                    "COPY TO STDOUT is not implemented; no statements were executed".into(),
                ));
            }
        }
        if word == "FROM" || word == "TO" {
            self.pending_from = Some(word == "FROM");
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_only_client_side_copy_protocol_forms() {
        for sql in [
            "COPY items FROM STDIN",
            "SELECT 1; COPY (SELECT 1) TO STDOUT",
            "CREATE FUNCTION f() RETURNS int LANGUAGE SQL BEGIN ATOMIC SELECT 1; END; COPY (SELECT 1) TO STDOUT",
        ] {
            assert!(
                matches!(
                    unsupported_copy_error(sql, true),
                    Some(AppError::Unsupported(_))
                ),
                "{sql}"
            );
        }
        for sql in [
            "COPY items TO '/tmp/items'",
            "COPY (SELECT * FROM stdin) TO '/tmp/output'",
            "SELECT 'COPY items FROM STDIN'",
        ] {
            assert!(unsupported_copy_error(sql, true).is_none(), "{sql}");
        }
    }
}
