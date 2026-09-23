mod completion;
mod highlighter;

use std::{
    borrow::Cow,
    env,
    fs::OpenOptions,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use nu_ansi_term::{Color, Style};
use reedline::{
    ColumnarMenu, DefaultHinter, Emacs, FileBackedHistory, Hinter, History, MenuBuilder, Prompt,
    PromptEditMode, PromptHistorySearch, PromptHistorySearchStatus, Reedline, ReedlineMenu,
    ValidationResult, Validator, default_emacs_keybindings,
};

use crate::{
    cli::Cli, error::Result, metadata::MetadataStore, output, scanner,
    transaction::TransactionStatus,
};

use completion::SqlCompleter;
use highlighter::SqlHighlighter;

pub fn create_editor(
    cli: &Cli,
    metadata: MetadataStore,
    standard_conforming_strings: Arc<AtomicBool>,
) -> Result<Reedline> {
    let history = history(cli);
    let menu = ColumnarMenu::default()
        .with_name("completion_menu")
        .with_columns(4);
    let mut keybindings = default_emacs_keybindings();
    keybindings.add_binding(
        reedline::KeyModifiers::NONE,
        reedline::KeyCode::Tab,
        reedline::ReedlineEvent::UntilFound(vec![
            reedline::ReedlineEvent::Menu("completion_menu".into()),
            reedline::ReedlineEvent::MenuNext,
        ]),
    );
    for modifiers in [reedline::KeyModifiers::ALT, reedline::KeyModifiers::SHIFT] {
        keybindings.add_binding(
            modifiers,
            reedline::KeyCode::Enter,
            reedline::ReedlineEvent::Edit(vec![reedline::EditCommand::InsertNewline]),
        );
    }

    Ok(Reedline::create()
        .with_history(history)
        .with_validator(Box::new(SqlValidator {
            standard_conforming_strings: Arc::clone(&standard_conforming_strings),
        }))
        .with_highlighter(Box::new(SqlHighlighter::new(
            !cli.no_color,
            Arc::clone(&standard_conforming_strings),
        )))
        .with_completer(Box::new(SqlCompleter::new(
            metadata,
            standard_conforming_strings,
        )))
        .with_hinter(Box::new(SafeHinter::default()))
        .with_menu(ReedlineMenu::EngineCompleter(Box::new(menu)))
        .with_edit_mode(Box::new(Emacs::new(keybindings))))
}

struct SafeHinter {
    inner: DefaultHinter,
    style: Style,
}

impl Default for SafeHinter {
    fn default() -> Self {
        Self {
            inner: DefaultHinter::default(),
            style: Style::new().fg(Color::LightGray),
        }
    }
}

impl Hinter for SafeHinter {
    fn handle(
        &mut self,
        line: &str,
        pos: usize,
        history: &dyn History,
        use_ansi_coloring: bool,
        cwd: &str,
    ) -> String {
        let hint = self.inner.handle(line, pos, history, false, cwd);
        let hint = output::safe_terminal_text(&hint);
        if use_ansi_coloring && !hint.is_empty() {
            self.style.paint(hint).to_string()
        } else {
            hint
        }
    }

    fn complete_hint(&self) -> String {
        self.inner.complete_hint()
    }

    fn next_hint_token(&self) -> String {
        self.inner.next_hint_token()
    }
}

pub fn replace_buffer(editor: &mut Reedline, contents: String) {
    editor.run_edit_commands(&[
        reedline::EditCommand::Clear,
        reedline::EditCommand::InsertString(contents),
    ]);
}

pub struct SqlValidator {
    standard_conforming_strings: Arc<AtomicBool>,
}

impl Validator for SqlValidator {
    fn validate(&self, line: &str) -> ValidationResult {
        if scanner::is_complete(
            line,
            self.standard_conforming_strings.load(Ordering::Relaxed),
        ) {
            ValidationResult::Complete
        } else {
            ValidationResult::Incomplete
        }
    }
}

#[derive(Clone)]
pub struct SqlPrompt {
    left: String,
}

impl SqlPrompt {
    pub fn new(user: &str, host: &str, database: &str, transaction: TransactionStatus) -> Self {
        let marker = match transaction {
            TransactionStatus::Idle => "",
            TransactionStatus::Active => "*",
            TransactionStatus::Failed => "!",
            TransactionStatus::Unknown => "?",
        };
        Self {
            left: format!(
                "{}@{}:{}{marker}",
                output::safe_terminal_text(user),
                output::safe_terminal_text(host),
                output::safe_terminal_text(database)
            ),
        }
    }
}

impl Prompt for SqlPrompt {
    fn render_prompt_left(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.left)
    }

    fn render_prompt_right(&self) -> Cow<'_, str> {
        Cow::Borrowed("")
    }

    fn render_prompt_indicator(&self, _mode: PromptEditMode) -> Cow<'_, str> {
        Cow::Borrowed("> ")
    }

    fn render_prompt_multiline_indicator(&self) -> Cow<'_, str> {
        Cow::Borrowed(".. ")
    }

    fn render_prompt_history_search_indicator(&self, search: PromptHistorySearch) -> Cow<'_, str> {
        let failing = matches!(search.status, PromptHistorySearchStatus::Failing);
        Cow::Owned(format!(
            "({}reverse-search: {}) ",
            if failing { "failed " } else { "" },
            output::safe_terminal_text(&search.term)
        ))
    }
}

const HISTORY_CAPACITY: usize = 10_000;

/// History is a convenience, so a history file that cannot be used safely
/// costs this session its saved history rather than refusing to start.
fn history(cli: &Cli) -> Box<dyn History> {
    let path = cli
        .history_file
        .clone()
        .unwrap_or_else(default_history_path);
    let file_history = prepare_history_file(&path, cli.history_file.is_some())
        .map_err(|error| error.to_string())
        .and_then(|path| {
            FileBackedHistory::with_file(HISTORY_CAPACITY, path).map_err(|error| error.to_string())
        });
    match file_history {
        Ok(history) => Box::new(history),
        Err(error) => {
            eprintln!(
                "warning: history file {} is unavailable ({}); this session's history will not be saved",
                output::safe_terminal_text(&path.display().to_string()),
                output::safe_terminal_text(&error)
            );
            Box::new(
                FileBackedHistory::new(HISTORY_CAPACITY)
                    .expect("in-memory history has no file to fail on"),
            )
        }
    }
}

/// Creates the history file if needed and checks that it is private to the
/// current user. Symbolic links are refused because the file is reopened by
/// path on every save.
fn prepare_history_file(path: &Path, user_supplied: bool) -> std::io::Result<PathBuf> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)?;
    }
    #[cfg(unix)]
    {
        use std::io::ErrorKind;
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path)?;
        let metadata = file.metadata()?;
        if !metadata.file_type().is_file() {
            return Err(std::io::Error::new(
                ErrorKind::InvalidInput,
                "not a regular file",
            ));
        }
        if metadata.uid() != unsafe { libc::geteuid() } {
            return Err(std::io::Error::new(
                ErrorKind::PermissionDenied,
                "not owned by the current user",
            ));
        }
        if metadata.permissions().mode() & 0o077 != 0 {
            if user_supplied {
                eprintln!(
                    "warning: history file {} is accessible by other users",
                    output::safe_terminal_text(&path.display().to_string())
                );
            } else {
                file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
            }
        }
    }
    #[cfg(not(unix))]
    {
        OpenOptions::new().create(true).append(true).open(path)?;
        let _ = user_supplied;
    }
    Ok(path.to_owned())
}

fn default_history_path() -> PathBuf {
    if let Some(path) = env::var_os("XDG_DATA_HOME") {
        return PathBuf::from(path).join("pgline/history");
    }
    if let Some(home) = env::var_os("HOME") {
        return PathBuf::from(home).join(".local/share/pgline/history");
    }
    PathBuf::from(".pgline-history")
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn history_hints_sanitize_content_before_applying_trusted_style() {
        let mut history = FileBackedHistory::default();
        history
            .save(reedline::HistoryItem::from_command_line(
                "select \x1b]52;c;payload\x07",
            ))
            .unwrap();
        let mut hinter = SafeHinter::default();
        let hint = hinter.handle("select ", 7, &history, true, "");
        assert!(!hint.contains("\x1b]52"));
        assert!(!hint.contains('\x07'));
        assert!(hint.contains(r"\x1b]52;c;payload\x07"));
        assert_eq!(hint.matches('\x1b').count(), 2);
    }

    #[test]
    fn reverse_search_prompt_sanitizes_the_search_term() {
        let prompt = SqlPrompt::new("u", "h", "d", TransactionStatus::Idle);
        let rendered = prompt.render_prompt_history_search_indicator(PromptHistorySearch::new(
            PromptHistorySearchStatus::Passing,
            "bad\x1b]52;c;payload\x07".into(),
        ));
        assert!(!rendered.contains('\x1b'));
        assert!(!rendered.contains('\x07'));
        assert!(rendered.contains(r"\x1b]52;c;payload\x07"));
    }

    #[cfg(unix)]
    #[test]
    fn creates_private_default_history() {
        use std::os::unix::fs::PermissionsExt;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("pgline").join("history");
        prepare_history_file(&path, false).unwrap();
        assert_eq!(
            std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    #[test]
    fn unusable_history_paths_fall_back_to_in_memory_history() {
        let directory = tempfile::tempdir().unwrap();
        let blocker = directory.path().join("not-a-directory");
        std::fs::write(&blocker, "").unwrap();
        let cli = Cli::try_parse_from([
            "pgline",
            "--history-file",
            blocker.join("history").to_str().unwrap(),
        ])
        .unwrap();

        let mut history = history(&cli);

        history
            .save(reedline::HistoryItem::from_command_line("select 1"))
            .expect("in-memory history accepts entries");
        assert!(!blocker.join("history").exists());
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symbolic_link_history_paths() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("target");
        std::fs::write(&target, "unchanged").unwrap();
        let path = directory.path().join("history");
        symlink(&target, &path).unwrap();

        prepare_history_file(&path, false).unwrap_err();
        assert_eq!(std::fs::read_to_string(target).unwrap(), "unchanged");
    }

    #[test]
    fn replaces_the_current_editor_buffer() {
        let mut editor = Reedline::create();
        editor.run_edit_commands(&[reedline::EditCommand::InsertString("\\e".into())]);
        replace_buffer(&mut editor, "select 1".into());
        assert_eq!(editor.current_buffer_contents(), "select 1");
        assert_eq!(editor.current_insertion_point(), "select 1".len());
    }

    #[test]
    fn validator_accepts_balanced_sql_without_semicolon() {
        let validator = SqlValidator {
            standard_conforming_strings: Arc::new(AtomicBool::new(true)),
        };
        assert!(matches!(
            validator.validate("select 1"),
            ValidationResult::Complete
        ));
        assert!(matches!(
            validator.validate("select ('unfinished'"),
            ValidationResult::Incomplete
        ));
        assert!(matches!(
            validator.validate("select 1)"),
            ValidationResult::Complete
        ));
    }

    #[test]
    fn prompt_marks_transaction_state() {
        assert_eq!(
            SqlPrompt::new("u", "h", "d", TransactionStatus::Active).render_prompt_left(),
            "u@h:d*"
        );
        assert_eq!(
            SqlPrompt::new("u", "h", "d", TransactionStatus::Failed).render_prompt_left(),
            "u@h:d!"
        );
        assert_eq!(
            SqlPrompt::new("u", "h", "d", TransactionStatus::Unknown).render_prompt_left(),
            "u@h:d?"
        );
    }
}
