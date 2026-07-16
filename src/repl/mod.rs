mod completion;
mod highlighter;
pub mod scanner;

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

use crate::{cli::Cli, error::Result, metadata::MetadataStore, output};

use completion::SqlCompleter;
use highlighter::SqlHighlighter;

pub fn create_editor(
    cli: &Cli,
    metadata: MetadataStore,
    standard_conforming_strings: Arc<AtomicBool>,
) -> Result<Reedline> {
    let history_path = cli
        .history_file
        .clone()
        .unwrap_or_else(default_history_path);
    if let Some(parent) = history_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let history_path = prepare_history_file(&history_path, cli.history_file.is_some())?;
    let history = Box::new(FileBackedHistory::with_file(10_000, history_path)?);
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
        .with_completer(Box::new(SqlCompleter::with_standard_conforming_strings(
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
        if scanner::is_complete_with_standard_conforming_strings(
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum TransactionStatus {
    #[default]
    Idle,
    Active,
    Failed,
    Unknown,
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

fn prepare_history_file(path: &Path, user_supplied: bool) -> std::io::Result<PathBuf> {
    #[cfg(unix)]
    {
        use std::io::ErrorKind;
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

        match std::fs::symlink_metadata(path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(std::io::Error::new(
                    ErrorKind::InvalidInput,
                    "history path must not be a symbolic link",
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }

        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty());
        let parent = parent.unwrap_or_else(|| Path::new("."));
        let parent = std::fs::canonicalize(parent)?;
        // Reedline later reopens the path itself, so O_NOFOLLOW on this open is
        // not sufficient unless other users also cannot replace the entry.
        validate_history_parent_chain(&parent)?;
        let file_name = path.file_name().ok_or_else(|| {
            std::io::Error::new(ErrorKind::InvalidInput, "history path has no file name")
        })?;
        // Pass Reedline the canonical parent path so a symlinked parent cannot
        // be redirected after validation.
        let path = parent.join(file_name);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(&path)?;
        let metadata = file.metadata()?;
        if !metadata.file_type().is_file() {
            return Err(std::io::Error::new(
                ErrorKind::InvalidInput,
                "history path must be a regular file",
            ));
        }
        if metadata.uid() != unsafe { libc::geteuid() } {
            return Err(std::io::Error::new(
                ErrorKind::PermissionDenied,
                "history file must be owned by the current user",
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
        Ok(path)
    }
    #[cfg(not(unix))]
    {
        OpenOptions::new().create(true).append(true).open(path)?;
        let _ = user_supplied;
        Ok(path.to_owned())
    }
}

#[cfg(unix)]
fn validate_history_parent_chain(parent: &Path) -> std::io::Result<()> {
    use std::io::ErrorKind;
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    const STICKY_BIT: u32 = 0o1000;
    let effective_uid = unsafe { libc::geteuid() };
    let mut child = parent.to_owned();
    let child_metadata = std::fs::metadata(&child)?;
    if !child_metadata.is_dir() {
        return Err(std::io::Error::new(
            ErrorKind::InvalidInput,
            "history parent must be a directory",
        ));
    }
    let child_mode = child_metadata.permissions().mode();
    if child_mode & 0o022 != 0 && child_mode & STICKY_BIT == 0 {
        return Err(std::io::Error::new(
            ErrorKind::PermissionDenied,
            "history parent must not permit replacement by another user unless it is sticky",
        ));
    }

    while let Some(ancestor) = child.parent() {
        if ancestor == child {
            break;
        }
        let ancestor_metadata = std::fs::metadata(ancestor)?;
        let mode = ancestor_metadata.permissions().mode();
        if mode & 0o022 != 0 {
            let child_metadata = std::fs::metadata(&child)?;
            if mode & STICKY_BIT == 0 || child_metadata.uid() != effective_uid {
                return Err(std::io::Error::new(
                    ErrorKind::PermissionDenied,
                    "history directory chain permits replacement by another user",
                ));
            }
        }
        child = ancestor.to_owned();
    }
    Ok(())
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
        let path = directory.path().join("history");
        prepare_history_file(&path, false).unwrap();
        assert_eq!(
            std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    #[cfg(unix)]
    #[test]
    fn rejects_replaceable_history_parents() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let parent = directory.path().join("shared");
        std::fs::create_dir(&parent).unwrap();
        std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o777)).unwrap();

        let error = prepare_history_file(&parent.join("history"), false).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);

        let nested = parent.join("private");
        std::fs::create_dir(&nested).unwrap();
        std::fs::set_permissions(&nested, std::fs::Permissions::from_mode(0o700)).unwrap();
        let error = prepare_history_file(&nested.join("history"), false).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    }

    #[cfg(unix)]
    #[test]
    fn accepts_owned_history_in_a_sticky_parent() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let parent = directory.path().join("sticky");
        std::fs::create_dir(&parent).unwrap();
        std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o1777)).unwrap();

        let path = prepare_history_file(&parent.join("history"), false).unwrap();
        assert!(path.is_file());
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

        let error = prepare_history_file(&path, false).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
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
