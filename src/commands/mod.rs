use std::{env, io::Write, process::Command};

use tempfile::NamedTempFile;

use crate::error::{AppError, Result};

pub mod catalog;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationKind {
    All,
    Table,
    View,
    MaterializedView,
    Index,
    Sequence,
}

#[derive(Debug, PartialEq, Eq)]
pub enum CatalogCommand {
    Describe {
        pattern: Option<String>,
        verbose: bool,
    },
    ListRelations {
        kind: RelationKind,
        pattern: Option<String>,
        verbose: bool,
    },
    Functions {
        pattern: Option<String>,
    },
    Schemas {
        pattern: Option<String>,
    },
    Databases {
        pattern: Option<String>,
    },
    Roles {
        pattern: Option<String>,
    },
    ConnectionInfo,
}

#[derive(Debug, PartialEq, Eq)]
pub enum SpecialCommand {
    Help,
    Quit,
    Edit(Option<String>),
    Expanded(Option<bool>),
    Timing(Option<bool>),
    Pager(Option<bool>),
    Refresh,
    Connect(String),
    Catalog(CatalogCommand),
}

/// Recognises a backslash command. `None` means the input is SQL; an `Err`
/// is an [`AppError::InvalidCommand`] describing what was wrong with it.
pub fn parse(input: &str) -> Option<Result<SpecialCommand>> {
    let input = input.trim();
    let rest = input.strip_prefix('\\')?;
    let (name, argument) = rest.split_once(char::is_whitespace).unwrap_or((rest, ""));
    let argument = argument.trim();
    let pattern = || (!argument.is_empty()).then(|| argument.to_owned());
    let verbose = name.ends_with('+');
    let relations = |kind| {
        Ok(SpecialCommand::Catalog(CatalogCommand::ListRelations {
            kind,
            pattern: pattern(),
            verbose,
        }))
    };

    Some(match name {
        "?" | "h" => no_argument(name, argument, SpecialCommand::Help),
        "q" | "quit" => no_argument(name, argument, SpecialCommand::Quit),
        "e" => Ok(SpecialCommand::Edit(pattern())),
        "x" => parse_toggle("x", argument, SpecialCommand::Expanded),
        "timing" => parse_toggle("timing", argument, SpecialCommand::Timing),
        "pager" => parse_toggle("pager", argument, SpecialCommand::Pager),
        "refresh" => no_argument(name, argument, SpecialCommand::Refresh),
        "c" | "connect" if argument.is_empty() => {
            invalid(format!("\\{name} requires a database name"))
        }
        "c" | "connect" => Ok(SpecialCommand::Connect(argument.to_owned())),
        "d" | "d+" => Ok(SpecialCommand::Catalog(CatalogCommand::Describe {
            pattern: pattern(),
            verbose,
        })),
        "dt" | "dt+" => relations(RelationKind::Table),
        "dv" | "dv+" => relations(RelationKind::View),
        "dm" | "dm+" => relations(RelationKind::MaterializedView),
        "di" | "di+" => relations(RelationKind::Index),
        "ds" | "ds+" => relations(RelationKind::Sequence),
        "df" => Ok(SpecialCommand::Catalog(CatalogCommand::Functions {
            pattern: pattern(),
        })),
        "dn" => Ok(SpecialCommand::Catalog(CatalogCommand::Schemas {
            pattern: pattern(),
        })),
        "l" => Ok(SpecialCommand::Catalog(CatalogCommand::Databases {
            pattern: pattern(),
        })),
        "du" => Ok(SpecialCommand::Catalog(CatalogCommand::Roles {
            pattern: pattern(),
        })),
        "conninfo" => no_argument(
            name,
            argument,
            SpecialCommand::Catalog(CatalogCommand::ConnectionInfo),
        ),
        _ => invalid(format!("unknown command: \\{name}. Type \\? for help.")),
    })
}

fn invalid(message: String) -> Result<SpecialCommand> {
    Err(AppError::InvalidCommand(message))
}

fn no_argument(name: &str, argument: &str, command: SpecialCommand) -> Result<SpecialCommand> {
    if argument.is_empty() {
        Ok(command)
    } else {
        invalid(format!("\\{name} does not accept arguments"))
    }
}

fn parse_toggle(
    name: &str,
    argument: &str,
    command: impl FnOnce(Option<bool>) -> SpecialCommand,
) -> Result<SpecialCommand> {
    match argument.to_ascii_lowercase().as_str() {
        "" => Ok(command(None)),
        "on" => Ok(command(Some(true))),
        "off" => Ok(command(Some(false))),
        _ => invalid(format!("\\{name} expects on or off")),
    }
}

pub fn edit_query(initial: &str) -> Result<Option<String>> {
    let mut file = NamedTempFile::new()?;
    file.write_all(initial.as_bytes())?;
    file.flush()?;

    let editor = env::var("VISUAL")
        .or_else(|_| env::var("EDITOR"))
        .unwrap_or_else(|_| {
            if cfg!(windows) {
                "notepad".into()
            } else {
                "vi".into()
            }
        });
    let mut parts = shlex::split(&editor)
        .ok_or(AppError::InvalidEditor)?
        .into_iter();
    let program = parts.next().ok_or(AppError::InvalidEditor)?;
    let status = Command::new(program)
        .args(parts)
        .arg(file.path())
        .status()?;
    if !status.success() {
        return Ok(None);
    }

    let query = std::fs::read_to_string(file.path())?;
    let query = query.trim();
    Ok((!query.is_empty()).then(|| query.to_owned()))
}

pub const HELP: &str = r#"Commands:
  \?                 Show this help
  \q                 Quit
  \e [SQL]           Edit SQL (or the last query) in $VISUAL/$EDITOR, then return it to the prompt
  \x [on|off]        Toggle expanded output
  \timing [on|off]   Toggle query timing
  \pager [on|off]    Toggle the output pager
  \refresh           Refresh completion metadata
  \c DATABASE        Connect to another database
  \d [PATTERN]       List or describe relations
  \d+ [PATTERN]      Describe relations with storage and size details
  \dt, \dv, \dm      List tables, views, or materialized views
  \di, \ds           List indexes or sequences
  \df, \dn           List functions or schemas
  \l, \du            List databases or roles
  \conninfo          Show current connection information

Patterns support * and ? wildcards and optional schema qualification.
Enter executes balanced SQL; Alt-Enter (or supported Shift-Enter) inserts a newline.
Ctrl-C clears input or cancels a query. Semicolons are optional for a single statement.
"#;

#[cfg(test)]
mod tests {
    use super::*;

    fn command(input: &str) -> SpecialCommand {
        parse(input).expect("a backslash command").unwrap()
    }

    fn rejection(input: &str) -> String {
        match parse(input).expect("a backslash command") {
            Err(AppError::InvalidCommand(message)) => message,
            other => panic!("expected an invalid command, got {other:?}"),
        }
    }

    #[test]
    fn parses_commands_and_toggles() {
        assert_eq!(command("\\q"), SpecialCommand::Quit);
        assert_eq!(command(" \\x on "), SpecialCommand::Expanded(Some(true)));
        assert_eq!(command("\\timing"), SpecialCommand::Timing(None));
        assert_eq!(command("\\refresh"), SpecialCommand::Refresh);
        assert_eq!(
            command("\\c analytics"),
            SpecialCommand::Connect("analytics".into())
        );
        assert!(parse("select 1").is_none());
    }

    #[test]
    fn rejects_malformed_commands_with_a_reason() {
        assert_eq!(rejection("\\q unexpected"), "\\q does not accept arguments");
        assert_eq!(rejection("\\pager banana"), "\\pager expects on or off");
        assert_eq!(rejection("\\c"), "\\c requires a database name");
        assert!(rejection("\\nope").starts_with("unknown command: \\nope"));
        for input in [
            "\\? unexpected",
            "\\conninfo unexpected",
            "\\refresh unexpected",
        ] {
            rejection(input);
        }
    }

    #[test]
    fn parses_catalog_commands() {
        assert_eq!(
            command("\\d+ public.user*"),
            SpecialCommand::Catalog(CatalogCommand::Describe {
                pattern: Some("public.user*".into()),
                verbose: true,
            })
        );
        assert_eq!(
            command("\\dt"),
            SpecialCommand::Catalog(CatalogCommand::ListRelations {
                kind: RelationKind::Table,
                pattern: None,
                verbose: false,
            })
        );
        assert_eq!(
            command("\\conninfo"),
            SpecialCommand::Catalog(CatalogCommand::ConnectionInfo)
        );
    }

    #[test]
    fn edit_command_accepts_seed_sql() {
        assert_eq!(
            command("\\e select 1;"),
            SpecialCommand::Edit(Some("select 1;".into()))
        );
    }
}
