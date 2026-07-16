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
    Invalid(String),
    Unknown(String),
}

pub fn parse(input: &str) -> Option<SpecialCommand> {
    let input = input.trim();
    let rest = input.strip_prefix('\\')?;
    let (name, argument) = rest.split_once(char::is_whitespace).unwrap_or((rest, ""));
    let argument = argument.trim();
    let pattern = || (!argument.is_empty()).then(|| argument.to_owned());

    Some(match name {
        "?" | "h" => no_argument(name, argument, SpecialCommand::Help),
        "q" | "quit" => no_argument(name, argument, SpecialCommand::Quit),
        "e" => SpecialCommand::Edit(pattern()),
        "x" => parse_toggle("x", argument, SpecialCommand::Expanded),
        "timing" => parse_toggle("timing", argument, SpecialCommand::Timing),
        "pager" => parse_toggle("pager", argument, SpecialCommand::Pager),
        "refresh" => no_argument(name, argument, SpecialCommand::Refresh),
        "c" | "connect" => required_argument(name, argument, SpecialCommand::Connect),
        "d" | "d+" => SpecialCommand::Catalog(CatalogCommand::Describe {
            pattern: pattern(),
            verbose: name.ends_with('+'),
        }),
        "dt" | "dt+" => catalog_relations(RelationKind::Table, pattern(), name),
        "dv" | "dv+" => catalog_relations(RelationKind::View, pattern(), name),
        "dm" | "dm+" => catalog_relations(RelationKind::MaterializedView, pattern(), name),
        "di" | "di+" => catalog_relations(RelationKind::Index, pattern(), name),
        "ds" | "ds+" => catalog_relations(RelationKind::Sequence, pattern(), name),
        "df" => SpecialCommand::Catalog(CatalogCommand::Functions { pattern: pattern() }),
        "dn" => SpecialCommand::Catalog(CatalogCommand::Schemas { pattern: pattern() }),
        "l" => SpecialCommand::Catalog(CatalogCommand::Databases { pattern: pattern() }),
        "du" => SpecialCommand::Catalog(CatalogCommand::Roles { pattern: pattern() }),
        "conninfo" => no_argument(
            name,
            argument,
            SpecialCommand::Catalog(CatalogCommand::ConnectionInfo),
        ),
        _ => SpecialCommand::Unknown(name.to_owned()),
    })
}

fn no_argument(name: &str, argument: &str, command: SpecialCommand) -> SpecialCommand {
    if argument.is_empty() {
        command
    } else {
        SpecialCommand::Invalid(format!("\\{name} does not accept arguments"))
    }
}

fn required_argument(
    name: &str,
    argument: &str,
    command: impl FnOnce(String) -> SpecialCommand,
) -> SpecialCommand {
    if argument.is_empty() {
        SpecialCommand::Invalid(format!("\\{name} requires a database name"))
    } else {
        command(argument.to_owned())
    }
}

fn parse_toggle(
    name: &str,
    argument: &str,
    command: impl FnOnce(Option<bool>) -> SpecialCommand,
) -> SpecialCommand {
    match argument.to_ascii_lowercase().as_str() {
        "" => command(None),
        "on" => command(Some(true)),
        "off" => command(Some(false)),
        _ => SpecialCommand::Invalid(format!("\\{name} expects on or off")),
    }
}

fn catalog_relations(kind: RelationKind, pattern: Option<String>, name: &str) -> SpecialCommand {
    SpecialCommand::Catalog(CatalogCommand::ListRelations {
        kind,
        pattern,
        verbose: name.ends_with('+'),
    })
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

    #[test]
    fn parses_commands_and_toggles() {
        assert_eq!(parse("\\q"), Some(SpecialCommand::Quit));
        assert_eq!(
            parse("\\q unexpected"),
            Some(SpecialCommand::Invalid(
                "\\q does not accept arguments".into()
            ))
        );
        assert!(matches!(
            parse("\\? unexpected"),
            Some(SpecialCommand::Invalid(_))
        ));
        assert!(matches!(
            parse("\\conninfo unexpected"),
            Some(SpecialCommand::Invalid(_))
        ));
        assert_eq!(
            parse(" \\x on "),
            Some(SpecialCommand::Expanded(Some(true)))
        );
        assert_eq!(parse("\\timing"), Some(SpecialCommand::Timing(None)));
        assert_eq!(parse("\\refresh"), Some(SpecialCommand::Refresh));
        assert_eq!(
            parse("\\c analytics"),
            Some(SpecialCommand::Connect("analytics".into()))
        );
        assert!(matches!(parse("\\c"), Some(SpecialCommand::Invalid(_))));
        assert!(matches!(
            parse("\\refresh unexpected"),
            Some(SpecialCommand::Invalid(_))
        ));
        assert_eq!(
            parse("\\pager banana"),
            Some(SpecialCommand::Invalid("\\pager expects on or off".into()))
        );
        assert_eq!(parse("select 1"), None);
    }

    #[test]
    fn parses_catalog_commands() {
        assert_eq!(
            parse("\\d+ public.user*"),
            Some(SpecialCommand::Catalog(CatalogCommand::Describe {
                pattern: Some("public.user*".into()),
                verbose: true,
            }))
        );
        assert_eq!(
            parse("\\dt"),
            Some(SpecialCommand::Catalog(CatalogCommand::ListRelations {
                kind: RelationKind::Table,
                pattern: None,
                verbose: false,
            }))
        );
        assert_eq!(
            parse("\\conninfo"),
            Some(SpecialCommand::Catalog(CatalogCommand::ConnectionInfo))
        );
    }

    #[test]
    fn edit_command_accepts_seed_sql() {
        assert_eq!(
            parse("\\e select 1;"),
            Some(SpecialCommand::Edit(Some("select 1;".into())))
        );
    }
}
