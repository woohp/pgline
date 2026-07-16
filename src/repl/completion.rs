use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use reedline::{Completer, Span, Suggestion};

use crate::{
    metadata::{Metadata, MetadataStore},
    output,
};

use super::scanner::{self, TokenKind};

const KEYWORDS: &[&str] = &[
    "ALTER",
    "ANALYZE",
    "BEGIN",
    "COMMIT",
    "CREATE",
    "DELETE",
    "DROP",
    "EXPLAIN",
    "FROM",
    "GRANT",
    "GROUP BY",
    "HAVING",
    "INSERT INTO",
    "JOIN",
    "LIMIT",
    "ORDER BY",
    "RETURNING",
    "ROLLBACK",
    "SELECT",
    "SET",
    "TRUNCATE",
    "UNION",
    "UPDATE",
    "VALUES",
    "WHERE",
    "WITH",
];

const COMMANDS: &[(&str, &str)] = &[
    ("\\?", "show help"),
    ("\\e", "edit the last query, then return it to the prompt"),
    ("\\q", "quit"),
    ("\\x", "toggle expanded output"),
    ("\\timing", "toggle query timing"),
    ("\\pager", "toggle pager"),
    ("\\refresh", "refresh completion metadata"),
    ("\\c", "connect to another database"),
    ("\\d", "list or describe relations"),
    ("\\d+", "describe relations with storage details"),
    ("\\dt", "list tables"),
    ("\\dv", "list views"),
    ("\\dm", "list materialized views"),
    ("\\di", "list indexes"),
    ("\\ds", "list sequences"),
    ("\\df", "list functions"),
    ("\\dn", "list schemas"),
    ("\\l", "list databases"),
    ("\\du", "list roles"),
    ("\\conninfo", "show connection information"),
];

#[derive(Clone)]
pub struct SqlCompleter {
    metadata: MetadataStore,
    standard_conforming_strings: Arc<AtomicBool>,
}

impl SqlCompleter {
    #[cfg(test)]
    fn new(metadata: Metadata) -> Self {
        let store = MetadataStore::default();
        store.replace(metadata);
        Self::with_standard_conforming_strings(store, Arc::new(AtomicBool::new(true)))
    }

    pub fn with_standard_conforming_strings(
        metadata: MetadataStore,
        standard_conforming_strings: Arc<AtomicBool>,
    ) -> Self {
        Self {
            metadata,
            standard_conforming_strings,
        }
    }

    fn candidates(&self, line: &str, word_start: usize) -> Vec<(String, Option<String>)> {
        self.metadata
            .with_current(|metadata| self.metadata_candidates(metadata, line, word_start))
    }

    fn metadata_candidates(
        &self,
        metadata: &Metadata,
        line: &str,
        word_start: usize,
    ) -> Vec<(String, Option<String>)> {
        if line.trim_start().starts_with('\\') {
            return COMMANDS
                .iter()
                .map(|(command, help)| ((*command).into(), Some((*help).into())))
                .collect();
        }

        let before = &line[..word_start];
        let standard_conforming_strings = self.standard_conforming_strings.load(Ordering::Relaxed);
        if let Some(before_dot) = before.strip_suffix('.') {
            if let Some(relations) =
                qualified_relations(metadata, before_dot, standard_conforming_strings)
            {
                return relations
                    .into_iter()
                    .map(|value| (value, Some("relation".into())))
                    .collect();
            }
            if let Some(columns) =
                qualified_columns(metadata, before_dot, standard_conforming_strings)
            {
                return columns
                    .iter()
                    .cloned()
                    .map(|value| (value, Some("column".into())))
                    .collect();
            }
        }

        let scan =
            scanner::scan_with_standard_conforming_strings(before, standard_conforming_strings);
        let previous = scan
            .tokens
            .iter()
            .rev()
            .find(|token| token.kind != TokenKind::Comment)
            .map(|token| before[token.start..token.end].to_ascii_uppercase());

        let mut values = BTreeSet::new();
        let relation_context = previous.as_deref().is_some_and(is_relation_context);
        if relation_context {
            values.extend(
                metadata
                    .relations
                    .iter()
                    .cloned()
                    .map(|value| (value, Some("relation".into()))),
            );
            values.extend(
                metadata
                    .schemas
                    .iter()
                    .cloned()
                    .map(|value| (value, Some("schema".into()))),
            );
        } else {
            values.extend(
                metadata
                    .columns
                    .iter()
                    .cloned()
                    .map(|value| (value, Some("column".into()))),
            );
            values.extend(
                KEYWORDS
                    .iter()
                    .map(|value| ((*value).into(), Some("keyword".into()))),
            );
        }
        values.into_iter().collect()
    }
}

fn qualified_relations(
    metadata: &Metadata,
    before_dot: &str,
    standard_conforming_strings: bool,
) -> Option<Vec<String>> {
    let scan =
        scanner::scan_with_standard_conforming_strings(before_dot, standard_conforming_strings);
    let schema_token = scan.tokens.last()?;
    if !matches!(schema_token.kind, TokenKind::Word | TokenKind::Keyword) {
        return None;
    }
    let previous = scan.tokens[..scan.tokens.len() - 1]
        .iter()
        .rev()
        .find(|token| token.kind != TokenKind::Comment)?;
    if !is_relation_context(
        before_dot[previous.start..previous.end]
            .to_ascii_uppercase()
            .as_str(),
    ) {
        return None;
    }
    let schema = identifier_value(&before_dot[schema_token.start..schema_token.end])?;
    Some(
        metadata
            .relations
            .iter()
            .filter_map(|relation| {
                let components = identifier_components(relation)?;
                (components.len() == 2 && components[0] == schema)
                    .then(|| last_identifier_component(relation).to_owned())
            })
            .collect(),
    )
}

fn is_relation_context(word: &str) -> bool {
    matches!(word, "FROM" | "JOIN" | "UPDATE" | "INTO" | "TABLE")
}

fn qualified_columns<'a>(
    metadata: &'a Metadata,
    before_dot: &str,
    standard_conforming_strings: bool,
) -> Option<&'a [String]> {
    // Compare PostgreSQL identifier values, not their spelling: users and "users"
    // are equivalent, while users and "Users" are not.
    let input = trailing_identifier_components(before_dot, standard_conforming_strings)?;
    metadata
        .relation_columns
        .iter()
        .find(|(qualifier, _)| {
            identifier_components(qualifier).as_deref() == Some(input.as_slice())
        })
        .map(|(_, columns)| columns.as_slice())
}

fn trailing_identifier_components(
    input: &str,
    standard_conforming_strings: bool,
) -> Option<Vec<String>> {
    let scan = scanner::scan_with_standard_conforming_strings(input, standard_conforming_strings);
    identifier_components_from_tokens(input, &scan.tokens, false)
}

fn identifier_components(input: &str) -> Option<Vec<String>> {
    let scan = scanner::scan(input);
    identifier_components_from_tokens(input, &scan.tokens, true)
}

fn identifier_components_from_tokens(
    input: &str,
    tokens: &[scanner::Token],
    require_all: bool,
) -> Option<Vec<String>> {
    let mut index = tokens.len();
    let mut components = Vec::new();
    loop {
        let token = tokens.get(index.checked_sub(1)?)?;
        if !matches!(token.kind, TokenKind::Word | TokenKind::Keyword) {
            return None;
        }
        components.push(identifier_value(&input[token.start..token.end])?);
        index -= 1;
        let Some(dot) = index.checked_sub(1).and_then(|dot| tokens.get(dot)) else {
            break;
        };
        if &input[dot.start..dot.end] != "." {
            break;
        }
        index -= 1;
    }
    if require_all && index != 0 {
        return None;
    }
    components.reverse();
    Some(components)
}

fn identifier_value(value: &str) -> Option<String> {
    if let Some(quoted) = value.strip_prefix('"') {
        return quoted
            .strip_suffix('"')
            .map(|value| value.replace("\"\"", "\""));
    }
    Some(value.to_lowercase())
}

fn completion_matches(value: &str, prefix_lower: &str) -> bool {
    let value_lower = value.to_ascii_lowercase();
    if value_lower.starts_with(prefix_lower) {
        return true;
    }
    let component = last_identifier_component(&value_lower);
    // Do not surface schema-qualified duplicates for an unqualified prefix.
    // Component-only matching is needed solely to ignore an opening quote.
    if component.len() != value_lower.len() {
        return false;
    }
    if prefix_lower.starts_with('"') {
        component.starts_with(prefix_lower)
    } else {
        component
            .strip_prefix('"')
            .unwrap_or(component)
            .starts_with(prefix_lower)
    }
}

fn last_identifier_component(value: &str) -> &str {
    let bytes = value.as_bytes();
    let mut quoted = false;
    let mut component_start = 0;
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'"' if quoted && bytes.get(index + 1) == Some(&b'"') => index += 2,
            b'"' => {
                quoted = !quoted;
                index += 1;
            }
            b'.' if !quoted => {
                component_start = index + 1;
                index += 1;
            }
            _ => index += 1,
        }
    }
    &value[component_start..]
}

fn completion_start(line: &str, pos: usize, standard_conforming_strings: bool) -> usize {
    let (start, _) = scanner::word_at(line, pos);
    let scan =
        scanner::scan_with_standard_conforming_strings(&line[..pos], standard_conforming_strings);
    scan.tokens
        .last()
        .filter(|token| token.end == pos && line.as_bytes().get(token.start) == Some(&b'"'))
        .map_or(start, |token| token.start)
}

impl Completer for SqlCompleter {
    fn complete(&mut self, line: &str, pos: usize) -> Vec<Suggestion> {
        let (start, prefix) = if line[..pos].trim_start().starts_with('\\') {
            let start = line[..pos].find('\\').unwrap_or(0);
            (start, &line[start..pos])
        } else {
            let start = completion_start(
                line,
                pos,
                self.standard_conforming_strings.load(Ordering::Relaxed),
            );
            (start, &line[start..pos])
        };
        let prefix_lower = prefix.to_ascii_lowercase();
        self.candidates(line, start)
            .into_iter()
            .filter(|(value, _)| {
                !value.chars().any(output::is_unsafe_terminal_character)
                    && completion_matches(value, &prefix_lower)
            })
            .map(|(value, description)| Suggestion {
                display_override: Some(output::safe_terminal_text(&value)),
                value,
                description,
                span: Span::new(start, pos),
                append_whitespace: true,
                ..Suggestion::default()
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn completer() -> SqlCompleter {
        SqlCompleter::new(test_metadata())
    }

    fn test_metadata() -> Metadata {
        Metadata {
            schemas: vec!["public".into()],
            relations: vec!["users".into(), "public.users".into()],
            columns: vec!["id".into(), "name".into()],
            relation_columns: HashMap::from([("users".into(), vec!["id".into(), "name".into()])]),
            truncated: false,
        }
    }

    #[test]
    fn suggests_relations_after_from() {
        let values: Vec<_> = completer()
            .complete("select * from us", 16)
            .into_iter()
            .map(|s| s.value)
            .collect();
        assert_eq!(values, ["users"]);
    }

    #[test]
    fn unqualified_prefixes_do_not_show_qualified_duplicates() {
        let values: Vec<_> = completer()
            .complete("select * from us", 16)
            .into_iter()
            .map(|suggestion| suggestion.value)
            .collect();
        assert_eq!(values, ["users"]);
    }

    #[test]
    fn follows_live_string_conformance_setting() {
        let setting = Arc::new(AtomicBool::new(true));
        let metadata = MetadataStore::default();
        metadata.replace(Metadata {
            relations: vec!["users".into()],
            ..Metadata::default()
        });
        let mut completer =
            SqlCompleter::with_standard_conforming_strings(metadata, Arc::clone(&setting));
        let line = "SELECT 'it\\'s' FROM us";

        assert!(completer.complete(line, line.len()).is_empty());
        setting.store(false, Ordering::Relaxed);
        assert_eq!(completer.complete(line, line.len())[0].value, "users");
    }

    #[test]
    fn observes_metadata_replacements() {
        let metadata = MetadataStore::default();
        let mut completer = SqlCompleter::with_standard_conforming_strings(
            metadata.clone(),
            Arc::new(AtomicBool::new(true)),
        );
        let line = "select * from us";
        assert!(completer.complete(line, line.len()).is_empty());

        metadata.replace(Metadata {
            relations: vec!["users".into()],
            ..Metadata::default()
        });

        assert_eq!(completer.complete(line, line.len())[0].value, "users");
    }

    #[test]
    fn suggests_relations_after_schema_qualification() {
        let values: Vec<_> = completer()
            .complete("select * from public.us", 23)
            .into_iter()
            .map(|suggestion| suggestion.value)
            .collect();
        assert_eq!(values, ["users"]);

        let metadata = Metadata {
            relations: vec!["\"odd.schema\".\"Order.Items\"".into()],
            ..Metadata::default()
        };
        let line = "select * from \"odd.schema\".Ord";
        let values: Vec<_> = SqlCompleter::new(metadata)
            .complete(line, line.len())
            .into_iter()
            .map(|suggestion| suggestion.value)
            .collect();
        assert_eq!(values, ["\"Order.Items\""]);
    }

    #[test]
    fn suggests_qualified_columns() {
        let values: Vec<_> = completer()
            .complete("select users.n", 14)
            .into_iter()
            .map(|s| s.value)
            .collect();
        assert_eq!(values, ["name"]);
    }

    #[test]
    fn equivalent_quoted_qualifiers_suggest_columns() {
        let line = "select \"users\".";
        let values: Vec<_> = completer()
            .complete(line, line.len())
            .into_iter()
            .map(|suggestion| suggestion.value)
            .collect();
        assert_eq!(values, ["id", "name"]);
    }

    #[test]
    fn quotes_identifiers_that_need_it() {
        let mut metadata = test_metadata();
        metadata.relations.extend([
            "\"CamelCase\"".into(),
            "\"a\"\"b\"".into(),
            "\"order\"".into(),
            "\"Order.Items\"".into(),
            "\"odd.schema\".\"Order.Items\"".into(),
        ]);
        let values: Vec<_> = SqlCompleter::new(metadata.clone())
            .complete("select * from ", 14)
            .into_iter()
            .map(|suggestion| suggestion.value)
            .collect();
        assert!(values.contains(&"\"CamelCase\"".into()));
        assert!(values.contains(&"\"a\"\"b\"".into()));
        assert!(values.contains(&"\"order\"".into()));
        assert!(values.contains(&"\"odd.schema\".\"Order.Items\"".into()));

        let partial: Vec<_> = SqlCompleter::new(metadata)
            .complete("select * from Ord", 17)
            .into_iter()
            .map(|suggestion| suggestion.value)
            .collect();
        assert!(partial.contains(&"\"Order.Items\"".into()));

        let line = "select * from \"a\"\"";
        let suggestion = SqlCompleter::new(Metadata {
            relations: vec!["\"a\"\"b\"".into()],
            ..Metadata::default()
        })
        .complete(line, line.len())
        .into_iter()
        .find(|suggestion| suggestion.value == "\"a\"\"b\"")
        .unwrap();
        assert_eq!(&line[suggestion.span.start..suggestion.span.end], "\"a\"\"");
    }

    #[test]
    fn suggests_columns_for_quoted_qualified_relations() {
        let metadata = Metadata {
            relation_columns: HashMap::from([(
                "\"odd.schema\".\"Order.Items\"".into(),
                vec!["\"select\"".into(), "\"CamelCase\"".into()],
            )]),
            ..Metadata::default()
        };
        let line = "select \"odd.schema\".\"Order.Items\".";
        let values: Vec<_> = SqlCompleter::new(metadata)
            .complete(line, line.len())
            .into_iter()
            .map(|suggestion| suggestion.value)
            .collect();
        assert_eq!(values, ["\"select\"", "\"CamelCase\""]);
    }

    #[test]
    fn excludes_identifiers_with_unsafe_terminal_characters() {
        for raw in [
            "\"bad\x1b]52;value\"",
            "\"bad\u{202e}value\"",
            "\"bad\u{2067}value\"",
        ] {
            let suggestions = SqlCompleter::new(Metadata {
                relations: vec![raw.into()],
                ..Metadata::default()
            })
            .complete("select * from ", 14);
            assert!(!suggestions.iter().any(|suggestion| suggestion.value == raw));
        }
    }

    #[test]
    fn suggests_special_commands() {
        assert!(
            completer()
                .complete("\\e", 2)
                .iter()
                .any(|s| s.value == "\\e")
        );
    }
}
