#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenKind {
    Keyword,
    Word,
    String,
    Number,
    Comment,
    Symbol,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Token {
    pub start: usize,
    pub end: usize,
    pub kind: TokenKind,
}

#[derive(Debug)]
pub struct ScanResult {
    pub tokens: Vec<Token>,
    pub balanced: bool,
}

pub fn scan(input: &str) -> ScanResult {
    scan_with_standard_conforming_strings(input, true)
}

pub fn scan_with_standard_conforming_strings(
    input: &str,
    standard_conforming_strings: bool,
) -> ScanResult {
    let bytes = input.as_bytes();
    let mut tokens = Vec::new();
    let mut index = 0;
    let mut balanced = true;
    let mut parens = 0usize;

    while index < bytes.len() {
        let start = index;
        match bytes[index] {
            byte if byte.is_ascii_whitespace() => {
                index += 1;
                continue;
            }
            b'-' if bytes.get(index + 1) == Some(&b'-') => {
                index += 2;
                while index < bytes.len() && bytes[index] != b'\n' {
                    index += 1;
                }
                tokens.push(Token {
                    start,
                    end: index,
                    kind: TokenKind::Comment,
                });
                continue;
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                index += 2;
                let mut depth = 1usize;
                while index < bytes.len() && depth > 0 {
                    if bytes.get(index..index + 2) == Some(b"/*") {
                        depth += 1;
                        index += 2;
                    } else if bytes.get(index..index + 2) == Some(b"*/") {
                        depth -= 1;
                        index += 2;
                    } else {
                        index += 1;
                    }
                }
                balanced &= depth == 0;
                tokens.push(Token {
                    start,
                    end: index,
                    kind: TokenKind::Comment,
                });
                continue;
            }
            b'\'' => {
                let escape_string = index > 0
                    && matches!(bytes[index - 1], b'e' | b'E')
                    && (index == 1 || !is_word_continue(bytes[index - 2]));
                let (end, closed) = scan_quoted(
                    bytes,
                    index,
                    b'\'',
                    escape_string || !standard_conforming_strings,
                );
                index = end;
                balanced &= closed;
                tokens.push(Token {
                    start,
                    end: index,
                    kind: TokenKind::String,
                });
            }
            b'"' => {
                let (end, closed) = scan_quoted(bytes, index, b'"', false);
                index = end;
                balanced &= closed;
                tokens.push(Token {
                    start,
                    end: index,
                    kind: TokenKind::Word,
                });
            }
            b'$' => {
                if let Some(delimiter_end) = dollar_delimiter_end(input, index) {
                    let delimiter = &bytes[index..delimiter_end];
                    index = delimiter_end;
                    if let Some(offset) = find_bytes(&bytes[index..], delimiter) {
                        index += offset + delimiter.len();
                    } else {
                        index = bytes.len();
                        balanced = false;
                    }
                    tokens.push(Token {
                        start,
                        end: index,
                        kind: TokenKind::String,
                    });
                } else {
                    index += 1;
                    tokens.push(Token {
                        start,
                        end: index,
                        kind: TokenKind::Symbol,
                    });
                }
            }
            byte if !byte.is_ascii() => {
                index += input[index..]
                    .chars()
                    .next()
                    .expect("non-empty input")
                    .len_utf8();
                while index < bytes.len() {
                    let character = input[index..].chars().next().expect("non-empty input");
                    if character.is_alphanumeric() || matches!(character, '_' | '$') {
                        index += character.len_utf8();
                    } else {
                        break;
                    }
                }
                tokens.push(Token {
                    start,
                    end: index,
                    kind: TokenKind::Word,
                });
            }
            byte if byte.is_ascii_digit() => {
                index += 1;
                while index < bytes.len()
                    && (bytes[index].is_ascii_alphanumeric() || matches!(bytes[index], b'.' | b'_'))
                {
                    index += 1;
                }
                tokens.push(Token {
                    start,
                    end: index,
                    kind: TokenKind::Number,
                });
            }
            byte if is_word_start(byte) => {
                index += 1;
                while index < bytes.len() && is_word_continue(bytes[index]) {
                    index += 1;
                }
                let text = &input[start..index];
                let kind = if is_keyword(text) {
                    TokenKind::Keyword
                } else {
                    TokenKind::Word
                };
                tokens.push(Token {
                    start,
                    end: index,
                    kind,
                });
            }
            b'(' => {
                parens += 1;
                index += 1;
                tokens.push(Token {
                    start,
                    end: index,
                    kind: TokenKind::Symbol,
                });
            }
            b')' => {
                // An extra closing parenthesis is invalid SQL, but it is not
                // incomplete input; submit it so PostgreSQL can report it.
                parens = parens.saturating_sub(1);
                index += 1;
                tokens.push(Token {
                    start,
                    end: index,
                    kind: TokenKind::Symbol,
                });
            }
            _ => {
                index += 1;
                tokens.push(Token {
                    start,
                    end: index,
                    kind: TokenKind::Symbol,
                });
            }
        }
    }

    ScanResult {
        tokens,
        balanced: balanced && parens == 0,
    }
}

#[cfg(test)]
pub fn is_complete(input: &str) -> bool {
    is_complete_with_standard_conforming_strings(input, true)
}

pub fn is_complete_with_standard_conforming_strings(
    input: &str,
    standard_conforming_strings: bool,
) -> bool {
    let trimmed = input.trim_start();
    if trimmed.is_empty() || trimmed.starts_with('\\') {
        return true;
    }
    scan_with_standard_conforming_strings(input, standard_conforming_strings).balanced
}

pub fn word_at(input: &str, cursor: usize) -> (usize, &str) {
    let cursor = floor_char_boundary(input, cursor.min(input.len()));
    let mut start = cursor;
    for (offset, character) in input[..cursor].char_indices().rev() {
        if character.is_alphanumeric() || matches!(character, '_' | '$') {
            start = offset;
        } else {
            break;
        }
    }
    (start, &input[start..cursor])
}

fn floor_char_boundary(input: &str, mut index: usize) -> usize {
    while !input.is_char_boundary(index) {
        index -= 1;
    }
    index
}

fn scan_quoted(
    bytes: &[u8],
    mut index: usize,
    quote: u8,
    backslash_escapes: bool,
) -> (usize, bool) {
    index += 1;
    while index < bytes.len() {
        if backslash_escapes && bytes[index] == b'\\' && index + 1 < bytes.len() {
            index += 2;
        } else if bytes[index] == quote {
            if bytes.get(index + 1) == Some(&quote) {
                index += 2;
            } else {
                return (index + 1, true);
            }
        } else {
            index += 1;
        }
    }
    (index, false)
}

fn dollar_delimiter_end(input: &str, start: usize) -> Option<usize> {
    let tag = &input[start + 1..];
    let mut characters = tag.char_indices();
    let (_, first) = characters.next()?;
    if first == '$' {
        return Some(start + 2);
    }
    if first != '_' && !first.is_alphabetic() {
        return None;
    }
    for (offset, character) in characters {
        if character == '$' {
            return Some(start + 1 + offset + 1);
        }
        if character != '_' && !character.is_alphanumeric() {
            return None;
        }
    }
    None
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn is_word_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_'
}

fn is_word_continue(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'$')
}

pub fn is_keyword(word: &str) -> bool {
    const KEYWORDS: &[&str] = &[
        "ALL",
        "ALTER",
        "ANALYZE",
        "AND",
        "AS",
        "ASC",
        "BEGIN",
        "BETWEEN",
        "BY",
        "CASE",
        "CHECK",
        "COMMIT",
        "CONFLICT",
        "CREATE",
        "CROSS",
        "CURRENT",
        "DATABASE",
        "DEFAULT",
        "DELETE",
        "DESC",
        "DISTINCT",
        "DO",
        "DROP",
        "ELSE",
        "END",
        "EXISTS",
        "EXPLAIN",
        "FALSE",
        "FETCH",
        "FOR",
        "FOREIGN",
        "FROM",
        "FULL",
        "GRANT",
        "GROUP",
        "HAVING",
        "ILIKE",
        "IN",
        "INDEX",
        "INNER",
        "INSERT",
        "INTERSECT",
        "INTO",
        "IS",
        "JOIN",
        "LATERAL",
        "LEFT",
        "LIKE",
        "LIMIT",
        "MATERIALIZED",
        "NATURAL",
        "NOT",
        "NOTHING",
        "NULL",
        "OFFSET",
        "ON",
        "ONLY",
        "OR",
        "ORDER",
        "OUTER",
        "OVER",
        "PARTITION",
        "PRIMARY",
        "REFERENCES",
        "RETURNING",
        "REVOKE",
        "RIGHT",
        "ROLLBACK",
        "SCHEMA",
        "SELECT",
        "SET",
        "TABLE",
        "THEN",
        "TO",
        "TRANSACTION",
        "TRUE",
        "TRUNCATE",
        "UNION",
        "UNIQUE",
        "UPDATE",
        "USING",
        "VALUES",
        "VIEW",
        "WHEN",
        "WHERE",
        "WINDOW",
        "WITH",
    ];
    KEYWORDS
        .binary_search(&word.to_ascii_uppercase().as_str())
        .is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completeness_depends_on_balanced_syntax_not_semicolons() {
        assert!(is_complete("select 1"));
        assert!(is_complete("select ';'"));
        assert!(!is_complete("select '"));
        assert!(is_complete("select ';'; -- done"));
        assert!(is_complete("-- comment only"));
        assert!(!is_complete("select 1 /* unfinished"));
        assert!(is_complete(r"SELECT E'it\'s valid'"));
        assert!(is_complete(r"SELECT E'backslash: \\'"));
        assert!(is_complete("SELECT E'multiline\\\nstill valid'"));
        assert!(!is_complete("SELECT E'multiline\\\nstill open"));
        assert!(is_complete_with_standard_conforming_strings(
            r"SELECT 'it\'s valid'",
            false,
        ));
        assert!(!is_complete_with_standard_conforming_strings(
            r"SELECT 'it\'s valid'",
            true,
        ));
    }

    #[test]
    fn completeness_handles_dollar_quotes_and_parentheses() {
        assert!(is_complete("do $$ begin raise notice ';'; end $$;"));
        assert!(!is_complete("select (1;"));
        assert!(is_complete("select 1)"));
        assert!(is_complete("select 1))"));
        assert!(!is_complete("select $tag$unfinished;"));
        assert!(is_complete("select $标签$not syntax$标签$"));
        assert_eq!(dollar_delimiter_end("$💥$", 0), None);
        assert_eq!(dollar_delimiter_end("$·$", 0), None);
        assert_eq!(dollar_delimiter_end("$标签$", 0), Some("$标签$".len()));
    }

    #[test]
    fn nested_comments_are_balanced() {
        assert!(is_complete("/* outer /* inner */ done */ select 1;"));
        assert!(!is_complete("/* unfinished select 1;"));
    }

    #[test]
    fn finds_current_word() {
        assert_eq!(word_at("select use", 10), (7, "use"));
        assert_eq!(word_at("schema.tab", 10), (7, "tab"));
        assert_eq!(word_at("select café", 12), (7, "café"));
        assert_eq!(scan("select café;").tokens.last().unwrap().end, 13);
    }
}
