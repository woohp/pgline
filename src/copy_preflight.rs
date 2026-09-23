use crate::{error::AppError, scanner};

/// Rejects `COPY ... FROM STDIN` and `COPY ... TO STDOUT`, which need the copy
/// protocol, before any statement in `sql` is sent.
pub(crate) fn unsupported_copy_error(
    sql: &str,
    standard_conforming_strings: bool,
) -> Option<AppError> {
    for words in scanner::statements(sql, standard_conforming_strings)? {
        if words.first().is_none_or(|word| word != "COPY") {
            continue;
        }
        for pair in words.windows(2) {
            match (pair[0].as_str(), pair[1].as_str()) {
                ("FROM", "STDIN") => {
                    return Some(AppError::Unsupported(
                        "COPY FROM STDIN is not implemented; no statements were executed".into(),
                    ));
                }
                ("TO", "STDOUT") => {
                    return Some(AppError::Unsupported(
                        "COPY TO STDOUT is not implemented; no statements were executed".into(),
                    ));
                }
                _ => {}
            }
        }
    }
    None
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
