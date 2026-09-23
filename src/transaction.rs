use crate::scanner;

/// The session's transaction state, as tracked client-side from the SQL that
/// was submitted. `Unknown` means the SQL was too ambiguous to track.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum TransactionStatus {
    #[default]
    Idle,
    Active,
    Failed,
    Unknown,
}

pub(crate) fn after_catalog_operation(state: TransactionStatus) -> TransactionStatus {
    match state {
        TransactionStatus::Active => TransactionStatus::Failed,
        state => state,
    }
}

pub(crate) fn after_success(
    mut state: TransactionStatus,
    sql: &str,
    standard_conforming_strings: bool,
) -> TransactionStatus {
    let Some(statements) = scanner::statements(sql, standard_conforming_strings) else {
        return TransactionStatus::Unknown;
    };
    for words in statements {
        state = apply_statement(state, &words);
    }
    state
}

pub(crate) fn after_error(
    mut state: TransactionStatus,
    sql: &str,
    completed_statements: usize,
    standard_conforming_strings: bool,
) -> TransactionStatus {
    let Some(statements) = scanner::statements(sql, standard_conforming_strings) else {
        return TransactionStatus::Unknown;
    };
    for words in statements.iter().take(completed_statements) {
        state = apply_statement(state, words);
    }
    let failed = statements.get(completed_statements);
    match state {
        TransactionStatus::Active | TransactionStatus::Failed
            if failed.is_some_and(|words| is_transaction_ending(words)) =>
        {
            TransactionStatus::Unknown
        }
        TransactionStatus::Active | TransactionStatus::Failed => TransactionStatus::Failed,
        TransactionStatus::Unknown => TransactionStatus::Unknown,
        TransactionStatus::Idle => {
            if failed.is_some_and(|words| {
                matches!(
                    words.first().map(String::as_str),
                    Some("BEGIN" | "START" | "CALL")
                )
            }) {
                TransactionStatus::Unknown
            } else {
                TransactionStatus::Idle
            }
        }
    }
}

fn is_transaction_ending(words: &[String]) -> bool {
    let rollback_to = words.iter().any(|word| word == "TO");
    matches!(
        words.first().map(String::as_str),
        Some("COMMIT" | "END" | "ABORT")
    ) || matches!(words.first().map(String::as_str), Some("ROLLBACK") if !rollback_to)
        || matches!(words, [first, second, ..] if first == "PREPARE" && second == "TRANSACTION")
}

fn apply_statement(state: TransactionStatus, words: &[String]) -> TransactionStatus {
    let chained = words
        .windows(2)
        .any(|pair| pair[0] == "AND" && pair[1] == "CHAIN");
    match words.first().map(String::as_str) {
        Some("BEGIN" | "START") => TransactionStatus::Active,
        Some("COMMIT" | "END") if chained => TransactionStatus::Active,
        Some("COMMIT" | "END") => TransactionStatus::Idle,
        Some("ROLLBACK") if words.iter().any(|word| word == "TO") => TransactionStatus::Active,
        Some("ROLLBACK" | "ABORT") if chained => TransactionStatus::Active,
        Some("ROLLBACK" | "ABORT") => TransactionStatus::Idle,
        Some("PREPARE") if words.get(1).is_some_and(|word| word == "TRANSACTION") => {
            TransactionStatus::Idle
        }
        Some("CALL") => TransactionStatus::Unknown,
        _ => state,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracks_completed_transaction_statements() {
        let cases = [
            (
                TransactionStatus::Idle,
                "BEGIN; COMMIT;",
                TransactionStatus::Idle,
            ),
            (
                TransactionStatus::Active,
                "ROLLBACK TO SAVEPOINT x",
                TransactionStatus::Active,
            ),
            (
                TransactionStatus::Active,
                "COMMIT AND CHAIN",
                TransactionStatus::Active,
            ),
            (
                TransactionStatus::Active,
                "ROLLBACK AND CHAIN",
                TransactionStatus::Active,
            ),
            (
                TransactionStatus::Active,
                "ROLLBACK AND NO CHAIN",
                TransactionStatus::Idle,
            ),
            (
                TransactionStatus::Failed,
                "ROLLBACK TO SAVEPOINT x",
                TransactionStatus::Active,
            ),
            (
                TransactionStatus::Active,
                "COMMIT AND NO CHAIN",
                TransactionStatus::Idle,
            ),
            (TransactionStatus::Active, "ABORT", TransactionStatus::Idle),
            (
                TransactionStatus::Failed,
                "ABORT AND NO CHAIN",
                TransactionStatus::Idle,
            ),
            (
                TransactionStatus::Failed,
                "ABORT AND CHAIN",
                TransactionStatus::Active,
            ),
            (
                TransactionStatus::Active,
                "PREPARE TRANSACTION 'x'",
                TransactionStatus::Idle,
            ),
        ];
        for (initial, sql, expected) in cases {
            assert_eq!(after_success(initial, sql, true), expected, "{sql}");
        }
    }

    #[test]
    fn nested_statements_do_not_change_transaction_state() {
        let function = "CREATE FUNCTION f() RETURNS int LANGUAGE SQL \
                        BEGIN ATOMIC SELECT CASE WHEN true THEN 1 END; SELECT 2; END";
        assert_eq!(
            after_success(TransactionStatus::Active, function, true),
            TransactionStatus::Active
        );
        assert_eq!(
            after_success(
                TransactionStatus::Active,
                &format!("{function}; COMMIT"),
                true
            ),
            TransactionStatus::Idle
        );
        let rule = "CREATE RULE r AS ON INSERT TO t DO (NOTIFY one; NOTIFY two)";
        assert_eq!(
            after_success(TransactionStatus::Active, rule, true),
            TransactionStatus::Active
        );
    }

    #[test]
    fn analysis_uses_submission_time_string_conformance() {
        assert_eq!(
            after_success(
                TransactionStatus::Active,
                r"SELECT 'before\'; COMMIT; still string'",
                false
            ),
            TransactionStatus::Active
        );
    }

    #[test]
    fn catalog_operations_fail_only_active_transactions() {
        // Catalog errors and backend-confirmed cancellations share this transition.
        assert_eq!(
            after_catalog_operation(TransactionStatus::Active),
            TransactionStatus::Failed
        );
        assert_eq!(
            after_catalog_operation(TransactionStatus::Unknown),
            TransactionStatus::Unknown
        );
        assert_eq!(
            after_catalog_operation(TransactionStatus::Idle),
            TransactionStatus::Idle
        );
    }

    #[test]
    fn failed_statements_update_state_conservatively() {
        let cases = [
            (
                TransactionStatus::Idle,
                "BEGIN; SELECT missing",
                1,
                TransactionStatus::Failed,
            ),
            (
                TransactionStatus::Active,
                "SELECT missing",
                0,
                TransactionStatus::Failed,
            ),
            (
                TransactionStatus::Active,
                "COMMIT",
                0,
                TransactionStatus::Unknown,
            ),
            (
                TransactionStatus::Active,
                "ROLLBACK TO missing",
                0,
                TransactionStatus::Failed,
            ),
            (
                TransactionStatus::Active,
                "ROLLBACK; SELECT missing",
                1,
                TransactionStatus::Idle,
            ),
            (
                TransactionStatus::Idle,
                "BEGIN; SELECT missing",
                0,
                TransactionStatus::Unknown,
            ),
            (
                TransactionStatus::Idle,
                "SELECT 1)",
                0,
                TransactionStatus::Idle,
            ),
        ];
        for (initial, sql, completed, expected) in cases {
            assert_eq!(
                after_error(initial, sql, completed, true),
                expected,
                "{sql}"
            );
        }
    }
}
