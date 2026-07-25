use std::collections::HashMap;

use futures_util::{StreamExt, pin_mut};
use tokio_postgres::{Client, types::ToSql};

use crate::{
    error::{AppError, Result},
    output::{self, BoundedBuffer, ResultSet, RetentionBudget},
};

use super::{CatalogCommand, RelationKind};

#[derive(Clone, Copy)]
pub struct CatalogLimits {
    pub row_limit: usize,
    pub max_field_width: usize,
}

pub async fn run(
    client: &Client,
    command: &CatalogCommand,
    limits: CatalogLimits,
) -> Result<String> {
    match command {
        CatalogCommand::Describe { pattern, verbose } => {
            if let Some(pattern) = pattern {
                describe_relations(client, pattern, *verbose, limits).await
            } else {
                list_relations(client, RelationKind::All, None, *verbose, limits).await
            }
        }
        CatalogCommand::ListRelations {
            kind,
            pattern,
            verbose,
        } => list_relations(client, *kind, pattern.as_deref(), *verbose, limits).await,
        CatalogCommand::Functions { pattern } => {
            list_functions(client, pattern.as_deref(), limits).await
        }
        CatalogCommand::Schemas { pattern } => {
            list_schemas(client, pattern.as_deref(), limits).await
        }
        CatalogCommand::Databases { pattern } => {
            list_databases(client, pattern.as_deref(), limits).await
        }
        CatalogCommand::Roles { pattern } => list_roles(client, pattern.as_deref(), limits).await,
        CatalogCommand::ConnectionInfo => connection_info(client, limits).await,
    }
}

async fn list_relations(
    client: &Client,
    kind: RelationKind,
    pattern: Option<&str>,
    verbose: bool,
    limits: CatalogLimits,
) -> Result<String> {
    let pattern = sql_pattern(pattern)?;
    let kinds = match kind {
        RelationKind::All => vec!["r", "p", "v", "m", "S", "f"],
        RelationKind::Table => vec!["r", "p", "f"],
        RelationKind::View => vec!["v"],
        RelationKind::MaterializedView => vec!["m"],
        RelationKind::Index => vec!["i", "I"],
        RelationKind::Sequence => vec!["S"],
    };
    let sql = if verbose {
        LIST_RELATIONS_VERBOSE
    } else {
        LIST_RELATIONS
    };
    query_table(client, sql, &[&kinds, &pattern], limits).await
}

async fn describe_relations(
    client: &Client,
    pattern: &str,
    verbose: bool,
    limits: CatalogLimits,
) -> Result<String> {
    let pattern = sql_pattern(Some(pattern))?;
    let mut retention_budget = RetentionBudget::for_human_result();
    let relations = load_relation_matches(client, &pattern, limits, &mut retention_budget).await?;
    if relations.is_empty() {
        return Ok(format!("Did not find any relation matching {pattern:?}.\n"));
    }

    let details =
        load_relation_details(client, &relations, verbose, limits, &mut retention_budget).await?;
    render_relation_descriptions(relations, details, verbose, limits)
}

async fn load_relation_matches(
    client: &Client,
    pattern: &str,
    limits: CatalogLimits,
    retention_budget: &mut RetentionBudget,
) -> Result<Vec<RelationDescription>> {
    let rows = client.query_raw(DESCRIBE_MATCHES, [pattern]).await?;
    pin_mut!(rows);
    let mut relations = Vec::new();
    let mut too_many = false;
    while let Some(row) = rows.next().await {
        let row = row?;
        if relations.len() == MAX_DESCRIBE_RELATIONS {
            too_many = true;
            continue;
        }
        let definition: Option<&str> = row.get(4);
        let (view_definition, view_definition_truncated, view_definition_limited) =
            retain_view_definition(definition, limits.max_field_width, retention_budget);
        relations.push(RelationDescription {
            oid: row.get(0),
            schema: row.get(1),
            name: row.get(2),
            kind: row.get(3),
            view_definition,
            view_definition_truncated,
            view_definition_limited,
        });
    }
    if too_many {
        return Err(AppError::InvalidCommand(format!(
            "describe pattern matched more than {MAX_DESCRIBE_RELATIONS} relations; use a narrower pattern"
        )));
    }
    Ok(relations)
}

fn retain_view_definition(
    definition: Option<&str>,
    max_field_width: usize,
    retention_budget: &mut RetentionBudget,
) -> (Option<String>, bool, bool) {
    let Some(definition) = definition else {
        return (None, false, false);
    };
    let retained_len = output::retained_field_len(definition, max_field_width);
    if !retention_budget.take(retained_len, 1) {
        return (None, false, true);
    }
    let (definition, truncated) = output::truncate_field(definition, max_field_width);
    (Some(definition), truncated, false)
}

struct RelationDetails {
    columns: HashMap<u32, CatalogTable>,
    constraints: HashMap<u32, CatalogTable>,
    indexes: HashMap<u32, CatalogTable>,
    /// Storage and size columns, populated only for the `+` command forms.
    storage: HashMap<u32, CatalogTable>,
}

async fn load_relation_details(
    client: &Client,
    relations: &[RelationDescription],
    verbose: bool,
    limits: CatalogLimits,
    retention_budget: &mut RetentionBudget,
) -> Result<RelationDetails> {
    let oids: Vec<u32> = relations.iter().map(|relation| relation.oid).collect();
    let column_sql = if verbose {
        DESCRIBE_COLUMNS_VERBOSE
    } else {
        DESCRIBE_COLUMNS
    };

    // Fetch each category for all matching relations at once so wildcard
    // descriptions use a fixed number of network round trips.
    let columns = query_grouped_tables(client, column_sql, &oids, limits, retention_budget).await?;
    let constraints = query_grouped_tables(
        client,
        DESCRIBE_CONSTRAINTS,
        &oids,
        limits,
        retention_budget,
    )
    .await?;
    let indexes =
        query_grouped_tables(client, DESCRIBE_INDEXES, &oids, limits, retention_budget).await?;
    let storage = if verbose {
        query_grouped_tables(client, DESCRIBE_DETAILS, &oids, limits, retention_budget).await?
    } else {
        HashMap::new()
    };
    Ok(RelationDetails {
        columns,
        constraints,
        indexes,
        storage,
    })
}

fn render_relation_descriptions(
    relations: Vec<RelationDescription>,
    mut details: RelationDetails,
    verbose: bool,
    limits: CatalogLimits,
) -> Result<String> {
    let mut rendered = catalog_buffer();
    for relation in relations {
        append_relation_description(&mut rendered, relation, &mut details, verbose, limits)?;
        if rendered.is_limited() {
            break;
        }
    }
    Ok(rendered.finish())
}

fn append_relation_description(
    rendered: &mut BoundedBuffer,
    relation: RelationDescription,
    details: &mut RelationDetails,
    verbose: bool,
    limits: CatalogLimits,
) -> Result<()> {
    let qualified_name = format!(
        "{}.{}",
        output::quote_identifier(&relation.schema),
        output::quote_identifier(&relation.name)
    );
    rendered.push(&format!(
        "{} {}\n",
        relation_label(&relation.kind),
        output::safe_terminal_text(&qualified_name)
    ));

    let columns = take_catalog_table(&mut details.columns, relation.oid, "columns")?;
    rendered.push(&columns.render(limits.row_limit));
    append_optional_catalog_table(
        rendered,
        "\nConstraints:\n",
        take_catalog_table(&mut details.constraints, relation.oid, "constraints")?,
        limits.row_limit,
    );
    append_optional_catalog_table(
        rendered,
        "\nIndexes:\n",
        take_catalog_table(&mut details.indexes, relation.oid, "indexes")?,
        limits.row_limit,
    );
    append_view_definition(rendered, &relation);
    if verbose {
        let storage = take_catalog_table(&mut details.storage, relation.oid, "details")?;
        rendered.push(&storage.render(limits.row_limit));
    }
    Ok(())
}

fn append_optional_catalog_table(
    rendered: &mut BoundedBuffer,
    heading: &str,
    table: CatalogTable,
    row_limit: usize,
) {
    if table.total_rows() == 0 {
        return;
    }
    rendered.push(heading);
    rendered.push(&table.render(row_limit));
}

fn append_view_definition(rendered: &mut BoundedBuffer, relation: &RelationDescription) {
    if relation.view_definition.is_none() && !relation.view_definition_limited {
        return;
    }
    let mut section = String::from("View definition:\n");
    if let Some(definition) = &relation.view_definition {
        section.push_str(&output::safe_terminal_text(definition));
        if relation.view_definition_truncated {
            section.push_str("\n[view definition truncated]");
        }
        section.push('\n');
    } else {
        section.push_str(CATALOG_OUTPUT_LIMIT_MARKER);
    }
    rendered.push(&section);
}

async fn list_functions(
    client: &Client,
    pattern: Option<&str>,
    limits: CatalogLimits,
) -> Result<String> {
    let pattern = sql_pattern(pattern)?;
    query_table(client, LIST_FUNCTIONS, &[&pattern], limits).await
}

async fn list_schemas(
    client: &Client,
    pattern: Option<&str>,
    limits: CatalogLimits,
) -> Result<String> {
    let pattern = sql_pattern(pattern)?;
    query_table(client, LIST_SCHEMAS, &[&pattern], limits).await
}

async fn list_databases(
    client: &Client,
    pattern: Option<&str>,
    limits: CatalogLimits,
) -> Result<String> {
    let pattern = sql_pattern(pattern)?;
    query_table(client, LIST_DATABASES, &[&pattern], limits).await
}

async fn list_roles(
    client: &Client,
    pattern: Option<&str>,
    limits: CatalogLimits,
) -> Result<String> {
    let pattern = sql_pattern(pattern)?;
    query_table(client, LIST_ROLES, &[&pattern], limits).await
}

async fn connection_info(client: &Client, limits: CatalogLimits) -> Result<String> {
    query_table(client, CONNECTION_INFO, &[], limits).await
}

struct RelationDescription {
    oid: u32,
    schema: String,
    name: String,
    kind: String,
    view_definition: Option<String>,
    view_definition_truncated: bool,
    view_definition_limited: bool,
}

const CATALOG_OUTPUT_LIMIT_MARKER: &str = "[output limited]\n";

struct CatalogTable {
    result: ResultSet,
}

impl CatalogTable {
    fn total_rows(&self) -> usize {
        self.result.total_rows
    }

    fn render(&self, row_limit: usize) -> String {
        let rows_limited = row_limit != 0 && self.result.total_rows > row_limit;
        output::render_human_table(&self.result, false, rows_limited)
    }
}

fn take_catalog_table(
    tables: &mut HashMap<u32, CatalogTable>,
    oid: u32,
    category: &str,
) -> Result<CatalogTable> {
    tables.remove(&oid).ok_or_else(|| {
        AppError::Internal(format!(
            "catalog query omitted {category} for relation OID {oid}"
        ))
    })
}

fn catalog_buffer() -> BoundedBuffer {
    BoundedBuffer::new(
        output::MAX_INTERACTIVE_BATCH_BYTES,
        CATALOG_OUTPUT_LIMIT_MARKER,
    )
}

async fn query_grouped_tables(
    client: &Client,
    sql: &str,
    oids: &[u32],
    limits: CatalogLimits,
    retention_budget: &mut RetentionBudget,
) -> Result<HashMap<u32, CatalogTable>> {
    let statement = client.prepare(sql).await?;
    let columns: Vec<String> = statement
        .columns()
        .iter()
        .skip(1)
        .map(|column| column.name().to_owned())
        .collect();
    let mut grouped: HashMap<u32, ResultSet> = oids
        .iter()
        .map(|oid| {
            (
                *oid,
                ResultSet::with_columns(columns.clone(), retention_budget),
            )
        })
        .collect();
    let rows = client.query_raw(&statement, [oids]).await?;
    pin_mut!(rows);

    while let Some(row) = rows.next().await {
        let row = row?;
        let oid: u32 = row.get(0);
        let result = grouped.get_mut(&oid).ok_or_else(|| {
            AppError::Internal(format!(
                "catalog query returned unexpected relation OID {oid}"
            ))
        })?;
        let values: Vec<Option<&str>> = (1..row.len()).map(|index| row.get(index)).collect();
        result.retain_human_row(
            &values,
            limits.row_limit,
            limits.max_field_width,
            retention_budget,
        );
    }

    Ok(grouped
        .into_iter()
        .map(|(oid, result)| (oid, CatalogTable { result }))
        .collect())
}

async fn query_table(
    client: &Client,
    sql: &str,
    params: &[&(dyn ToSql + Sync)],
    limits: CatalogLimits,
) -> Result<String> {
    let table = query_table_result(client, sql, params, limits).await?;
    let mut rendered = catalog_buffer();
    rendered.push_truncated(&table.render(limits.row_limit));
    Ok(rendered.finish())
}

async fn query_table_result(
    client: &Client,
    sql: &str,
    params: &[&(dyn ToSql + Sync)],
    limits: CatalogLimits,
) -> Result<CatalogTable> {
    let statement = client.prepare(sql).await?;
    let columns = statement
        .columns()
        .iter()
        .map(|column| column.name().to_owned())
        .collect();
    let rows = client.query_raw(&statement, params.iter().copied()).await?;
    pin_mut!(rows);

    let mut budget = RetentionBudget::for_human_result();
    let mut result = ResultSet::with_columns(columns, &mut budget);
    while let Some(row) = rows.next().await {
        let row = row?;
        let values: Vec<Option<&str>> = (0..row.len()).map(|index| row.get(index)).collect();
        result.retain_human_row(
            &values,
            limits.row_limit,
            limits.max_field_width,
            &mut budget,
        );
    }
    Ok(CatalogTable { result })
}

pub fn sql_pattern(pattern: Option<&str>) -> Result<String> {
    let Some(pattern) = pattern else {
        return Ok("%".into());
    };
    let mut output = String::new();
    let mut quoted = false;
    let mut characters = pattern.chars().peekable();
    while let Some(character) = characters.next() {
        match character {
            '"' if quoted && characters.peek() == Some(&'"') => {
                characters.next();
                output.push('"');
            }
            '"' => quoted = !quoted,
            '*' if !quoted => output.push('%'),
            '?' if !quoted => output.push('_'),
            '%' | '_' | '\\' => {
                output.push('\\');
                output.push(character);
            }
            character if quoted => output.push(character),
            character => output.extend(character.to_lowercase()),
        }
    }
    if quoted {
        return Err(crate::error::AppError::InvalidCommand(
            "unterminated quoted catalog pattern".into(),
        ));
    }
    Ok(output)
}

fn relation_label(kind: &str) -> &'static str {
    match kind {
        "v" => "View",
        "m" => "Materialized view",
        "i" | "I" => "Index",
        "S" => "Sequence",
        "f" => "Foreign table",
        _ => "Table",
    }
}

// Explicit E strings make the LIKE escape character independent of the
// session's standard_conforming_strings setting.
const LIST_RELATIONS: &str = r#"
SELECT n.nspname::text AS "Schema",
       c.relname::text AS "Name",
       CASE c.relkind WHEN 'r' THEN 'table' WHEN 'p' THEN 'partitioned table'
         WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'S' THEN 'sequence'
         WHEN 'f' THEN 'foreign table' WHEN 'i' THEN 'index' WHEN 'I' THEN 'partitioned index'
         ELSE c.relkind::text END::text AS "Type",
       pg_get_userbyid(c.relowner)::text AS "Owner"
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind::text = ANY($1)
  AND (c.relname LIKE $2 ESCAPE E'\\' OR (n.nspname || '.' || c.relname) LIKE $2 ESCAPE E'\\')
  AND ($2 <> '%' OR (n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'))
ORDER BY 1, 2
"#;

const LIST_RELATIONS_VERBOSE: &str = r#"
SELECT n.nspname::text AS "Schema",
       c.relname::text AS "Name",
       CASE c.relkind WHEN 'r' THEN 'table' WHEN 'p' THEN 'partitioned table'
         WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'S' THEN 'sequence'
         WHEN 'f' THEN 'foreign table' WHEN 'i' THEN 'index' WHEN 'I' THEN 'partitioned index'
         ELSE c.relkind::text END::text AS "Type",
       pg_get_userbyid(c.relowner)::text AS "Owner",
       pg_size_pretty(pg_total_relation_size(c.oid))::text AS "Size",
       obj_description(c.oid, 'pg_class')::text AS "Description"
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind::text = ANY($1)
  AND (c.relname LIKE $2 ESCAPE E'\\' OR (n.nspname || '.' || c.relname) LIKE $2 ESCAPE E'\\')
  AND ($2 <> '%' OR (n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'))
ORDER BY 1, 2
"#;

const MAX_DESCRIBE_RELATIONS: usize = 100;

const DESCRIBE_MATCHES: &str = r#"
SELECT c.oid, n.nspname::text, c.relname::text, c.relkind::text,
       CASE WHEN c.relkind IN ('v', 'm') THEN pg_get_viewdef(c.oid, true) END::text
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r','p','v','m','S','f','i','I')
  AND (c.relname LIKE $1 ESCAPE E'\\' OR (n.nspname || '.' || c.relname) LIKE $1 ESCAPE E'\\')
ORDER BY n.nspname, c.relname
LIMIT 101
"#;

const DESCRIBE_COLUMNS: &str = r#"
SELECT a.attrelid, a.attname::text AS "Column",
       pg_catalog.format_type(a.atttypid, a.atttypmod)::text AS "Type",
       CASE WHEN a.attnotnull THEN 'not null' ELSE '' END::text AS "Nullable",
       pg_get_expr(d.adbin, d.adrelid)::text AS "Default"
FROM pg_catalog.pg_attribute a
LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
WHERE a.attrelid = ANY($1) AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY pg_catalog.array_position($1, a.attrelid), a.attnum
"#;

const DESCRIBE_COLUMNS_VERBOSE: &str = r#"
SELECT a.attrelid, a.attname::text AS "Column",
       pg_catalog.format_type(a.atttypid, a.atttypmod)::text AS "Type",
       CASE WHEN a.attnotnull THEN 'not null' ELSE '' END::text AS "Nullable",
       pg_get_expr(d.adbin, d.adrelid)::text AS "Default",
       CASE a.attstorage WHEN 'p' THEN 'plain' WHEN 'e' THEN 'external'
         WHEN 'm' THEN 'main' WHEN 'x' THEN 'extended' END::text AS "Storage",
       col_description(a.attrelid, a.attnum)::text AS "Description"
FROM pg_catalog.pg_attribute a
LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
WHERE a.attrelid = ANY($1) AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY pg_catalog.array_position($1, a.attrelid), a.attnum
"#;

const DESCRIBE_CONSTRAINTS: &str = r#"
SELECT conrelid, conname::text AS "Name", pg_get_constraintdef(oid, true)::text AS "Definition"
FROM pg_catalog.pg_constraint WHERE conrelid = ANY($1)
ORDER BY pg_catalog.array_position($1, conrelid), conname
"#;

const DESCRIBE_INDEXES: &str = r#"
SELECT i.indrelid, c.relname::text AS "Name", pg_get_indexdef(i.indexrelid)::text AS "Definition"
FROM pg_catalog.pg_index i
JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
WHERE i.indrelid = ANY($1)
ORDER BY pg_catalog.array_position($1, i.indrelid), c.relname
"#;

const DESCRIBE_DETAILS: &str = r#"
SELECT c.oid, pg_size_pretty(pg_total_relation_size(c.oid))::text AS "Total size",
       COALESCE(am.amname, '')::text AS "Access method",
       CASE c.relpersistence WHEN 'p' THEN 'permanent' WHEN 'u' THEN 'unlogged'
         WHEN 't' THEN 'temporary' END::text AS "Persistence",
       obj_description(c.oid, 'pg_class')::text AS "Description"
FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam
WHERE c.oid = ANY($1)
ORDER BY pg_catalog.array_position($1, c.oid)
"#;

const LIST_FUNCTIONS: &str = r#"
SELECT n.nspname::text AS "Schema", p.proname::text AS "Name",
       pg_get_function_result(p.oid)::text AS "Result data type",
       pg_get_function_arguments(p.oid)::text AS "Argument data types",
       CASE p.prokind WHEN 'a' THEN 'agg' WHEN 'w' THEN 'window' WHEN 'p' THEN 'proc' ELSE 'func' END::text AS "Type"
FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
WHERE (p.proname LIKE $1 ESCAPE E'\\' OR (n.nspname || '.' || p.proname) LIKE $1 ESCAPE E'\\')
  AND ($1 <> '%' OR (n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'))
ORDER BY 1, 2, 4
"#;

const LIST_SCHEMAS: &str = r#"
SELECT n.nspname::text AS "Name", pg_get_userbyid(n.nspowner)::text AS "Owner",
       obj_description(n.oid, 'pg_namespace')::text AS "Description"
FROM pg_catalog.pg_namespace n
WHERE n.nspname LIKE $1 ESCAPE E'\\'
  AND ($1 <> '%' OR (n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'))
ORDER BY 1
"#;

const LIST_DATABASES: &str = r#"
SELECT d.datname::text AS "Name", pg_get_userbyid(d.datdba)::text AS "Owner",
       pg_encoding_to_char(d.encoding)::text AS "Encoding", d.datcollate::text AS "Collate",
       d.datctype::text AS "Ctype", pg_size_pretty(pg_database_size(d.datname))::text AS "Size"
FROM pg_catalog.pg_database d WHERE d.datname LIKE $1 ESCAPE E'\\' ORDER BY 1
"#;

const LIST_ROLES: &str = r#"
SELECT r.rolname::text AS "Role name",
       concat_ws(', ', CASE WHEN r.rolsuper THEN 'Superuser' END,
         CASE WHEN r.rolcreatedb THEN 'Create DB' END, CASE WHEN r.rolcreaterole THEN 'Create role' END,
         CASE WHEN NOT r.rolcanlogin THEN 'Cannot login' END, CASE WHEN r.rolreplication THEN 'Replication' END,
         CASE WHEN r.rolbypassrls THEN 'Bypass RLS' END)::text AS "Attributes",
       ARRAY(SELECT b.rolname FROM pg_catalog.pg_auth_members m
             JOIN pg_catalog.pg_roles b ON b.oid = m.roleid WHERE m.member = r.oid)::text AS "Member of"
FROM pg_catalog.pg_roles r WHERE r.rolname LIKE $1 ESCAPE E'\\' ORDER BY 1
"#;

const CONNECTION_INFO: &str = r#"
SELECT current_database()::text AS "Database", current_user::text AS "User",
       COALESCE(inet_server_addr()::text, 'local')::text AS "Host",
       COALESCE(inet_server_port()::text, 'local')::text AS "Port",
       current_setting('server_version')::text AS "Server version"
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_psql_globs_to_safe_like_patterns() {
        assert_eq!(sql_pattern(None).unwrap(), "%");
        assert_eq!(sql_pattern(Some("Public.User*")).unwrap(), "public.user%");
        assert_eq!(sql_pattern(Some("under_score")).unwrap(), "under\\_score");
        assert_eq!(sql_pattern(Some("item?")).unwrap(), "item_");
        assert_eq!(
            sql_pattern(Some("public.\"CamelCase\"")).unwrap(),
            "public.CamelCase"
        );
        assert_eq!(sql_pattern(Some("\"literal*?\"")).unwrap(), "literal*?");
        assert_eq!(sql_pattern(Some("\"a\"\"b\"")).unwrap(), "a\"b");
        assert!(sql_pattern(Some("\"unfinished")).is_err());
    }

    #[test]
    fn combined_catalog_output_stays_within_the_interactive_batch_limit() {
        let mut rendered = catalog_buffer();
        rendered.push(&"x".repeat(output::MAX_INTERACTIVE_BATCH_BYTES));

        let rendered = rendered.finish();
        assert!(rendered.len() <= output::MAX_INTERACTIVE_BATCH_BYTES);
        assert!(rendered.ends_with(CATALOG_OUTPUT_LIMIT_MARKER));
    }

    #[tokio::test]
    #[ignore = "requires PGLINE_TEST_URL"]
    async fn catalog_commands_work_when_test_database_is_configured() {
        let database = crate::test_support::connect().await;
        database
            .client
            .batch_execute("SET standard_conforming_strings = off")
            .await
            .unwrap();
        let commands = [
            CatalogCommand::Describe {
                pattern: None,
                verbose: false,
            },
            CatalogCommand::ListRelations {
                kind: RelationKind::Table,
                pattern: None,
                verbose: true,
            },
            CatalogCommand::ListRelations {
                kind: RelationKind::View,
                pattern: None,
                verbose: false,
            },
            CatalogCommand::ListRelations {
                kind: RelationKind::MaterializedView,
                pattern: None,
                verbose: false,
            },
            CatalogCommand::ListRelations {
                kind: RelationKind::Index,
                pattern: None,
                verbose: false,
            },
            CatalogCommand::ListRelations {
                kind: RelationKind::Sequence,
                pattern: None,
                verbose: false,
            },
            CatalogCommand::Functions { pattern: None },
            CatalogCommand::Schemas { pattern: None },
            CatalogCommand::Databases { pattern: None },
            CatalogCommand::Roles { pattern: None },
            CatalogCommand::ConnectionInfo,
        ];
        for command in commands {
            let rendered = run(
                &database.client,
                &command,
                CatalogLimits {
                    row_limit: 1000,
                    max_field_width: 500,
                },
            )
            .await
            .unwrap();
            assert!(!rendered.is_empty(), "empty output for {command:?}");
        }

        let relation = database
            .client
            .query_opt(
                "SELECT c.relname::text FROM pg_catalog.pg_class c \
                 JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
                 WHERE c.relkind IN ('r','p') AND n.nspname !~ '^pg_' LIMIT 1",
                &[],
            )
            .await
            .unwrap();
        if let Some(relation) = relation {
            let pattern: String = relation.get(0);
            let rendered = run(
                &database.client,
                &CatalogCommand::Describe {
                    pattern: Some(pattern),
                    verbose: true,
                },
                CatalogLimits {
                    row_limit: 1000,
                    max_field_width: 500,
                },
            )
            .await
            .unwrap();
            assert!(rendered.contains("Storage"));
            assert!(rendered.contains("Total size"));
        }
        database
            .client
            .batch_execute(
                "CREATE TEMP TABLE pgline_batch_describe_one (one_marker text PRIMARY KEY); \
                 CREATE TEMP TABLE pgline_batch_describe_two \
                   (two_marker integer CONSTRAINT pgline_two_positive CHECK (two_marker > 0)); \
                 CREATE TEMP VIEW pgline_batch_describe_view AS \
                   SELECT one_marker AS view_marker FROM pgline_batch_describe_one",
            )
            .await
            .unwrap();
        let rendered = run(
            &database.client,
            &CatalogCommand::Describe {
                pattern: Some("pgline_batch_describe_*".into()),
                verbose: true,
            },
            CatalogLimits {
                row_limit: 1000,
                max_field_width: 500,
            },
        )
        .await
        .unwrap();
        let one_start = rendered.find("pgline_batch_describe_one").unwrap();
        let two_start = rendered.find("pgline_batch_describe_two").unwrap();
        let view_start = rendered.find("pgline_batch_describe_view").unwrap();
        assert!(one_start < two_start && two_start < view_start);
        let one = &rendered[one_start..two_start];
        let two = &rendered[two_start..view_start];
        let view = &rendered[view_start..];
        assert!(one.contains("one_marker"));
        assert!(one.contains("pgline_batch_describe_one_pkey"));
        assert!(!one.contains("two_marker"));
        assert!(two.contains("two_marker"));
        assert!(two.contains("pgline_two_positive"));
        assert!(!two.contains("one_marker"));
        assert!(view.contains("view_marker"));
        assert!(view.contains("View definition:"));

        database
            .client
            .batch_execute("SET standard_conforming_strings = on")
            .await
            .unwrap();
    }
}
