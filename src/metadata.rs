use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, RwLock},
};

use futures_util::{StreamExt, pin_mut};
use tokio_postgres::Client;

use crate::{error::Result, output};

const SCHEMA_LIMIT: usize = 1_000;
const RELATION_LIMIT: usize = 5_000;
const COLUMN_LIMIT: usize = 50_000;

#[derive(Clone, Copy)]
struct MetadataLimits {
    schemas: usize,
    relations: usize,
    columns: usize,
}

const METADATA_LIMITS: MetadataLimits = MetadataLimits {
    schemas: SCHEMA_LIMIT,
    relations: RELATION_LIMIT,
    columns: COLUMN_LIMIT,
};

#[derive(Debug, Clone, Default)]
pub struct Metadata {
    pub schemas: Vec<String>,
    pub relations: Vec<String>,
    pub columns: Vec<String>,
    pub relation_columns: HashMap<String, Vec<String>>,
    pub truncated: bool,
}

#[derive(Clone, Default)]
pub struct MetadataStore {
    current: Arc<RwLock<Metadata>>,
}

impl MetadataStore {
    pub fn replace(&self, metadata: Metadata) {
        *self.current.write().expect("metadata lock poisoned") = metadata;
    }

    pub fn with_current<T>(&self, read: impl FnOnce(&Metadata) -> T) -> T {
        read(&self.current.read().expect("metadata lock poisoned"))
    }
}

fn has_unsafe_terminal_characters(value: &str) -> bool {
    value.chars().any(output::is_unsafe_terminal_character)
}

impl Metadata {
    pub async fn load(client: &Client) -> Result<Self> {
        Self::load_with_limits(client, METADATA_LIMITS).await
    }

    async fn load_with_limits(client: &Client, limits: MetadataLimits) -> Result<Self> {
        // Ask PostgreSQL to render each component independently. Joining raw
        // names first would lose the distinction between qualification and a
        // literal dot inside an identifier.
        let mut truncated = false;
        let schema_query_limit = (limits.schemas + 1) as i64;
        let schema_rows = client
            .query_raw(
                r#"
                SELECT pg_catalog.format('%I', n.nspname)::text
                FROM pg_catalog.pg_namespace n
                WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
                  AND n.nspname !~ '^pg_toast'
                ORDER BY n.nspname = ANY(pg_catalog.current_schemas(false)) DESC,
                         n.nspname
                LIMIT $1
                "#,
                [&schema_query_limit],
            )
            .await?;
        pin_mut!(schema_rows);
        let mut schemas = Vec::new();
        let mut schema_count = 0;
        while let Some(row) = schema_rows.next().await {
            if schema_count == limits.schemas {
                truncated = true;
                break;
            }
            schema_count += 1;
            let schema: String = row?.get(0);
            if !has_unsafe_terminal_characters(&schema) {
                schemas.push(schema);
            }
        }

        let mut relations = BTreeSet::new();
        let mut relation_columns: HashMap<String, Vec<String>> = HashMap::new();
        let mut relation_names = HashMap::new();
        let relation_query_limit = (limits.relations + 1) as i64;
        let relation_rows = client
            .query_raw(
                r#"
                SELECT c.oid, pg_catalog.format('%I', n.nspname)::text,
                       pg_catalog.format('%I', c.relname)::text,
                       pg_catalog.pg_table_is_visible(c.oid)
                FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relkind IN ('r', 'p', 'v', 'm', 'f')
                  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                  AND n.nspname !~ '^pg_toast'
                ORDER BY pg_catalog.pg_table_is_visible(c.oid) DESC,
                         n.nspname, c.relname
                LIMIT $1
                "#,
                [&relation_query_limit],
            )
            .await?;
        pin_mut!(relation_rows);
        let mut relation_oids = Vec::new();
        let mut relation_count = 0;
        while let Some(row) = relation_rows.next().await {
            if relation_count == limits.relations {
                truncated = true;
                break;
            }
            relation_count += 1;
            let row = row?;
            let oid: u32 = row.get(0);
            let schema: String = row.get(1);
            let relation: String = row.get(2);
            let visible: bool = row.get(3);
            if has_unsafe_terminal_characters(&schema) || has_unsafe_terminal_characters(&relation)
            {
                continue;
            }
            let qualified_relation = format!("{schema}.{relation}");
            let unqualified_relation = visible.then_some(relation);
            if let Some(relation) = &unqualified_relation {
                relations.insert(relation.clone());
                relation_columns.entry(relation.clone()).or_default();
            }
            relations.insert(qualified_relation.clone());
            relation_columns
                .entry(qualified_relation.clone())
                .or_default();
            relation_names.insert(oid, (qualified_relation, unqualified_relation));
            relation_oids.push(oid);
        }

        let mut columns = BTreeSet::new();
        let column_query_limit = (limits.columns + 1) as i64;
        let column_rows = client
            .query_raw(
                r#"
                SELECT a.attrelid, pg_catalog.format('%I', a.attname)::text
                FROM pg_catalog.pg_attribute a
                JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relkind IN ('r', 'p', 'v', 'm', 'f')
                  AND a.attnum > 0 AND NOT a.attisdropped
                  AND a.attrelid = ANY($1)
                ORDER BY pg_catalog.array_position($1, a.attrelid), a.attnum
                LIMIT $2
                "#,
                [
                    &relation_oids as &(dyn tokio_postgres::types::ToSql + Sync),
                    &column_query_limit,
                ],
            )
            .await?;
        pin_mut!(column_rows);
        let mut column_count = 0;
        while let Some(row) = column_rows.next().await {
            if column_count == limits.columns {
                truncated = true;
                break;
            }
            let row = row?;
            column_count += 1;
            let oid: u32 = row.get(0);
            let column: String = row.get(1);
            if has_unsafe_terminal_characters(&column) {
                continue;
            }
            let Some((qualified_relation, unqualified_relation)) = relation_names.get(&oid) else {
                continue;
            };
            columns.insert(column.clone());
            if let Some(relation) = unqualified_relation {
                relation_columns
                    .entry(relation.clone())
                    .or_default()
                    .push(column.clone());
            }
            // PostgreSQL guarantees unique live attribute names per relation,
            // so preserving query order also preserves physical column order.
            relation_columns
                .entry(qualified_relation.clone())
                .or_default()
                .push(column);
        }

        Ok(Self {
            schemas,
            relations: relations.into_iter().collect(),
            columns: columns.into_iter().collect(),
            relation_columns,
            truncated,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore = "requires PGLINE_TEST_URL"]
    async fn truncated_metadata_prioritizes_search_path_relations() {
        let database = crate::test_support::connect().await;
        database
            .client
            .batch_execute(
                "BEGIN; \
                 CREATE SCHEMA pgline_priority_a; \
                 CREATE SCHEMA pgline_priority_z; \
                 CREATE TABLE pgline_priority_a.hidden(hidden_column int); \
                 CREATE TABLE pgline_priority_z.visible(first_column int, second_column int); \
                 SET LOCAL search_path = pgline_priority_z",
            )
            .await
            .unwrap();

        let metadata = Metadata::load_with_limits(
            &database.client,
            MetadataLimits {
                schemas: 1,
                relations: 1,
                columns: 1,
            },
        )
        .await
        .unwrap();
        assert_eq!(metadata.schemas, ["pgline_priority_z"]);
        assert!(metadata.relations.contains(&"visible".into()));
        assert_eq!(
            metadata.relation_columns.get("visible").unwrap(),
            &["first_column"]
        );
        assert!(metadata.truncated);
        database.client.batch_execute("ROLLBACK").await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires PGLINE_TEST_URL"]
    async fn qualified_columns_preserve_table_column_order() {
        let database = crate::test_support::connect().await;
        database
            .client
            .batch_execute(
                "BEGIN; CREATE TEMP TABLE pgline_column_order_test \
                 (z_first integer, a_second integer, m_third integer)",
            )
            .await
            .unwrap();

        let metadata = Metadata::load(&database.client).await.unwrap();
        database.client.batch_execute("ROLLBACK").await.unwrap();

        assert_eq!(
            metadata.relation_columns["pgline_column_order_test"],
            ["z_first", "a_second", "m_third"]
        );
    }

    #[tokio::test]
    #[ignore = "requires PGLINE_TEST_URL"]
    async fn unqualified_columns_only_use_the_visible_relation() {
        let database = crate::test_support::connect().await;
        database
            .client
            .batch_execute(
                r#"BEGIN;
                 CREATE SCHEMA pi_visible_a;
                 CREATE SCHEMA pi_hidden_b;
                 CREATE SCHEMA "Empty.Schema";
                 CREATE TABLE pi_visible_a.users(visible_column int);
                 CREATE TABLE pi_visible_a.empty();
                 CREATE TABLE pi_visible_a."Order.Items"("select" int, "CamelCase" int, "a""b" int);
                 CREATE TABLE pi_hidden_b.users(hidden_column int);
                 SET LOCAL search_path = pi_visible_a, pi_hidden_b;"#,
            )
            .await
            .unwrap();

        let metadata = Metadata::load(&database.client).await.unwrap();
        assert_eq!(
            metadata.relation_columns.get("users").unwrap(),
            &["visible_column"]
        );
        assert_eq!(
            metadata.relation_columns.get("pi_hidden_b.users").unwrap(),
            &["hidden_column"]
        );
        assert!(metadata.schemas.contains(&"\"Empty.Schema\"".into()));
        assert!(metadata.relations.contains(&"empty".into()));
        assert!(
            metadata
                .relations
                .contains(&"pi_visible_a.\"Order.Items\"".into())
        );
        assert_eq!(
            metadata
                .relation_columns
                .get("pi_visible_a.\"Order.Items\"")
                .unwrap(),
            &["\"select\"", "\"CamelCase\"", "\"a\"\"b\""]
        );
        database.client.batch_execute("ROLLBACK").await.unwrap();
    }
}
