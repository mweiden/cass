use cass::{
    Database, SqlEngine,
    query::{QueryError, QueryOutput},
    storage::{Storage, local::LocalStorage},
};
use std::sync::Arc;

async fn setup_table() -> (Database, SqlEngine) {
    let dir = tempfile::tempdir().unwrap();
    let storage: Arc<dyn Storage> = Arc::new(LocalStorage::new(dir.path()));
    // keep the tempdir alive for the duration of the test
    std::mem::forget(dir);
    let db = Database::new(storage, "wal.log").await.unwrap();
    let engine = SqlEngine::new();
    engine
        .execute(
            &db,
            "CREATE TABLE items (pk TEXT, ck TEXT, score TEXT, PRIMARY KEY(pk, ck))",
        )
        .await
        .unwrap();
    for (ck, score) in [("a", "10"), ("b", "2"), ("c", "30"), ("d", "4")] {
        engine
            .execute(
                &db,
                &format!("INSERT INTO items (pk, ck, score) VALUES ('p', '{ck}', '{score}')"),
            )
            .await
            .unwrap();
    }
    (db, engine)
}

fn rows(out: QueryOutput) -> Vec<std::collections::BTreeMap<String, String>> {
    match out {
        QueryOutput::Rows(rows) => rows,
        _ => panic!("expected row output"),
    }
}

fn column(rows: &[std::collections::BTreeMap<String, String>], col: &str) -> Vec<String> {
    rows.iter().map(|r| r[col].clone()).collect()
}

#[tokio::test]
async fn order_by_ascending_and_descending() {
    let (db, engine) = setup_table().await;
    let out = rows(
        engine
            .execute(&db, "SELECT * FROM items WHERE pk = 'p' ORDER BY ck ASC")
            .await
            .unwrap(),
    );
    assert_eq!(column(&out, "ck"), vec!["a", "b", "c", "d"]);

    let out = rows(
        engine
            .execute(&db, "SELECT * FROM items WHERE pk = 'p' ORDER BY ck DESC")
            .await
            .unwrap(),
    );
    assert_eq!(column(&out, "ck"), vec!["d", "c", "b", "a"]);
}

#[tokio::test]
async fn order_by_compares_numbers_numerically() {
    let (db, engine) = setup_table().await;
    let out = rows(
        engine
            .execute(&db, "SELECT * FROM items WHERE pk = 'p' ORDER BY score")
            .await
            .unwrap(),
    );
    // String comparison would yield 10 < 2 < 30 < 4.
    assert_eq!(column(&out, "score"), vec!["2", "4", "10", "30"]);
}

#[tokio::test]
async fn limit_truncates_results() {
    let (db, engine) = setup_table().await;
    let out = rows(
        engine
            .execute(
                &db,
                "SELECT * FROM items WHERE pk = 'p' ORDER BY ck LIMIT 2",
            )
            .await
            .unwrap(),
    );
    assert_eq!(column(&out, "ck"), vec!["a", "b"]);

    let out = rows(
        engine
            .execute(
                &db,
                "SELECT * FROM items WHERE pk = 'p' ORDER BY ck LIMIT 0",
            )
            .await
            .unwrap(),
    );
    assert!(out.is_empty());
}

#[tokio::test]
async fn limit_with_offset_pages_results() {
    let (db, engine) = setup_table().await;
    let out = rows(
        engine
            .execute(
                &db,
                "SELECT * FROM items WHERE pk = 'p' ORDER BY ck LIMIT 2 OFFSET 1",
            )
            .await
            .unwrap(),
    );
    assert_eq!(column(&out, "ck"), vec!["b", "c"]);

    // Offset past the end yields no rows rather than an error.
    let out = rows(
        engine
            .execute(
                &db,
                "SELECT * FROM items WHERE pk = 'p' ORDER BY ck LIMIT 2 OFFSET 100",
            )
            .await
            .unwrap(),
    );
    assert!(out.is_empty());
}

#[tokio::test]
async fn order_by_multiple_columns() {
    let (db, engine) = setup_table().await;
    // Two rows share the same score so the secondary key decides.
    engine
        .execute(
            &db,
            "INSERT INTO items (pk, ck, score) VALUES ('p', 'e', '2')",
        )
        .await
        .unwrap();
    let out = rows(
        engine
            .execute(
                &db,
                "SELECT * FROM items WHERE pk = 'p' ORDER BY score ASC, ck DESC",
            )
            .await
            .unwrap(),
    );
    assert_eq!(column(&out, "ck"), vec!["e", "b", "d", "a", "c"]);
}

#[tokio::test]
async fn unsupported_order_expression_is_rejected_not_ignored() {
    let (db, engine) = setup_table().await;
    // Ordering by an arbitrary expression is not supported; it must surface
    // as an error rather than returning silently unsorted rows.
    let res = engine
        .execute(
            &db,
            "SELECT * FROM items WHERE pk = 'p' ORDER BY ck || 'x'",
        )
        .await;
    assert!(matches!(res, Err(QueryError::Unsupported)));
}

#[tokio::test]
async fn order_by_missing_column_sorts_nulls_last() {
    let (db, engine) = setup_table().await;
    // Row without a score: insert only the key columns.
    engine
        .execute(&db, "INSERT INTO items (pk, ck) VALUES ('p', 'z')")
        .await
        .unwrap();
    let out = rows(
        engine
            .execute(&db, "SELECT * FROM items WHERE pk = 'p' ORDER BY score")
            .await
            .unwrap(),
    );
    assert_eq!(out.last().unwrap()["ck"], "z");
}
