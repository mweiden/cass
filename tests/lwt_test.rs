use cass::lwt::Coordinator;
use cass::storage::{Storage, local::LocalStorage};
use cass::{Database, SqlEngine, query::QueryOutput};
use std::sync::Arc;

#[tokio::test]
async fn cas_insert_succeeds() {
    let lwt = Coordinator::new(3);
    let ok = lwt.compare_and_set(None, "foo").await;
    assert!(ok);
    let committed = lwt.committed_values().await;
    assert!(committed.iter().all(|v| v.as_deref() == Some("foo")));
}

#[tokio::test]
async fn cas_mismatch_fails() {
    let lwt = Coordinator::new(3);
    assert!(lwt.compare_and_set(None, "foo").await);
    let ok = lwt.compare_and_set(Some("bar"), "baz").await;
    assert!(!ok);
    let committed = lwt.committed_values().await;
    assert!(committed.iter().all(|v| v.as_deref() == Some("foo")));
}

#[tokio::test]
async fn sql_conditional_writes() {
    let tmp = tempfile::tempdir().unwrap();
    let storage: Arc<dyn Storage> = Arc::new(LocalStorage::new(tmp.path()));
    let db = Database::new(storage, "wal.log").await.unwrap();
    let engine = SqlEngine::new();

    engine
        .execute(&db, "CREATE TABLE kv (k TEXT, v TEXT, PRIMARY KEY(k))")
        .await
        .unwrap();

    let res = engine
        .execute(&db, "INSERT INTO kv (k, v) VALUES ('a','1') IF NOT EXISTS")
        .await
        .unwrap();
    match res {
        QueryOutput::Mutation { count, .. } => assert_eq!(count, 1),
        _ => panic!("unexpected"),
    }

    let res = engine
        .execute(&db, "INSERT INTO kv (k, v) VALUES ('a','2') IF NOT EXISTS")
        .await
        .unwrap();
    match res {
        QueryOutput::Mutation { count, .. } => assert_eq!(count, 0),
        _ => panic!("unexpected"),
    }

    let res = engine
        .execute(&db, "UPDATE kv SET v='3' WHERE k='a' IF v='1'")
        .await
        .unwrap();
    match res {
        QueryOutput::Mutation { count, .. } => assert_eq!(count, 1),
        _ => panic!("unexpected"),
    }

    let res = engine
        .execute(&db, "SELECT v FROM kv WHERE k='a'")
        .await
        .unwrap();
    match res {
        QueryOutput::Rows(rows) => {
            assert_eq!(rows[0].get("v"), Some(&"3".to_string()));
        }
        _ => panic!("unexpected"),
    }

    let res = engine
        .execute(&db, "UPDATE kv SET v='4' WHERE k='a' IF v='1'")
        .await
        .unwrap();
    match res {
        QueryOutput::Mutation { count, .. } => assert_eq!(count, 0),
        _ => panic!("unexpected"),
    }
}
