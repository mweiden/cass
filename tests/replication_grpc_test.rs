use std::time::Duration;
use tokio::time::sleep;

use cass::rpc::{QueryRequest, cass_client::CassClient, query_response};

mod common;
use common::{CassProcess, free_http_addr};

#[tokio::test]
async fn union_and_lww_across_replicas() {
    let base1 = free_http_addr();
    let base2 = free_http_addr();
    let dir1 = tempfile::tempdir().unwrap();
    let dir2 = tempfile::tempdir().unwrap();

    let _child1 = CassProcess::spawn([
        "server",
        "--data-dir",
        dir1.path().to_str().unwrap(),
        "--node-addr",
        &base1,
        "--peer",
        &base2,
        "--rf",
        "2",
    ]);

    let _child2 = CassProcess::spawn([
        "server",
        "--data-dir",
        dir2.path().to_str().unwrap(),
        "--node-addr",
        &base2,
        "--peer",
        &base1,
        "--rf",
        "2",
    ]);

    for _ in 0..20 {
        let ok1 = CassClient::connect(base1.clone()).await.is_ok();
        let ok2 = CassClient::connect(base2.clone()).await.is_ok();
        if ok1 && ok2 {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }

    let mut c1 = CassClient::connect(base1.clone()).await.unwrap();
    let mut c2 = CassClient::connect(base2.clone()).await.unwrap();

    c1.query(QueryRequest {
        sql: "CREATE TABLE kv (id TEXT, val TEXT, PRIMARY KEY(id))".into(),
        ts: 0,
    })
    .await
    .unwrap();

    c1.internal(QueryRequest {
        sql: "INSERT INTO kv (id, val) VALUES ('a','va1')".into(),
        ts: 1,
    })
    .await
    .unwrap();
    c2.internal(QueryRequest {
        sql: "INSERT INTO kv (id, val) VALUES ('a','va2')".into(),
        ts: 2,
    })
    .await
    .unwrap();
    c2.internal(QueryRequest {
        sql: "INSERT INTO kv (id, val) VALUES ('b','vb')".into(),
        ts: 3,
    })
    .await
    .unwrap();

    let res_a = c1
        .query(QueryRequest {
            sql: "SELECT val FROM kv WHERE id = 'a'".into(),
            ts: 0,
        })
        .await
        .unwrap()
        .into_inner();
    match res_a.payload {
        Some(query_response::Payload::Rows(rs)) => {
            let last = rs.rows.last().and_then(|r| r.columns.get("val"));
            assert_eq!(last, Some(&"va2".to_string()));
        }
        _ => panic!("unexpected"),
    }

    let res_b = c1
        .query(QueryRequest {
            sql: "SELECT val FROM kv WHERE id = 'b'".into(),
            ts: 0,
        })
        .await
        .unwrap()
        .into_inner();
    match res_b.payload {
        Some(query_response::Payload::Rows(rs)) => {
            assert_eq!(rs.rows[0].columns.get("val"), Some(&"vb".to_string()));
        }
        _ => panic!("unexpected"),
    }

    let res_c = c1
        .query(QueryRequest {
            sql: "SELECT val FROM kv WHERE id IN ('a', 'b')".into(),
            ts: 0,
        })
        .await
        .unwrap()
        .into_inner();
    match res_c.payload {
        Some(query_response::Payload::Rows(rs)) => {
            assert_eq!(rs.rows.len(), 2);
        }
        _ => panic!("unexpected"),
    }

    let cnt = c1
        .query(QueryRequest {
            sql: "SELECT COUNT(*) FROM kv WHERE id = 'a'".into(),
            ts: 0,
        })
        .await
        .unwrap()
        .into_inner();
    match cnt.payload {
        Some(query_response::Payload::Rows(rs)) => {
            assert_eq!(rs.rows.len(), 1);
            assert_eq!(rs.rows[0].columns.get("count"), Some(&"1".to_string()));
        }
        _ => panic!("unexpected count"),
    }

    let ack = c1
        .query(QueryRequest {
            sql: "INSERT INTO kv (id, val) VALUES ('x','1'),('y','2')".into(),
            ts: 0,
        })
        .await
        .unwrap()
        .into_inner();
    match ack.payload {
        Some(query_response::Payload::Mutation(m)) => {
            assert_eq!(m.op, "INSERT");
            assert_eq!(m.count, 2);
        }
        _ => panic!("unexpected ack"),
    }
}
