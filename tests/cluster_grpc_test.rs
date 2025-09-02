use cass::rpc::{PanicRequest, QueryRequest, cass_client::CassClient, query_response};
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tonic::transport::Channel;

mod common;
use common::CassProcess;

async fn replication_scenarios(c1: &mut CassClient<Channel>, c2: &mut CassClient<Channel>) {
    c1.internal(QueryRequest {
        sql: "--ts:1\nINSERT INTO kv (id, val) VALUES ('a','va1')".into(),
    })
    .await
    .unwrap();
    c2.internal(QueryRequest {
        sql: "--ts:2\nINSERT INTO kv (id, val) VALUES ('a','va2')".into(),
    })
    .await
    .unwrap();
    c2.internal(QueryRequest {
        sql: "--ts:3\nINSERT INTO kv (id, val) VALUES ('b','vb')".into(),
    })
    .await
    .unwrap();
    let res_a = c1
        .query(QueryRequest {
            sql: "SELECT val FROM kv WHERE id = 'a'".into(),
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

async fn read_repair(c1: &mut CassClient<Channel>, c2: &mut CassClient<Channel>) {
    c1.internal(QueryRequest {
        sql: "--ts:2\nINSERT INTO kv (id, val) VALUES ('r','new')".into(),
    })
    .await
    .unwrap();
    c2.internal(QueryRequest {
        sql: "--ts:1\nINSERT INTO kv (id, val) VALUES ('r','old')".into(),
    })
    .await
    .unwrap();
    c1.query(QueryRequest {
        sql: "SELECT val FROM kv WHERE id = 'r'".into(),
    })
    .await
    .unwrap();
    let resp = c2
        .query(QueryRequest {
            sql: "SELECT val FROM kv WHERE id = 'r'".into(),
        })
        .await
        .unwrap()
        .into_inner();
    if let Some(query_response::Payload::Rows(rs)) = resp.payload {
        assert_eq!(rs.rows[0].columns.get("val"), Some(&"new".to_string()));
    } else {
        panic!("unexpected response");
    }
}

async fn show_tables_with_unhealthy_replica(
    c1: &mut CassClient<Channel>,
    base2: &str,
    child2: &mut CassProcess,
) {
    let mut c2 = CassClient::connect(base2.to_string()).await.unwrap();
    c2.panic(PanicRequest {}).await.unwrap();
    let start = Instant::now();
    loop {
        let res = c1
            .query(QueryRequest {
                sql: "SHOW TABLES".into(),
            })
            .await;
        if let Ok(resp) = res {
            let inner = resp.into_inner();
            if let Some(query_response::Payload::Tables(t)) = inner.payload {
                assert!(t.tables.contains(&"kv".to_string()));
                break;
            }
        }
        if start.elapsed() > Duration::from_secs(2) {
            panic!("SHOW TABLES did not succeed in time");
        }
        sleep(Duration::from_millis(50)).await;
    }
    child2.kill();
}

#[tokio::test]
async fn cluster_end_to_end() {
    let base1 = "http://127.0.0.1:18081";
    let base2 = "http://127.0.0.1:18082";
    let dir1 = tempfile::tempdir().unwrap();
    let dir2 = tempfile::tempdir().unwrap();
    let mut child1 = CassProcess::spawn([
        "server",
        "--data-dir",
        dir1.path().to_str().unwrap(),
        "--node-addr",
        base1,
        "--peer",
        base2,
        "--rf",
        "2",
    ]);
    let mut child2 = CassProcess::spawn([
        "server",
        "--data-dir",
        dir2.path().to_str().unwrap(),
        "--node-addr",
        base2,
        "--peer",
        base1,
        "--rf",
        "2",
    ]);
    for _ in 0..20 {
        let ok1 = CassClient::connect(base1.to_string()).await.is_ok();
        let ok2 = CassClient::connect(base2.to_string()).await.is_ok();
        if ok1 && ok2 {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
    let mut c1 = CassClient::connect(base1.to_string()).await.unwrap();
    let mut c2 = CassClient::connect(base2.to_string()).await.unwrap();
    c1.query(QueryRequest {
        sql: "CREATE TABLE kv (id TEXT, val TEXT, PRIMARY KEY(id))".into(),
    })
    .await
    .unwrap();
    replication_scenarios(&mut c1, &mut c2).await;
    read_repair(&mut c1, &mut c2).await;
    show_tables_with_unhealthy_replica(&mut c1, base2, &mut child2).await;
    child1.kill();
}
