use cass::rpc::{PanicRequest, QueryRequest, cass_client::CassClient, query_response};
use std::time::{Duration, Instant};
use tokio::time::sleep;

mod common;
use common::{CassProcess, free_http_addr};

#[tokio::test]
async fn show_tables_with_unhealthy_replica() {
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

    let mut child2 = CassProcess::spawn([
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
    })
    .await
    .unwrap();

    // Mark the second node unhealthy and allow gossip to propagate
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
                assert_eq!(t.tables, vec!["kv".to_string()]);
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
