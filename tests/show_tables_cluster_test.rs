use cass::rpc::{PanicRequest, QueryRequest, cass_client::CassClient, query_response};
use std::{
    process::{Command, Stdio},
    thread,
    time::Duration,
};

#[tokio::test]
async fn show_tables_with_unhealthy_replica() {
    let base1 = "http://127.0.0.1:18081";
    let base2 = "http://127.0.0.1:18082";
    let dir1 = tempfile::tempdir().unwrap();
    let dir2 = tempfile::tempdir().unwrap();
    let bin = env!("CARGO_BIN_EXE_cass");

    let mut child1 = Command::new(bin)
        .args([
            "server",
            "--data-dir",
            dir1.path().to_str().unwrap(),
            "--node-addr",
            base1,
            "--peer",
            base2,
            "--rf",
            "2",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    let mut child2 = Command::new(bin)
        .args([
            "server",
            "--data-dir",
            dir2.path().to_str().unwrap(),
            "--node-addr",
            base2,
            "--peer",
            base1,
            "--rf",
            "2",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    for _ in 0..20 {
        let ok1 = CassClient::connect(base1.to_string()).await.is_ok();
        let ok2 = CassClient::connect(base2.to_string()).await.is_ok();
        if ok1 && ok2 {
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }

    let mut c1 = CassClient::connect(base1.to_string()).await.unwrap();
    let mut c2 = CassClient::connect(base2.to_string()).await.unwrap();

    c1.query(QueryRequest {
        sql: "CREATE TABLE kv (id TEXT, val TEXT, PRIMARY KEY(id))".into(),
    })
    .await
    .unwrap();

    // Mark the second node unhealthy and allow gossip to propagate
    c2.panic(PanicRequest {}).await.unwrap();
    thread::sleep(Duration::from_secs(2));

    let res = c1
        .query(QueryRequest {
            sql: "SHOW TABLES".into(),
        })
        .await
        .unwrap()
        .into_inner();
    match res.payload {
        Some(query_response::Payload::Tables(t)) => {
            assert_eq!(t.tables, vec!["kv".to_string()]);
        }
        _ => panic!("unexpected"),
    }

    child1.kill().unwrap();
    child2.kill().unwrap();
}
