use cass::rpc::{QueryRequest, cass_client::CassClient, query_response};
use std::time::Duration;
use tokio::time::sleep;

mod common;
use common::CassProcess;

#[tokio::test]
async fn show_tables_via_grpc() {
    let dir = tempfile::tempdir().unwrap();
    let base = "http://127.0.0.1:18121";
    let _child = CassProcess::spawn([
        "server",
        "--data-dir",
        dir.path().to_str().unwrap(),
        "--node-addr",
        base,
        "--rf",
        "1",
    ]);

    for _ in 0..20 {
        if CassClient::connect(base.to_string()).await.is_ok() {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }

    let mut client = CassClient::connect(base.to_string()).await.unwrap();
    client
        .query(QueryRequest {
            sql: "CREATE TABLE kv (id TEXT, val TEXT, PRIMARY KEY(id))".into(),
        })
        .await
        .unwrap();

    let res = client
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
}
