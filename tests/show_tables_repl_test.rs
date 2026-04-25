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

    // Wait until the server can actually handle gRPC requests. Under
    // coverage-instrumented builds the server may accept TCP connections before
    // its gRPC stack is fully initialised, producing a broken-pipe error on the
    // first real call if we only probe at the transport level.
    let mut client = 'ready: {
        for _ in 0..50 {
            if let Ok(mut c) = CassClient::connect(base.to_string()).await {
                if c.query(QueryRequest {
                    sql: "SHOW TABLES".into(),
                    ts: 0,
                })
                .await
                .is_ok()
                {
                    break 'ready c;
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
        panic!("server did not become ready within 5 s");
    };
    client
        .query(QueryRequest {
            sql: "CREATE TABLE kv (id TEXT, val TEXT, PRIMARY KEY(id))".into(),
            ts: 0,
        })
        .await
        .unwrap();

    let res = client
        .query(QueryRequest {
            sql: "SHOW TABLES".into(),
            ts: 0,
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
