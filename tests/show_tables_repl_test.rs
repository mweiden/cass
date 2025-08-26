use cass::rpc::{QueryRequest, cass_client::CassClient, query_response};
use std::{
    process::{Command, Stdio},
    thread,
    time::Duration,
};

#[tokio::test]
async fn show_tables_via_grpc() {
    let _ = std::fs::remove_dir_all("/tmp/cass-data");
    let mut child = Command::new(env!("CARGO_BIN_EXE_cass"))
        .arg("server")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn server");

    let base = "http://127.0.0.1:8080".to_string();
    for _ in 0..10 {
        if CassClient::connect(base.clone()).await.is_ok() {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }

    let mut client = CassClient::connect(base.clone()).await.unwrap();
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

    child.kill().unwrap();
}
