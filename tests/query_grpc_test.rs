use cass::rpc::{QueryRequest, cass_client::CassClient, query_response};
use std::{
    process::{Command, Stdio},
    thread,
    time::Duration,
};

#[tokio::test]
async fn grpc_query_roundtrip() {
    let tmp_dir = tempfile::tempdir().unwrap();

    let bin = env!("CARGO_BIN_EXE_cass");
    let base = "http://127.0.0.1:8080";
    let mut child = Command::new(bin)
        .args([
            "server",
            "--data-dir",
            tmp_dir.path().to_str().unwrap(),
            "--node-addr",
            base,
            "--rf",
            "1",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    for _ in 0..10 {
        if CassClient::connect(base).await.is_ok() {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }

    let mut client = CassClient::connect(base).await.unwrap();
    client
        .query(QueryRequest {
            sql: "CREATE TABLE kv (id TEXT, val TEXT, PRIMARY KEY(id))".into(),
        })
        .await
        .unwrap();

    client
        .query(QueryRequest {
            sql: "INSERT INTO kv (id, val) VALUES ('foo','bar')".into(),
        })
        .await
        .unwrap();

    let res = client
        .query(QueryRequest {
            sql: "SELECT val FROM kv WHERE id = 'foo'".into(),
        })
        .await
        .unwrap()
        .into_inner();
    match res.payload {
        Some(query_response::Payload::Rows(rs)) => {
            assert_eq!(rs.rows.len(), 1);
            assert_eq!(rs.rows[0].columns.get("val"), Some(&"bar".to_string()));
        }
        _ => panic!("unexpected response"),
    }

    let count = client
        .query(QueryRequest {
            sql: "SELECT COUNT(*) FROM kv WHERE id = 'foo'".into(),
        })
        .await
        .unwrap()
        .into_inner();
    match count.payload {
        Some(query_response::Payload::Rows(rs)) => {
            assert_eq!(rs.rows.len(), 1);
            assert_eq!(rs.rows[0].columns.get("count"), Some(&"1".to_string()));
        }
        _ => panic!("unexpected count response"),
    }

    child.kill().unwrap();
}
