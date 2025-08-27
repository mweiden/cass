use std::{
    process::{Command, Stdio},
    thread,
    time::Duration,
};

use cass::rpc::{QueryRequest, cass_client::CassClient, query_response};

#[tokio::test]
async fn select_requires_partition_key() {
    let base = "http://127.0.0.1:18105";
    let dir = tempfile::tempdir().unwrap();
    let bin = env!("CARGO_BIN_EXE_cass");
    let mut child = Command::new(bin)
        .args([
            "server",
            "--data-dir",
            dir.path().to_str().unwrap(),
            "--node-addr",
            base,
            "--rf",
            "1",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    for _ in 0..20 {
        if CassClient::connect(base.to_string()).await.is_ok() {
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }

    let mut client = CassClient::connect(base.to_string()).await.unwrap();
    client
        .query(QueryRequest {
            sql: "CREATE TABLE orders (customer_id TEXT, order_id TEXT, order_date TEXT, PRIMARY KEY(customer_id, \
                order_id))".into(),
        })
        .await
        .unwrap();
    client
        .query(QueryRequest {
            sql: "INSERT INTO orders VALUES ('nike','abc123','2025-08-25')".into(),
        })
        .await
        .unwrap();
    client
        .query(QueryRequest {
            sql: "INSERT INTO orders VALUES ('nike','def456','2025-08-26')".into(),
        })
        .await
        .unwrap();

    let res = client
        .query(QueryRequest {
            sql: "SELECT * FROM orders".into(),
        })
        .await;
    assert!(res.unwrap_err().message().contains("partition key"));

    let rows = client
        .query(QueryRequest {
            sql: "SELECT * FROM orders WHERE customer_id = 'nike'".into(),
        })
        .await
        .unwrap()
        .into_inner();
    if let Some(query_response::Payload::Rows(rs)) = rows.payload {
        assert_eq!(rs.rows.len(), 2);
    } else {
        panic!("unexpected response");
    }

    let cnt = client
        .query(QueryRequest {
            sql: "SELECT COUNT(*) FROM orders WHERE customer_id = 'nike'".into(),
        })
        .await
        .unwrap()
        .into_inner();
    if let Some(query_response::Payload::Rows(rs)) = cnt.payload {
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(rs.rows[0].columns.get("count"), Some(&"2".to_string()));
    } else {
        panic!("unexpected count response");
    }

    child.kill().unwrap();
}
