use cass::rpc::{QueryRequest, cass_client::CassClient, query_response};
use std::time::Duration;
use tokio::time::sleep;
use tonic::transport::Channel;

mod common;
use common::CassProcess;

async fn basic_queries(client: &mut CassClient<Channel>) {
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
}

async fn show_tables(client: &mut CassClient<Channel>) {
    let res = client
        .query(QueryRequest {
            sql: "SHOW TABLES".into(),
        })
        .await
        .unwrap()
        .into_inner();
    match res.payload {
        Some(query_response::Payload::Tables(t)) => {
            assert!(t.tables.contains(&"kv".to_string()));
        }
        _ => panic!("unexpected"),
    }
}

async fn partition_key_requirement(client: &mut CassClient<Channel>) {
    client
        .query(QueryRequest {
            sql: "CREATE TABLE orders (customer_id TEXT, order_id TEXT, order_date TEXT, PRIMARY KEY(customer_id, order_id))"
                .into(),
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
}

#[tokio::test]
async fn grpc_query_suite() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let base = "http://127.0.0.1:8080";
    let _child = CassProcess::spawn([
        "server",
        "--node-addr",
        base,
        "--data-dir",
        tmp_dir.path().to_str().unwrap(),
    ]);
    for _ in 0..10 {
        if CassClient::connect(base).await.is_ok() {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    let mut client = CassClient::connect(base).await.unwrap();
    basic_queries(&mut client).await;
    show_tables(&mut client).await;
    partition_key_requirement(&mut client).await;
}
