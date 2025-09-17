use cass::rpc::{QueryRequest, cass_client::CassClient, query_response};
use tokio::time::{Duration, sleep};

mod common;
use common::CassProcess;

#[tokio::test]
async fn lwt_update_on_orders_distributed_all_read_consistency() {
    let base1 = "http://127.0.0.1:18211";
    let base2 = "http://127.0.0.1:18212";
    let dir1 = tempfile::tempdir().unwrap();
    let dir2 = tempfile::tempdir().unwrap();

    let _c1 = CassProcess::spawn([
        "server",
        "--data-dir",
        dir1.path().to_str().unwrap(),
        "--node-addr",
        base1,
        "--peer",
        base2,
        "--rf",
        "2",
        "--read-consistency",
        "all",
    ]);
    let _c2 = CassProcess::spawn([
        "server",
        "--data-dir",
        dir2.path().to_str().unwrap(),
        "--node-addr",
        base2,
        "--peer",
        base1,
        "--rf",
        "2",
        "--read-consistency",
        "all",
    ]);

    for _ in 0..40 {
        let ok1 = CassClient::connect(base1.to_string()).await.is_ok();
        let ok2 = CassClient::connect(base2.to_string()).await.is_ok();
        if ok1 && ok2 {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }

    sleep(Duration::from_secs(2)).await;

    let mut client = CassClient::connect(base1.to_string()).await.unwrap();
    client
        .query(QueryRequest { sql: "CREATE TABLE orders (customer_id TEXT, order_id TEXT, order_date TEXT, PRIMARY KEY(customer_id, order_id))".into() })
        .await
        .unwrap();
    client
        .query(QueryRequest {
            sql: "INSERT INTO orders VALUES ('nike','abc123','2025-08-27')".into(),
        })
        .await
        .unwrap();
    client
        .query(QueryRequest {
            sql: "INSERT INTO orders VALUES ('nike','def456','2025-08-26')".into(),
        })
        .await
        .unwrap();

    // Successful LWT update when condition matches
    let resp = client
        .query(QueryRequest { sql: "UPDATE orders SET order_date='2025-08-25' WHERE customer_id='nike' AND order_id='abc123' IF order_date='2025-08-27'".into() })
        .await
        .unwrap()
        .into_inner();
    if let Some(query_response::Payload::Rows(rs)) = resp.payload {
        let row = &rs.rows[0].columns;
        assert_eq!(row.get("[applied]"), Some(&"true".to_string()));
    } else {
        panic!("unexpected response for successful LWT");
    }

    // Failed LWT update when condition no longer matches; should include current value
    let resp2 = client
        .query(QueryRequest { sql: "UPDATE orders SET order_date='2025-08-24' WHERE customer_id='nike' AND order_id='abc123' IF order_date='2025-08-27'".into() })
        .await
        .unwrap()
        .into_inner();
    if let Some(query_response::Payload::Rows(rs)) = resp2.payload {
        let row = &rs.rows[0].columns;
        assert_eq!(row.get("[applied]"), Some(&"false".to_string()));
        assert_eq!(row.get("order_date"), Some(&"2025-08-25".to_string()));
    } else {
        panic!("unexpected response for failed LWT");
    }

    // Verify actual DB value remains '2025-08-25' after first success
    let resp3 = client
        .query(QueryRequest {
            sql: "SELECT order_date FROM orders WHERE customer_id='nike' AND order_id='abc123'"
                .into(),
        })
        .await
        .unwrap()
        .into_inner();
    if let Some(query_response::Payload::Rows(rs)) = resp3.payload {
        let row = &rs.rows[0].columns;
        assert_eq!(row.get("order_date"), Some(&"2025-08-25".to_string()));
    } else {
        panic!("unexpected select response");
    }
}
