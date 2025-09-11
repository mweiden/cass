use cass::storage::Storage;
use cass::storage::s3::S3Storage;

use s3s::auth::SimpleAuth;
use s3s::service::S3ServiceBuilder;
use s3s_fs::FileSystem;

use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

use std::sync::LazyLock;
use tokio::sync::Mutex;
use tokio::sync::MutexGuard;

async fn serial() -> MutexGuard<'static, ()> {
    static LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));
    LOCK.lock().await
}

async fn start_s3_server() -> (String, JoinHandle<()>, tempfile::TempDir) {
    let tmp = tempfile::tempdir().unwrap();
    let fs = FileSystem::new(tmp.path()).unwrap();
    let mut builder = S3ServiceBuilder::new(fs);
    builder.set_auth(SimpleAuth::from_single("test", "test"));
    let service = builder.build();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let handle = tokio::spawn(async move {
        let server = ConnBuilder::new(TokioExecutor::new());
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                break;
            };
            let svc = service.clone();
            if server
                .serve_connection(TokioIo::new(socket), svc)
                .await
                .is_err()
            {
                break;
            }
        }
    });

    (format!("http://{}", addr), handle, tmp)
}

#[tokio::test]
async fn s3_storage_put_and_get() {
    let _guard = serial().await;
    let (endpoint, handle, _tmp) = start_s3_server().await;

    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", "test");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
        std::env::set_var("AWS_REGION", "us-east-1");
        std::env::set_var("AWS_ENDPOINT", &endpoint);
    }

    let storage = S3Storage::new("test-bucket").await.unwrap();
    storage
        .put("nested/dir/file.txt", b"hello".to_vec())
        .await
        .unwrap();
    let data = storage.get("nested/dir/file.txt").await.unwrap();
    assert_eq!(data, b"hello");

    handle.abort();
}

#[tokio::test]
async fn s3_storage_get_missing_errors() {
    let _guard = serial().await;
    let (endpoint, handle, _tmp) = start_s3_server().await;

    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", "test");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
        std::env::set_var("AWS_REGION", "us-east-1");
        std::env::set_var("AWS_ENDPOINT", &endpoint);
    }

    let storage = S3Storage::new("test-bucket").await.unwrap();
    storage.put("exists", b"data".to_vec()).await.unwrap();
    let res = storage.get("missing").await;
    assert!(res.is_err());

    handle.abort();
}

#[tokio::test]
async fn s3_storage_append() {
    let _guard = serial().await;
    let (endpoint, handle, _tmp) = start_s3_server().await;

    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", "test");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
        std::env::set_var("AWS_REGION", "us-east-1");
        std::env::set_var("AWS_ENDPOINT", &endpoint);
    }

    let storage = S3Storage::new("test-bucket").await.unwrap();
    storage
        .append("nested/dir/file.txt", b"hello")
        .await
        .unwrap();
    storage
        .append("nested/dir/file.txt", b" world")
        .await
        .unwrap();
    let data = storage.get("nested/dir/file.txt").await.unwrap();
    assert_eq!(data, b"hello world");

    handle.abort();
}
