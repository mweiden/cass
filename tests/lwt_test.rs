use cass::lwt::{Coordinator, ReplicaStore};
use cass::rpc::{
    FlushRequest, FlushResponse, HealthRequest, HealthResponse, LwtCommitRequest,
    LwtCommitResponse, LwtPrepareRequest, LwtPrepareResponse, LwtProposeRequest,
    LwtProposeResponse, LwtReadRequest, LwtReadResponse, PanicRequest, PanicResponse, QueryRequest,
    QueryResponse,
    cass_server::{Cass, CassServer},
};
use std::{net::SocketAddr, sync::Arc};
use tonic::transport::Server;
use tonic::{Request, Response, Status};

struct TestService {
    store: Arc<ReplicaStore>,
}

#[tonic::async_trait]
impl Cass for TestService {
    async fn query(&self, _req: Request<QueryRequest>) -> Result<Response<QueryResponse>, Status> {
        Err(Status::unimplemented("query"))
    }
    async fn internal(
        &self,
        _req: Request<QueryRequest>,
    ) -> Result<Response<QueryResponse>, Status> {
        Err(Status::unimplemented("internal"))
    }
    async fn flush(&self, _req: Request<FlushRequest>) -> Result<Response<FlushResponse>, Status> {
        Ok(Response::new(FlushResponse {}))
    }
    async fn flush_internal(
        &self,
        _req: Request<FlushRequest>,
    ) -> Result<Response<FlushResponse>, Status> {
        Ok(Response::new(FlushResponse {}))
    }
    async fn panic(&self, _req: Request<PanicRequest>) -> Result<Response<PanicResponse>, Status> {
        Ok(Response::new(PanicResponse { healthy: true }))
    }
    async fn health(
        &self,
        _req: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        Ok(Response::new(HealthResponse {
            info: String::new(),
        }))
    }
    async fn lwt_prepare(
        &self,
        req: Request<LwtPrepareRequest>,
    ) -> Result<Response<LwtPrepareResponse>, Status> {
        let r = req.into_inner();
        match self.store.prepare(&r.key, r.ballot).await {
            Ok((accepted, committed)) => {
                let (ab, av) = accepted.unwrap_or((0, String::new()));
                Ok(Response::new(LwtPrepareResponse {
                    ok: true,
                    promised: r.ballot,
                    accepted_ballot: ab,
                    accepted_value: av,
                    committed_value: committed.unwrap_or_default(),
                }))
            }
            Err(promised) => Ok(Response::new(LwtPrepareResponse {
                ok: false,
                promised,
                accepted_ballot: 0,
                accepted_value: String::new(),
                committed_value: String::new(),
            })),
        }
    }
    async fn lwt_propose(
        &self,
        req: Request<LwtProposeRequest>,
    ) -> Result<Response<LwtProposeResponse>, Status> {
        let r = req.into_inner();
        match self.store.accept(&r.key, r.ballot, &r.value).await {
            Ok(()) => Ok(Response::new(LwtProposeResponse {
                ok: true,
                promised: r.ballot,
            })),
            Err(promised) => Ok(Response::new(LwtProposeResponse {
                ok: false,
                promised,
            })),
        }
    }
    async fn lwt_commit(
        &self,
        req: Request<LwtCommitRequest>,
    ) -> Result<Response<LwtCommitResponse>, Status> {
        let r = req.into_inner();
        self.store.commit(&r.key, &r.value).await;
        Ok(Response::new(LwtCommitResponse {}))
    }
    async fn lwt_read(
        &self,
        req: Request<LwtReadRequest>,
    ) -> Result<Response<LwtReadResponse>, Status> {
        let r = req.into_inner();
        let val = self.store.read(&r.key).await.unwrap_or_default();
        Ok(Response::new(LwtReadResponse { value: val }))
    }
}

async fn spawn_node(port: u16) -> (tokio::task::JoinHandle<()>, String) {
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let url = format!("http://127.0.0.1:{}", port);
    let store = Arc::new(ReplicaStore::default());
    let svc = TestService { store };
    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(CassServer::new(svc))
            .serve(addr)
            .await
            .unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    (handle, url)
}

#[tokio::test]
async fn cas_insert_succeeds() {
    let (h1, a1) = spawn_node(5701).await;
    let (h2, a2) = spawn_node(5702).await;
    let (h3, a3) = spawn_node(5703).await;
    let lwt = Coordinator::new(vec![a1.clone(), a2.clone(), a3.clone()]);
    let ok = lwt.compare_and_set("k", None, "foo").await;
    assert!(ok);
    let committed = lwt.committed_values("k").await;
    assert!(committed.iter().all(|v| v.as_deref() == Some("foo")));
    h1.abort();
    h2.abort();
    h3.abort();
}

#[tokio::test]
async fn cas_mismatch_fails() {
    let (h1, a1) = spawn_node(5801).await;
    let (h2, a2) = spawn_node(5802).await;
    let (h3, a3) = spawn_node(5803).await;
    let lwt = Coordinator::new(vec![a1.clone(), a2.clone(), a3.clone()]);
    assert!(lwt.compare_and_set("k", None, "foo").await);
    let ok = lwt.compare_and_set("k", Some("bar"), "baz").await;
    assert!(!ok);
    let committed = lwt.committed_values("k").await;
    assert!(committed.iter().all(|v| v.as_deref() == Some("foo")));
    h1.abort();
    h2.abort();
    h3.abort();
}
