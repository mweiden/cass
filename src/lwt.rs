use std::collections::HashMap;

use tokio::sync::RwLock;

use crate::rpc::{
    LwtCommitRequest, LwtPrepareRequest, LwtProposeRequest, LwtReadRequest, cass_client::CassClient,
};

#[derive(Default)]
struct Entry {
    promised: u64,
    accepted: Option<(u64, String)>,
    committed: Option<String>,
}

/// Per-node replica state for lightweight transactions.
#[derive(Default)]
pub struct ReplicaStore {
    inner: RwLock<HashMap<String, Entry>>,
}

impl ReplicaStore {
    pub async fn prepare(
        &self,
        key: &str,
        ballot: u64,
    ) -> Result<(Option<(u64, String)>, Option<String>), u64> {
        let mut map = self.inner.write().await;
        let ent = map.entry(key.to_string()).or_default();
        if ballot > ent.promised {
            ent.promised = ballot;
            Ok((ent.accepted.clone(), ent.committed.clone()))
        } else {
            Err(ent.promised)
        }
    }

    pub async fn accept(&self, key: &str, ballot: u64, value: &str) -> Result<(), u64> {
        let mut map = self.inner.write().await;
        let ent = map.entry(key.to_string()).or_default();
        if ballot >= ent.promised {
            ent.promised = ballot;
            ent.accepted = Some((ballot, value.to_string()));
            Ok(())
        } else {
            Err(ent.promised)
        }
    }

    pub async fn commit(&self, key: &str, value: &str) {
        let mut map = self.inner.write().await;
        let ent = map.entry(key.to_string()).or_default();
        ent.committed = Some(value.to_string());
        ent.accepted = None;
    }

    pub async fn read(&self, key: &str) -> Option<String> {
        let map = self.inner.read().await;
        map.get(key).and_then(|e| e.committed.clone())
    }
}

/// Network coordinator for running Paxos rounds across cass replicas.
pub struct Coordinator {
    peers: Vec<String>,
}

impl Coordinator {
    /// Create a coordinator that will contact the provided peer URLs.
    pub fn new(peers: Vec<String>) -> Self {
        Self { peers }
    }

    fn quorum(&self) -> usize {
        self.peers.len() / 2 + 1
    }

    /// Execute a compare-and-set operation for a particular key.
    pub async fn compare_and_set(
        &self,
        key: &str,
        expected: Option<&str>,
        new_value: &str,
    ) -> bool {
        use std::time::{SystemTime, UNIX_EPOCH};
        let ballot = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        // Phase 1: Prepare
        let mut promises = Vec::new();
        for node in &self.peers {
            if let Ok(mut client) = CassClient::connect(node.clone()).await {
                match client
                    .lwt_prepare(LwtPrepareRequest {
                        key: key.to_string(),
                        ballot,
                    })
                    .await
                {
                    Ok(resp) => {
                        let r = resp.into_inner();
                        if r.ok {
                            let accepted = if r.accepted_ballot > 0 {
                                Some((r.accepted_ballot, r.accepted_value))
                            } else {
                                None
                            };
                            let committed = if r.committed_value.is_empty() {
                                None
                            } else {
                                Some(r.committed_value)
                            };
                            promises.push((accepted, committed));
                        }
                    }
                    Err(_) => {}
                }
            }
        }
        if promises.len() < self.quorum() {
            return false;
        }
        for (_, committed) in &promises {
            if committed.as_deref() != expected {
                return false;
            }
        }
        let mut proposal = new_value.to_string();
        for (accepted, _) in &promises {
            if let Some((_b, v)) = accepted {
                proposal = v.clone();
            }
        }

        // Phase 2: Propose
        let mut acks = 0usize;
        for node in &self.peers {
            if let Ok(mut client) = CassClient::connect(node.clone()).await {
                if let Ok(resp) = client
                    .lwt_propose(LwtProposeRequest {
                        key: key.to_string(),
                        ballot,
                        value: proposal.clone(),
                    })
                    .await
                {
                    if resp.into_inner().ok {
                        acks += 1;
                    }
                }
            }
        }
        if acks < self.quorum() {
            return false;
        }

        // Phase 3: Commit
        for node in &self.peers {
            if let Ok(mut client) = CassClient::connect(node.clone()).await {
                let _ = client
                    .lwt_commit(LwtCommitRequest {
                        key: key.to_string(),
                        value: proposal.clone(),
                    })
                    .await;
            }
        }
        proposal == new_value
    }

    /// Fetch the committed value for `key` from all peers.
    pub async fn committed_values(&self, key: &str) -> Vec<Option<String>> {
        let mut vals = Vec::new();
        for node in &self.peers {
            if let Ok(mut client) = CassClient::connect(node.clone()).await {
                if let Ok(resp) = client
                    .lwt_read(LwtReadRequest {
                        key: key.to_string(),
                    })
                    .await
                {
                    let v = resp.into_inner().value;
                    if v.is_empty() {
                        vals.push(None);
                    } else {
                        vals.push(Some(v));
                    }
                } else {
                    vals.push(None);
                }
            } else {
                vals.push(None);
            }
        }
        vals
    }
}
// ReplicaStore is expected to be held by each server instance.
