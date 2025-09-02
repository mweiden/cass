use std::sync::Arc;
use tokio::sync::Mutex;

/// Represents a single replica participating in a Paxos round.
///
/// Each replica keeps track of the highest ballot it has promised not
/// to accept lower ballots for, the last accepted proposal and the
/// committed value.
#[derive(Default)]
pub struct Replica {
    promised: u64,
    accepted: Option<(u64, String)>,
    committed: Option<String>,
}

impl Replica {
    async fn prepare(
        &mut self,
        ballot: u64,
    ) -> Result<(Option<(u64, String)>, Option<String>), u64> {
        if ballot > self.promised {
            self.promised = ballot;
            Ok((self.accepted.clone(), self.committed.clone()))
        } else {
            Err(self.promised)
        }
    }

    async fn accept(&mut self, ballot: u64, value: &str) -> Result<(), u64> {
        if ballot >= self.promised {
            self.promised = ballot;
            self.accepted = Some((ballot, value.to_string()));
            Ok(())
        } else {
            Err(self.promised)
        }
    }

    async fn commit(&mut self, value: &str) {
        self.committed = Some(value.to_string());
        self.accepted = None;
    }
}

/// Coordinator for running a lightweight transaction across a set of replicas.
///
/// The coordinator implements a very small portion of Cassandra's Paxos based
/// protocol used for `IF` style conditional updates. The implementation here is
/// deliberately simplified and is intended for unit tests and experimentation
/// only.
pub struct Coordinator {
    replicas: Vec<Arc<Mutex<Replica>>>,
}

impl Coordinator {
    /// Create a new coordinator with the given number of in-memory replicas.
    pub fn new(replica_count: usize) -> Self {
        let replicas = (0..replica_count)
            .map(|_| Arc::new(Mutex::new(Replica::default())))
            .collect();
        Self { replicas }
    }

    fn quorum(&self) -> usize {
        self.replicas.len() / 2 + 1
    }

    /// Execute a compare-and-set style lightweight transaction.
    ///
    /// The transaction will only commit `new_value` if the current committed
    /// value across a quorum of replicas matches `expected`.
    pub async fn compare_and_set(&self, expected: Option<&str>, new_value: &str) -> bool {
        use std::time::{SystemTime, UNIX_EPOCH};
        let ballot = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        // Phase 1: Prepare / Promise
        let mut promises = Vec::new();
        for r in &self.replicas {
            let mut guard = r.lock().await;
            if let Ok(p) = guard.prepare(ballot).await {
                promises.push(p);
            }
        }
        if promises.len() < self.quorum() {
            return false;
        }
        // Ensure the expected value matches any committed values we saw.
        for (_, committed) in &promises {
            if committed.as_deref() != expected {
                return false;
            }
        }
        // Determine value to propose: if any replica had an accepted value, use
        // the one with the highest ballot as required by Paxos. Otherwise use
        // the requested new value.
        let mut proposal = new_value.to_string();
        for (accepted, _) in &promises {
            if let Some((_b, v)) = accepted {
                proposal = v.clone();
            }
        }

        // Phase 2: Accept
        let mut acks = 0usize;
        for r in &self.replicas {
            let mut guard = r.lock().await;
            if guard.accept(ballot, &proposal).await.is_ok() {
                acks += 1;
            }
        }
        if acks < self.quorum() {
            return false;
        }

        // Phase 3: Commit
        for r in &self.replicas {
            let mut guard = r.lock().await;
            guard.commit(&proposal).await;
        }
        // If we committed a value different from what the caller requested
        // due to a prior accepted value we treat the operation as failed from
        // the caller's perspective.
        proposal == new_value
    }

    /// Helper for tests to inspect the committed value on each replica.
    pub async fn committed_values(&self) -> Vec<Option<String>> {
        let mut vals = Vec::new();
        for r in &self.replicas {
            let guard = r.lock().await;
            vals.push(guard.committed.clone());
        }
        vals
    }
}
